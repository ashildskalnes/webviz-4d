import numpy as np
import pandas as pd
import math
import polars as pl
import polars.selectors as cs
from time import time
from datetime import datetime

from webviz_4d._datainput.well import (
    get_short_wellname,
    create_well_layer,
)


prod_units = {
    "WOPTH": "kSm3",
    "WGPTH": "MSm3",
    "WWPTH": "kSm3",
}

prod_labels = {
    "WOPTH": "oil",
    "WGPTH": "gas",
    "WWPTH": "water",
}

# inj_units = {
#     "GI_VOL": "MSm3",
#     "WI_VOL": "kSm3",
# }

# inj_labels = {
#     "GI_VOL": "gas",
#     "WI_VOL": "water",
# }

ecl_scaling = {
    "WOPTH": 1000,
    "WGPTH": 1000000,
    "WGITH": 1000000,
    "WWPTH": 1000,
    "WWITH": 1000,
}


def print_all(df):
    with pl.Config() as cfg:
        cfg.set_tbl_rows(-1)
        print(df)


def get_ecl_names(columns, key):
    ecl_names = []
    ecl_short_names = []

    wells = [s for s in columns if key in s]

    for well in wells:
        ecl_name = well.replace(key + ":", "")

        if ecl_name not in ecl_names:
            ecl_names.append(ecl_name)

    for ecl_name in ecl_names:
        ecl_short_name = ecl_name

        if "/" in ecl_short_name:
            if "-" in ecl_short_name:
                ind = ecl_short_name.index("-")
                ecl_short_name = ecl_name[ind + 1 :]
            else:
                print(" - WARNING short name not recognized:", ecl_short_name)

        ecl_short_names.append(ecl_short_name.replace("_", ""))

    ecl_df = pd.DataFrame(
        list(zip(ecl_names, ecl_short_names)),
        columns=["ECL name", "ECL shortname"],
    )

    return ecl_df


def match_ecl2smda(smda_df, ecl_df):
    smda_names = smda_df["unique_wellbore_identifier"].to_list()
    smda_short_names = []

    for smda_name in smda_names:
        smda_short_names.append(get_short_wellname(smda_name))

    smda_names_column = []
    smda_short_names_column = []

    for index, row in ecl_df.iterrows():
        ecl_name = row["ECL name"]
        short_name = row["ECL shortname"]

        if ecl_name in smda_names:
            smda_names_column.append(ecl_name)
            smda_short_names_column.append("")
        elif short_name in smda_short_names:
            index = smda_short_names.index(short_name)
            smda_names_column.append(smda_names[index])
            smda_short_names_column.append(short_name)
        else:
            smda_names_column.append("")
            smda_short_names_column.append("")
            # print("WARNING SMDA name not found:", ecl_name)

    return smda_names_column, smda_short_names_column


def load_eclipse_prod_data(summary_df, ecl_keywords, drilled_wells_info, dates_4d):
    prod_data_list = []

    for key in ecl_keywords:
        print()
        print("Eclipse key:", key)
        scaling = ecl_scaling.get(key)

        date_column = summary_df.select("DATE")

        selected_columns = summary_df.select(cs.starts_with(key))
        all_columns = selected_columns.with_columns(DATE=date_column.to_series())

        ecl_table = get_ecl_names(selected_columns.columns, key)
        ecl2smda_names, ecl2smda_shortnames = match_ecl2smda(
            drilled_wells_info, ecl_table
        )

        ecl_table["unique_wellbore_identifier"] = ecl2smda_names
        ecl_table["SMDA short_name"] = ecl2smda_shortnames

        start_dates = []

        # tic = time.perf_counter()

        for column in all_columns.columns:
            if column != "DATE":
                non_zero_values = all_columns.filter(pl.col(column) > 0)

                start_date = ""

                if len(non_zero_values) > 0:
                    start_date = str(non_zero_values.select("DATE").to_numpy()[0][0])[
                        :10
                    ]

                start_dates.append(start_date)

        ecl_polars_table = pl.from_pandas(ecl_table)

        ecl_polars_table = ecl_polars_table.with_columns(
            pl.Series("Start date", values=start_dates)
        )

        for date_4d in dates_4d:
            column_name = key + "_" + date_4d
            print(" - column name", column_name)
            date_str = date_4d + " 00:00:00"
            datetime_object = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")

            volumes = all_columns.filter(pl.col("DATE") == datetime_object)

            scaled_volumes = np.array(volumes.rows()[0][:-1]) / scaling
            col = pl.Series(scaled_volumes)
            ecl_polars_table = ecl_polars_table.with_columns(
                pl.Series(name=column_name, values=col)
            )

        prod_data_list.append(ecl_polars_table)

    return prod_data_list


def create_eclipse_production_layers(
    prod_data_list: list,
    interval_4d: str,
    wellbore_trajectories,
    surface_picks,
    layer_options: dict = {},
    well_colors: dict = {},
):
    interval_well_layers = []

    if "production" in layer_options:
        layer_name = "production"

        well_layer = None

        well_layer = create_eclipse_well_layer(
            prod_data_list=prod_data_list,
            interval_4d=interval_4d,
            trajectories_df=wellbore_trajectories,
            surface_picks=surface_picks,
            color_settings=well_colors,
            layer_name=layer_name,
            label="Producers",
        )

        if well_layer:
            interval_well_layers.append(well_layer)

    return interval_well_layers


def extract_interval_volume(prod_data_table, key, wellbore_name, time1, time2):
    volumes = prod_data_table.filter(
        pl.col("unique_wellbore_identifier") == wellbore_name
    )
    volume2 = volumes.select(key + "_" + time2).to_numpy()[0][0]
    volume1 = volumes.select(key + "_" + time1).to_numpy()[0][0]
    interval_volume = volume2 - volume1
    fluid_text = (
        prod_labels.get(key)
        + " {:.0f}".format(interval_volume)
        + " ["
        + prod_units[key]
        + "]"
    )

    return interval_volume, fluid_text


def create_tooltip(layer_name, row, fluid_texts):
    short_name = row["ECL shortname"]
    start_date = row["Start date"][:4]

    tooltip = short_name + ": "

    if "prod" in layer_name:
        tooltip = tooltip + layer_name[:4] + " ("

        prod_keys = list(prod_units.keys())

        for ind, key in enumerate(prod_keys):
            tooltip = tooltip + fluid_texts[ind] + ", "

        tooltip = tooltip + "Start:" + start_date + ")"

    return tooltip


def create_eclipse_well_layer(
    prod_data_list,
    interval_4d: str = None,
    trajectories_df: pd.DataFrame = None,
    surface_picks: pd.DataFrame = None,
    color_settings: dict = {},
    layer_name: str = "",
    label: str = "",
    uwi="unique_wellbore_identifier",
):
    """Make layeredmap wells layer"""

    tooltips = []
    layer_df = pd.DataFrame()

    md_start_list = []
    md_end = np.nan
    wellbores = []
    colors = []

    interval = None

    if interval_4d is not None:
        interval = interval_4d
        time1 = interval[11:]
        time2 = interval[:10]

    oil_prod_data = prod_data_list[0]

    valid_prod_data = oil_prod_data.filter(pl.col(uwi) != "")
    metadata_df = valid_prod_data.to_pandas()

    # not_valid_prod_data = prod_data.filter(pl.col(uwi) == "")

    # if len(not_valid_prod_data) > 0:
    #     print("Not valid UWI")
    #     print_all(not_valid_prod_data)
    #     print()

    for index, row in metadata_df.iterrows():
        status = False
        color = color_settings.get("default")

        if layer_name == "production":
            color = color_settings.get("oil_production")

        wellbore_name = row[uwi]

        md_start = 0

        if surface_picks is not None and not surface_picks.empty:
            try:
                selected_surface_pick = surface_picks[
                    surface_picks[uwi] == wellbore_name
                ]
                md_start = selected_surface_pick["md"].to_numpy()[0]

                status = True
            except:
                md_start = np.nan

        if interval is not None and oil_prod_data is not None:
            status = False

            if md_start is not None and not math.isnan(md_start):
                keys = prod_labels.keys()

                interval_volumes = []
                fluid_texts = []

                for index, key in enumerate(keys):
                    interval_volume, fluid_text = extract_interval_volume(
                        prod_data_list[index], key, wellbore_name, time1, time2
                    )
                    interval_volumes.append(interval_volume)
                    fluid_texts.append(fluid_text)

                tooltip = create_tooltip(layer_name, row, fluid_texts)

                if tooltip != "" and interval_volumes[0] > 0:
                    status = True

        if status:
            tooltips.append(tooltip)
            md_start_list.append(md_start)
            wellbores.append(wellbore_name)
            colors.append(color)

    layer_df["unique_wellbore_identifier"] = wellbores
    layer_df["color"] = colors
    layer_df["tooltip"] = tooltips
    layer_df["layer_name"] = "well_layer_" + layer_name
    layer_df["md_start"] = md_start_list
    layer_df["md_end"] = md_end
    layer_df["interval"] = interval

    pdm_well_layer = create_well_layer(layer_df, trajectories_df, label=label)

    return pdm_well_layer
