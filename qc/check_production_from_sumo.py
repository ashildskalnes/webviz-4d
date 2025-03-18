import numpy as np
import pandas as pd
import argparse
import polars as pl
import polars.selectors as cs
from time import time
from datetime import datetime
from pprint import pprint

from webviz_4d._datainput.well import (
    get_short_wellname,
    create_well_layer,
)
from webviz_4d._datainput._sumo import get_sumo_case

from fmu.sumo.explorer import Explorer


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
