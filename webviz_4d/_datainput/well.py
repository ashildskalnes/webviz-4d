import pandas as pd
import math
import numpy as np
import xtgeo
import time
import xtgeo.cxtgeo._cxtgeo as _cxtgeo

from webviz_4d._providers.wellbore_provider._provider_impl_file import (
    ProviderImplFile,
)


def load_smda_metadata(provider, field):
    metadata = provider.drilled_wellbore_metadata(
        field=field,
    )
    dataframe = metadata.dataframe

    return dataframe


def load_smda_wellbores(provider, field):
    trajectories = provider.drilled_trajectories(
        field_name=field,
    )

    dataframe = trajectories.dataframe

    unique_wellbores = dataframe["unique_wellbore_identifier"].unique()
    print(" - drilled wellbores:", len(unique_wellbores))

    return dataframe


def load_planned_wells(provider, field):
    planned_wells = provider.get_planned_wellbores(
        field_name=field,
    )

    dataframe = planned_wells.trajectories.dataframe
    unique_wellbores = dataframe["unique_wellbore_identifier"].unique()
    print(" - planned wellbores:", len(unique_wellbores))

    return planned_wells


def load_pdm_info(provider, field):
    metadata = provider.get_pdm_wellbores(field_name=field)
    dataframe = metadata.dataframe

    return dataframe


def create_basic_well_layers(
    basic_well_layers_dict,
    planned_wells_info,
    planned_wells_df,
    drilled_wells_info,
    drilled_wells_df,
    surface_picks,
    well_colors,
):
    basic_well_layers = []
    print("Creating basic well layers ...")

    for key, value in basic_well_layers_dict.items():
        layer_name = key
        label = value
        color = well_colors.get(layer_name, None)

        if color is None:
            color = well_colors.get("default", None)

        # print("  ", layer_name)

        tooltips = []
        md_end = np.nan
        layer_df = pd.DataFrame()

        md_start_list = []
        wellbores = []
        colors = []

        if layer_name == "planned":
            metadata = planned_wells_info
            trajectories = planned_wells_df
        if layer_name == "drilled_wells":
            metadata = drilled_wells_info
            trajectories = drilled_wells_df
        elif layer_name == "reservoir_section":
            metadata = drilled_wells_info
            trajectories = drilled_wells_df

        for row in metadata.iterrows():
            # print(row)
            status = False

            df = row[1]
            wellbore_name = df["unique_wellbore_identifier"]

            md_start = 0

            if layer_name == "reservoir_section":
                if surface_picks is not None and not surface_picks.empty:
                    try:
                        selected_surface_pick = surface_picks[
                            surface_picks["unique_wellbore_identifier"] == wellbore_name
                        ]
                        md_start = selected_surface_pick["md"].to_numpy()[0]

                        status = True
                    except:
                        md_start = np.nan
            else:
                status = True

            if status:
                tooltip = create_basic_tooltip(row, layer_name)

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

        well_layer = create_well_layer(layer_df, trajectories, label=label)

        if well_layer:
            basic_well_layers.append(well_layer)

    return basic_well_layers


def create_pdm_well_layer(
    interval_4d: list = None,
    metadata_df: pd.DataFrame = None,
    trajectories_df: pd.DataFrame = None,
    prod_data: pd.DataFrame = None,
    surface_picks: pd.DataFrame = None,
    completion_df: pd.DataFrame = None,
    color_settings: dict = {},
    layer_name: str = "",
    label: str = "",
    uwi="unique_wellbore_identifier",
):
    """Make layeredmap wells layer"""
    prod_units = {
        "OIL_VOL": "kSm3",
        "GAS_VOL": "MSm3",
        "WATER_VOL": "kSm3",
    }

    prod_labels = {
        "OIL_VOL": "oil",
        "GAS_VOL": "gas",
        "WATER_VOL": "water",
    }

    inj_units = {
        "GI_VOL": "MSm3",
        "WI_VOL": "kSm3",
    }

    inj_labels = {
        "GI_VOL": "gas",
        "WI_VOL": "water",
    }

    if not color_settings:
        color_settings = {
            "default": "black",
            "planned": "purple",
            "oil_production": "green",
            "gas_production": "red",
            "gas_injection": "salmon",
            "water_injection": "blue",
            "wag_injection": "cyan",
        }

    tooltips = []
    layer_df = pd.DataFrame()

    md_start_list = []
    md_end = np.nan
    wellbores = []
    colors = []

    interval = None

    if interval_4d is not None:
        interval = interval_4d

    # print("Layer name", layer_name)
    for row in metadata_df.iterrows():
        # print(row)
        status = False
        color = color_settings.get("default")

        if layer_name == "planned":
            color = color_settings.get("planned")

        df = row[1]
        wellbore_name = df[uwi]

        md_start = 0

        if layer_name not in ["planned", "drilled_wells"]:
            if surface_picks is not None and not surface_picks.empty:
                try:
                    selected_surface_pick = surface_picks[
                        surface_picks["unique_wellbore_identifier"] == wellbore_name
                    ]
                    md_start = selected_surface_pick["md"].to_numpy()[0]

                    status = True
                except:
                    md_start = np.nan

            if interval is not None and prod_data is not None:
                if md_start is not None and not math.isnan(md_start):
                    if "production" in layer_name:
                        fluids = ["OIL_VOL", "GAS_VOL", "WATER_VOL"]
                        units = prod_units
                        labels = prod_labels
                    elif "injection in layer_name":
                        fluids = ["GI_VOL", "WI_VOL"]
                        units = inj_units
                        labels = inj_labels

                    status, volumes = get_production_data(
                        prod_data, wellbore_name, fluids
                    )

                    if status:
                        short_name = get_short_wellname(wellbore_name)
                        fluids_text = ""

                        for key in units.keys():
                            fluids_text = (
                                fluids_text
                                + labels[key]
                                + " {:.0f}".format(volumes[key].values[0])
                                + " ["
                                + units[key]
                                + "], "
                            )
                        tooltip = (
                            short_name
                            + ": "
                            + layer_name
                            + " ("
                            + fluids_text[:-2]
                            + ")"
                        )

                        if (
                            "OIL_VOL" in volumes.columns
                            and volumes["OIL_VOL"].values[0] > 0
                        ):
                            color = color_settings.get("oil_production")
                        elif (
                            "GAS_VOL" in volumes.columns
                            and volumes["GAS_VOL"].values[0] > 0
                        ):
                            color = color_settings.get("gas_production")
                        elif (
                            "GI_VOL" in volumes.columns
                            and volumes["GI_VOL"].values[0] > 0
                            and volumes["WI_VOL"].values[0] > 0
                        ):
                            color = color_settings.get("wag_injection")
                        elif (
                            "GI_VOL" in volumes.columns
                            and volumes["GI_VOL"].values[0] > 0
                        ):
                            color = color_settings.get("gas_injection")
                        elif (
                            "WI_VOL" in volumes.columns
                            and volumes["WI_VOL"].values[0] > 0
                        ):
                            color = color_settings.get("water_injection")
        else:
            if md_start is not None and not math.isnan(md_start):
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

    # print(layer_df)

    pdm_well_layer = create_well_layer(layer_df, trajectories_df, label=label)

    return pdm_well_layer


def get_production_data(prod_data, wellbore_name, fluids):
    status = False

    try:
        wellbore_volumes = prod_data[prod_data["WB_UWBI"] == wellbore_name]
        volumes_df = wellbore_volumes[fluids]
        fluid_volumes = volumes_df.dropna()

        if fluid_volumes is not None and not fluid_volumes.empty:
            sum_vol = 0

            for fluid in fluids:
                vol = fluid_volumes[fluid].values[0]
                sum_vol = sum_vol + vol

            if sum_vol > 0:
                status = True
    except:
        wellbore_volumes = None

    return status, fluid_volumes


def create_well_layer(
    layer_df, wells_df, label="Drilled wells", uwi="unique_wellbore_identifier"
):
    """Make layeredmap wells layer"""
    # t0 = time.time()
    data = []

    for _index, row in layer_df.iterrows():
        true_name = row[uwi]
        well_dataframe = wells_df[wells_df["unique_wellbore_identifier"] == true_name]

        polyline_data = get_well_polyline(
            well_dataframe,
            row["md_start"],
            row["md_end"],
            row["color"],
            row["tooltip"],
        )

        if polyline_data:
            data.append(polyline_data)

    layer = {"name": label, "checked": False, "base_layer": False, "data": data}

    return layer


def create_basic_tooltip(row, layer_name):
    tooltip = None

    if layer_name in [
        "planned",
        "drilled_wells",
        "reservoir_sections",
    ]:
        df = row[1]
        wellbore_name = df["unique_wellbore_identifier"]
        short_name = get_short_wellname(wellbore_name)

        if short_name == "":
            short_name = wellbore_name

        purpose = df["purpose"]
        content = df["content"]
        status = df["status"]

        if content is None:
            content = ""

        if purpose is None:
            if status is not None:
                purpose = df["status"]
            else:
                purpose = ""

        tooltip = short_name + ":" + purpose + "(" + content + ")"
        # print(wellbore_name, tooltip)

    return tooltip


def get_short_wellname(wellname):
    """Well name on a short name form where blockname and spaces are removed.
    This should cope with both North Sea style and Haltenbanken style.
    E.g.: '31/2-G-5 AH' -> 'G-5AH', '6472_11-F-23_AH_T2' -> 'F-23AHT2'
    """
    newname = []
    first1 = False
    first2 = False
    for letter in wellname:
        if first1 and first2:
            newname.append(letter)
            continue
        if letter in ("_", "/"):
            first1 = True
            continue
        if first1 and letter == "-":
            first2 = True
            continue

    xname = "".join(newname)
    xname = xname.replace("_", "")
    xname = xname.replace(" ", "")
    return xname


def load_well(well_path):
    """Return a well object (xtgeo) for a given file (RMS ascii format)"""
    return xtgeo.well_from_file(well_path, mdlogname="MD")



def load_all_wells(metadata, delta):
    """For all wells in a folder return
    - a list of dataframes with the well trajectories
    - dataframe with metadata for all the wells"""

    all_wells_list = []

    try:
        wellfiles = metadata["file_name"]
        wellfiles.dropna(inplace=True)
    except:
        wellfiles = []
        raise Exception("No wellfiles found")

    for wellfile in wellfiles:
        # well = load_well(wellfile)
        well = xtgeo.well_from_file(wellfile, mdlogname="MD")

        # Resample well trajectory to delta
        try:
            well.rescale(delta=delta)
        except:
            print(
                "WARNING:", well.name, ": rescaling failed, keeping original trajectory"
            )

        well.dataframe = well.dataframe[["X_UTME", "Y_UTMN", "Z_TVDSS", "MD"]]
        well_metadata = metadata.loc[metadata["wellbore.rms_name"] == well.wellname]
        layer_name = well_metadata["layer_name"].values[0]

        if layer_name == "Drilled wells":
            well.dataframe["WELLBORE_NAME"] = well.truewellname
            short_name = well.shortwellname
        else:
            well.dataframe["WELLBORE_NAME"] = well.wellname
            short_name = well.wellname

        well_info = metadata.loc[metadata["wellbore.short_name"] == short_name]
        layer_name = well_info["layer_name"].values[0]
        well.dataframe["layer_name"] = layer_name

        all_wells_list.append(well.dataframe)

    all_wells_df = pd.concat(all_wells_list)
    return all_wells_df


def get_position_data(well_dataframe, md_start, md_end):
    """Return x- and y-values for a well between given depths"""
    rename_dict = {
        "MD": "md",
        "X_UTME": "easting",
        "Y_UTMN": "northing",
        "Z_TVDSS": "tvd_msl",
    }

    delta = 200
    positions = None

    if "MD" in well_dataframe.columns:
        well_dataframe = well_dataframe.rename(columns=rename_dict)

    if not well_dataframe.empty and not math.isnan(md_start):
        well_df = well_dataframe[well_dataframe["md"] >= md_start]
        resampled_df = resample_well(well_df, md_start, md_end, delta)
        positions = resampled_df[["easting", "northing"]].values

    return positions


def resample_well(well_df, md_start, md_end, delta):
    # Resample well trajectory by selecting only positions with a lateral distance larger than the given delta value
    if math.isnan(md_end):
        md_end = well_df["md"].iloc[-1]

    dfr = well_df[(well_df["md"] >= md_start) & (well_df["md"] <= md_end)]

    x = dfr["easting"].values
    y = dfr["northing"].values
    tvd = dfr["tvd_msl"].values
    md = dfr["md"].values

    x_new = [x[0]]
    y_new = [y[0]]
    tvd_new = [tvd[0]]
    md_new = [md[0]]
    j = 0

    for i in range(1, len(x)):
        dist = ((x[i] - x[j]) ** 2 + (y[i] - y[j]) ** 2) ** 0.5
        # print(i, j, md[i], dist)

        if dist > delta:
            x_new.append(x[i])
            y_new.append(y[i])
            tvd_new.append(tvd[i])
            md_new.append(md[i])
            j = i

    x_new.append(x[-1])
    y_new.append(y[-1])
    tvd_new.append(tvd[-1])
    md_new.append(md[-1])

    dfr_new = pd.DataFrame()
    dfr_new["easting"] = x_new
    dfr_new["northing"] = y_new
    dfr_new["tvd_msl"] = tvd_new
    dfr_new["md"] = md_new

    return dfr_new


def get_well_polyline(
    well_dataframe,
    md_start,
    md_end,
    color,
    tooltip,
):
    """Create polyline data - contains well trajectory, color and tooltip"""

    positions = get_position_data(well_dataframe, md_start, md_end)

    if positions is not None:
        return {
            "type": "polyline",
            "color": color,
            "positions": positions,
            "tooltip": tooltip,
        }
    else:
        return {}


def get_surface_picks(wellbores_df, surf):
    """get Surface picks (stolen and adjusted from xtgeo)"""

    surface_picks = pd.DataFrame()

    if surf is None:
        print("WARNING: No Top reservoir surface loaded")
        return surface_picks

    md_values = []
    print("Extracting surface picks for top reservoir ...")

    wellbore_names = wellbores_df["unique_wellbore_identifier"].unique()

    for wellbore in wellbore_names:
        wellbore_df = wellbores_df[
            wellbores_df["unique_wellbore_identifier"] == wellbore
        ]
        xcor = wellbore_df["easting"].values
        ycor = wellbore_df["northing"].values
        zcor = wellbore_df["tvd_msl"].values
        mcor = wellbore_df["md"].values

        nval, xres, yres, zres, mres, dres = _cxtgeo.well_surf_picks(
            xcor,
            ycor,
            zcor,
            mcor,
            surf.ncol,
            surf.nrow,
            surf.xori,
            surf.yori,
            surf.xinc,
            surf.yinc,
            surf.yflip,
            surf.rotation,
            surf.npvalues1d,
            xcor.size,
            xcor.size,
            xcor.size,
            xcor.size,
            xcor.size,
        )

        if nval > 0:
            md_value = mres[0]
        else:
            md_value = np.nan

        md_values.append(md_value)

    surface_picks["unique_wellbore_identifier"] = wellbore_names
    surface_picks["md"] = md_values

    non_nan_values = [item for item in md_values if str(item) != "nan"]
    print(" - surface picks:", len(non_nan_values))

    return surface_picks


def check_interval_date(interval, selected_date):
    """Check if a selected date is included in a 4D interval or not"""
    # if selected_date is None or (
    #     not isinstance(selected_date, str) and math.isnan(selected_date)
    # ):

    if selected_date is None or not isinstance(selected_date, str):
        status = None
    elif selected_date >= interval[:10]:
        if selected_date < interval[11:]:
            status = "inside"
        else:
            status = "greater"
    else:
        status = "less"

    return status


def create_production_layers(
    field_name: str = "",
    pdm_provider: ProviderImplFile = None,
    interval_4d: str = "",
    wellbore_trajectories: pd.DataFrame = pd.DataFrame(),
    surface_picks: pd.DataFrame = pd.DataFrame(),
    layer_options: dict = {},
    well_colors: dict = {},
    prod_interval: str = "Day",
):
    tic = time.perf_counter()

    production_data = pdm_provider.get_field_prod_data(
        field_name=field_name,
        start_date=interval_4d[-10:],
        end_date=interval_4d[:10],
        interval=prod_interval,
    )

    injection_data = pdm_provider.get_field_inj_data(
        field_name=field_name,
        start_date=interval_4d[-10:],
        end_date=interval_4d[:10],
        interval=prod_interval,
    )

    toc = time.perf_counter()
    # print(f"Downloaded production and injection data in {toc - tic:0.4f} seconds")

    volumes = pd.merge(
        production_data.dataframe,
        injection_data.dataframe,
        how="outer",
    )

    pdm_wellbores = volumes["WB_UWBI"].tolist()
    pdm_trajectories = wellbore_trajectories[
        wellbore_trajectories["unique_wellbore_identifier"].isin(pdm_wellbores)
    ]

    # print("Creating interval well layers ...")
    interval_well_layers = []

    for key, value in layer_options.items():
        layer_name = key
        color = well_colors.get(layer_name, None)

        if color is None:
            color = well_colors.get("default", None)

        well_layer = create_pdm_well_layer(
            interval_4d=interval_4d,
            metadata_df=volumes,
            trajectories_df=pdm_trajectories,
            surface_picks=surface_picks,
            prod_data=volumes,
            color_settings=well_colors,
            layer_name=key,
            label=value,
            uwi="WB_UWBI",
        )

        if well_layer:
            interval_well_layers.append(well_layer)

    return interval_well_layers


def get_well_position_data(well_dataframe, md_start, md_end, delta):
    """Return x- and y-values for a well between given depths"""
    positions = [[]]

    td = well_dataframe["md"].iloc[-1]

    if not math.isnan(md_start):
        if md_start > td:
            print("WARNING: Wellbore: ", well_dataframe["WELLBORE_NAME"].iloc[0])
            print("md_start:", md_start, "TD:", td)

            return positions

        well_df = well_dataframe[well_dataframe["md"] >= md_start]

        resampled_df = resample_well(well_df, md_start, md_end, delta)
        positions = resampled_df[["easting", "northing"]].values

    return positions


def resample_well(well_df, md_start, md_end, delta):
    # Resample well trajectory by selecting only positions with a lateral distance larger than the given delta value
    dfr_new = pd.DataFrame()

    if not md_end or math.isnan(md_end):
        md_end = well_df["md"].iloc[-1]

    dfr = well_df[(well_df["md"] >= md_start) & (well_df["md"] <= md_end)]

    x = dfr["easting"].values
    y = dfr["northing"].values
    tvd = dfr["tvd_msl"].values
    md = dfr["md"].values

    x_new = [x[0]]
    y_new = [y[0]]
    tvd_new = [tvd[0]]
    md_new = [md[0]]
    j = 0

    for i in range(1, len(x)):
        dist = ((x[i] - x[j]) ** 2 + (y[i] - y[j]) ** 2) ** 0.5
        # print(i, j, md[i], dist)

        if dist > delta:
            x_new.append(x[i])
            y_new.append(y[i])
            tvd_new.append(tvd[i])
            md_new.append(md[i])
            j = i

    # Check if the last original positions should be added
    if md_new[-1] < md[-1]:
        x_new.append(x[-1])
        y_new.append(y[-1])
        tvd_new.append(tvd[-1])
        md_new.append(md[-1])

    dfr_new["easting"] = x_new
    dfr_new["northing"] = y_new
    dfr_new["tvd_msl"] = tvd_new
    dfr_new["md"] = md_new

    return dfr_new


def get_rms_name(wellbore_name):
    rms_name = wellbore_name.replace("/", "_").replace("NO ", "").replace(" ", "_")

    return rms_name
