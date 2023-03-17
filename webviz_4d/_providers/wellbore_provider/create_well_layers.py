import numpy as np
import pandas as pd
import math

from webviz_4d.create_well_layers import check_interval_date


def create_new_well_layer(
    interval_4d: list = None,
    metadata_df: pd.DataFrame = None,
    trajectories_df: pd.DataFrame = None,
    prod_data: pd.DataFrame = None,
    surface_picks: pd.DataFrame = None,
    completion_df: pd.DataFrame = None,
    color: str = None,
    layer_name: str = "",
    label: str = "",
):
    """Make layeredmap wells layer"""
    tooltips = []
    layer_df = pd.DataFrame()

    md_start_list = []
    md_end = np.nan
    wellbores = []

    interval = None

    if interval_4d is not None:
        interval = interval_4d

    print(layer_name, "interval=", interval)

    for row in metadata_df.iterrows():
        tooltip = create_tooltip(row, layer_name)

        df = row[1]
        wellbore_name = df["unique_wellbore_identifier"]

        if surface_picks is not None and not surface_picks.empty:
            try:
                selected_surface_pick = surface_picks[
                    surface_picks["unique_wellbore_identifier"] == wellbore_name
                ]
                md_start = selected_surface_pick["md"].to_numpy()[0]
            except:
                md_start = np.nan
        else:
            md_start = 0

        if interval is not None and prod_data is not None:
            status, start_date, end_date = check_production_data(
                prod_data, layer_name, wellbore_name, interval
            )
            # print(wellbore_name, status, start_date, end_date)

            if status:
                tooltip = tooltip + " Start:" + start_date[:4] + " End:" + end_date[:4]

        if tooltip is not None and md_start is not None and not math.isnan(md_start):
            tooltips.append(tooltip)
            md_start_list.append(md_start)
            wellbores.append(wellbore_name)
            if layer_name in ["production", "injection"]:
                print(tooltip)
        else:
            # print(
            #     "Wellbore",
            #     wellbore_name,
            #     " not in",
            #     layer_name,
            # )
            pass

    layer_df["unique_wellbore_identifier"] = wellbores
    layer_df["color"] = color
    layer_df["tooltip"] = tooltips
    layer_df["layer_name"] = "well_layer_" + layer_name
    layer_df["md_start"] = md_start_list
    layer_df["md_end"] = md_end

    # well_layer = make_new_smda_well_layer(
    #     layer_df,
    #     trajectories_df,
    #     label=label,
    # )

    return layer_df


def check_production_data(prod_data, layer_name, wellbore_name, interval):
    status = False
    start_date = None
    end_date = None

    try:
        selected_prod_data = prod_data.loc[
            (prod_data["WB_UWBI"] == wellbore_name)
            & (prod_data["PURPOSE"] == layer_name)
        ]
        start_date = selected_prod_data["WB_START_DATE"]
        start_date = start_date.values[0]

        if start_date is not None:
            start_date = str(start_date)[:10]

        end_date = selected_prod_data["WB_END_DATE"]
        end_date = end_date.values[0]

        if end_date is not None:
            end_date = str(end_date)[11:]

        started = check_interval_date(interval, start_date)
        stopped = check_interval_date(interval, end_date)

        if started == "less" and (stopped == "greater" or stopped is None):
            if end_date is None:
                end_date = "----"
    except:
        # print(wellbore_name, "not in PDM")
        pass

    return status, start_date, end_date


def make_new_smda_well_layer(
    layer_df,
    wells_df,
    label="Drilled wells",
):
    """Make layeredmap wells layer"""
    # t0 = time.time()
    data = []

    for _index, row in layer_df.iterrows():
        true_name = row["unique_wellbore_identifier"]
        well_dataframe = wells_df[wells_df["unique_wellbore_identifier"] == true_name]

        polyline_data = {}

        if polyline_data:
            data.append(polyline_data)

    layer = {"name": label, "checked": False, "base_layer": False, "data": data}

    return layer


def create_tooltip(row, layer_name):
    tooltip = None

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


def get_info(start_date, stop_date, fluid, volume):
    """Create information string for production/injection wells"""
    units = {"oil": "[kSm3]", "water": "[km3]", "gas": "[MSm3]"}

    if volume is None or volume == 0:
        return None

    unit = units.get(fluid)

    if stop_date is None or (not isinstance(stop_date, str) and math.isnan(stop_date)):
        stop_date_txt = "---"
    else:
        stop_date_txt = stop_date[:4]

    if fluid == "wag":
        info = "(WAG) Start: " + str(start_date[:4]) + " Last: " + str(stop_date_txt)
    else:
        info = (
            fluid
            + " {:.0f} ".format(volume)
            + unit
            + " Start: "
            + str(start_date[:4])
            + " Last: "
            + str(stop_date_txt)
        )

    return info


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
