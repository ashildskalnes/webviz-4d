import os
import pandas as pd
import numpy as np
import xtgeo
import math
import argparse
from re import A
from io import BytesIO
import logging

from fmu.sumo.explorer import Explorer
import xtgeo
import xtgeo.cxtgeo._cxtgeo as _cxtgeo

from webviz_4d._providers.wellbore_provider._provider_impl_file import (
    ProviderImplFile,
)

from webviz_4d._providers.wellbore_provider.create_well_layers import (
    create_new_well_layer,
)


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


def get_well_polyline(
    well_dataframe,
    delta,
    md_start,
    md_end,
    color,
    tooltip,
):
    """Create polyline data - contains well trajectory, color and tooltip"""
    layer_dict = {}

    positions = get_position_data(well_dataframe, md_start, md_end, delta)

    if len(positions) > 1:
        layer_dict = {
            "type": "polyline",
            "color": color,
            "positions": positions,
            "tooltip": tooltip,
        }

    return layer_dict


def get_position_data(well_dataframe, md_start, md_end, delta):
    """Return x- and y-values for a well between given depths"""
    positions = [[]]

    td = well_dataframe["MD"].iloc[-1]

    if not math.isnan(md_start):
        if md_start > td:
            print("WARNING: Wellbore: ", well_dataframe["WELLBORE_NAME"].iloc[0])
            print("md_start:", md_start, "TD:", td)

            return positions

        well_df = well_dataframe[well_dataframe["MD"] >= md_start]

        resampled_df = resample_well(well_df, md_start, md_end, delta)
        positions = resampled_df[["X_UTME", "Y_UTMN"]].values

    return positions


def resample_well(well_df, md_start, md_end, delta):
    # Resample well trajectory by selecting only positions with a lateral distance larger than the given delta value
    dfr_new = pd.DataFrame()

    if not md_end or math.isnan(md_end):
        md_end = well_df["MD"].iloc[-1]

    dfr = well_df[(well_df["MD"] >= md_start) & (well_df["MD"] <= md_end)]

    x = dfr["X_UTME"].values
    y = dfr["Y_UTMN"].values
    tvd = dfr["Z_TVDSS"].values
    md = dfr["MD"].values

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

    dfr_new["X_UTME"] = x_new
    dfr_new["Y_UTMN"] = y_new
    dfr_new["Z_TVDSS"] = tvd_new
    dfr_new["MD"] = md_new

    return dfr_new


def get_rms_name(wellbore_name):
    rms_name = wellbore_name.replace("/", "_").replace("NO ", "").replace(" ", "_")

    return rms_name


def get_surface_picks(wellbores_df, surf):
    """get Surface picks (stolen and adjusted from xtgeo)"""

    surface_picks = pd.DataFrame()
    md_values = []

    wellbore_names = wellbores_df["unique_wellbore_identifier"].unique()
    md_values = []

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

    return surface_picks


def main():
    logging.getLogger("").setLevel(level=logging.WARNING)
    description = "Testing creation of well layers"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("field_name", help="Enter field name")
    args = parser.parse_args()
    print(args)

    omnia_env = ".omniaapi"
    home = os.path.expanduser("~")
    env_path = os.path.expanduser(os.path.join(home, omnia_env))

    smda_provider = ProviderImplFile(env_path, "SMDA")
    pdm_provider = ProviderImplFile(env_path, "PDM")

    sumo = Explorer(env="prod")
    js_id = "f37593b1-4e3b-684e-46a1-4211b09cf197"
    my_case = sumo.get_case_by_id(js_id)
    print("Case name:", my_case.name)

    field = args.field_name.upper()
    print("Field:", field)

    # Load top reservoir surface from sumo
    surface_attribute = ["depth_structural_model"]
    surface_name = ["Draupne Fm. 1 JS Top"]

    surfaces = my_case.get_objects(
        object_type="surface",
        aggregations=["mean"],
        object_names=surface_name,
        tag_names=surface_attribute,
    )

    if len(surfaces) > 0:
        s = surfaces[0]
        bytestring = BytesIO(s.blob)
        xtgeo_surface = xtgeo.surface_from_file(bytestring)
    else:
        xtgeo_surface = None

    # Get planned wells for a selected field
    # planned_wellbores = provider.get_planned_wellbores(
    #     field_name=field,
    # )
    # planned_trajectories = planned_wellbores.trajectories.dataframe
    # planned_metadata = planned_wellbores.metadata.dataframe
    # print(planned_metadata)
    # print(planned_trajectories)

    # layer_name = "planned"
    # label = "Planned wells"
    # color = "purple"
    # surface_picks = None

    # well_layer = create_new_well_layer(
    #     interval_4d=None,
    #     metadata_df=planned_metadata,
    #     trajectories_df=planned_trajectories,
    #     surface_picks=surface_picks,
    #     prod_data=None,
    #     color=color,
    #     layer_name=layer_name,
    #     label=label,
    # )

    # print(well_layer)

    # Get all drilled wellbore trajectories for the selected field
    all_trajectories = smda_provider.drilled_trajectories(
        field_name=field,
    )
    wellbores_df = all_trajectories.dataframe

    # Get metadata for all drilled wells
    print("\nGet metadata for all drilled wells ...")
    metadata = smda_provider.drilled_wellbore_metadata(
        field=field,
    )

    dataframe = metadata.dataframe

    drilled_wells_columns = [
        "unique_wellbore_identifier",
        "purpose",
        "status",
        "content",
    ]

    selected_metadata = dataframe[drilled_wells_columns]

    # Extract surface picks for all drilled wellbores
    surface_picks = get_surface_picks(wellbores_df, xtgeo_surface)
    print("Top reservoir (surface) picks")
    print(surface_picks)

    # Get production wells for a selected field
    layer_name = "production"
    color = "green"
    pdm_data = pdm_provider.get_pdm_wellbores(field_name=field)
    pdm_data_df = pdm_data.dataframe
    prod_data = pdm_data_df[pdm_data_df["PURPOSE"] == layer_name]
    interval_4d = ["2022-05-15-2021-05-15"]

    well_layer = create_new_well_layer(
        interval_4d=interval_4d,
        metadata_df=selected_metadata,
        trajectories_df=wellbores_df,
        surface_picks=surface_picks,
        prod_data=prod_data,
        color=color,
        layer_name=layer_name,
        label="Production",
    )

    print(well_layer)
    print()

    # Get injection wells for a selected field
    layer_name = "injection"
    color = "green"
    pdm_data = pdm_provider.get_pdm_wellbores(field_name=field)
    pdm_data_df = pdm_data.dataframe
    prod_data = pdm_data_df[pdm_data_df["PURPOSE"] == layer_name]
    interval_4d = ["2020-10-01-2019-10-01"]

    well_layer = create_new_well_layer(
        interval_4d=interval_4d,
        metadata_df=selected_metadata,
        trajectories_df=wellbores_df,
        surface_picks=surface_picks,
        prod_data=prod_data,
        color=color,
        layer_name=layer_name,
        label="Injection",
    )

    print(well_layer)
    print()

    layer_name = "drilled_wells"
    label = "Drilled wells"
    interval_4d = None
    color = "black"
    md_start = 0
    md_end = ""

    well_layer = create_new_well_layer(
        interval_4d=interval_4d,
        metadata_df=selected_metadata,
        trajectories_df=wellbores_df,
        color=color,
        layer_name=layer_name,
        label=label,
    )

    print(well_layer)

    layer_name = "reservoir_sections"
    color = "black"

    well_layer = create_new_well_layer(
        interval_4d=None,
        metadata_df=selected_metadata,
        trajectories_df=wellbores_df,
        surface_picks=surface_picks,
        prod_data=None,
        color=color,
        layer_name=layer_name,
        label="Reservoir sections",
    )

    print(well_layer)
    print()


if __name__ == "__main__":
    main()
