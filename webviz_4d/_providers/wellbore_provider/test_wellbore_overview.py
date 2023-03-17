import os
import numpy as np
import pandas as pd
import time
import argparse
from typing import Optional
from io import BytesIO
import xtgeo
import xtgeo.cxtgeo._cxtgeo as _cxtgeo
import logging

from fmu.sumo.explorer import Explorer

from webviz_4d._providers.wellbore_provider._provider_impl_file import (
    ProviderImplFile,
)


def get_sumo_depth_surfaces(
    env: str, sumo_case_id: str, surface_names: list, aggregations: Optional[str] = []
):
    sumo = Explorer(env=env)
    my_case = sumo.get_case_by_id(sumo_case_id)
    print("Case name:", my_case.name)
    print("Depth surfaces:", surface_names)

    sumo_names, sumo_bytestrings = get_sumo_bytestrings(
        case=my_case,
        names=surface_names,
        attribute="depth_structural_model",
        aggregations=aggregations,
    )

    depth_surfaces = []

    for bytestring in sumo_bytestrings:
        depth_surface = open_surface_with_xtgeo(bytestring)
        depth_surfaces.append(depth_surface)

    return sumo_names, depth_surfaces


def get_sumo_bytestrings(
    case: str,
    names: list,
    attribute: str,
    times: Optional[str] = [],
    ensemble_name: Optional[str] = [],
    realization_names: Optional[str] = [],
    aggregations: Optional[str] = "",
):
    # Replace with code from sumo
    ensemble_ids = [0]

    # Replace with code from sumo
    if realization_names is not None:
        realization_ids = []
        for realization in realization_names:
            realization_id = int(realization[-1:])
            realization_ids.append(realization_id)

    ensemble_ids = ensemble_ids
    realization_ids = realization_ids
    surface_attributes = [attribute]
    surface_names = names
    surface_time_intervals = times
    aggregations = aggregations

    sumo_bytestrings = []

    # print("surface_names", surface_names)
    # print("surface_attributes", surface_attributes)
    # print("surface_time_interval", surface_time_intervals)
    # print("ensemble_ids", ensemble_ids)
    # print("realization_ids", realization_ids)
    # print("aggregations", aggregations)

    sumo_surface_names = []
    try:
        surfaces = case.get_objects(
            object_type="surface",
            object_names=surface_names,
            tag_names=surface_attributes,
            time_intervals=surface_time_intervals,
            iteration_ids=ensemble_ids,
            realization_ids=realization_ids,
            aggregations=aggregations,
        )

        for s in surfaces:
            sumo_bytestrings.append(BytesIO(s.blob))
            sumo_surface_names.append(s.name)

    except:
        sumo_bytestrings = []
        print(
            "Warning: Surfaces not found:",
            names,
            attribute,
            times,
            ensemble_name,
            realization_names,
            aggregations,
        )

    return sumo_surface_names, sumo_bytestrings


def open_surface_with_xtgeo(sumo_bytestring):
    if sumo_bytestring:
        surface = xtgeo.surface_from_file(sumo_bytestring)
    else:
        surface = None
        print("WARNING: non-existing sumo_bytestring")

    return surface


def get_surface_picks(wellbores_df, surface_names, surfaces):
    """get Surface picks (stolen and adjusted from xtgeo)"""

    surface_picks = pd.DataFrame()

    if surfaces is None:
        print("WARNING: No surfaces loaded")
        return surface_picks

    wellbore_names = wellbores_df["unique_wellbore_identifier"].unique()

    m = len(wellbore_names)
    n = len(surface_names)
    md_values = np.empty([m, n], dtype=float)

    i = 0
    for wellbore in wellbore_names:
        wellbore_df = wellbores_df[
            wellbores_df["unique_wellbore_identifier"] == wellbore
        ]
        xcor = wellbore_df["easting"].values
        ycor = wellbore_df["northing"].values
        zcor = wellbore_df["tvd_msl"].values
        mcor = wellbore_df["md"].values

        j = 0
        for surface in surfaces:
            surface_name = surface_names[j]

            nval, xres, yres, zres, mres, dres = _cxtgeo.well_surf_picks(
                xcor,
                ycor,
                zcor,
                mcor,
                surface.ncol,
                surface.nrow,
                surface.xori,
                surface.yori,
                surface.xinc,
                surface.yinc,
                surface.yflip,
                surface.rotation,
                surface.npvalues1d,
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

            md_values[i][j] = md_value
            j = j + 1

        i = i + 1

    surface_picks["unique_wellbore_identifier"] = wellbore_names

    for i in range(0, len(surface_names)):
        column_name = surface_names[i] + " md_value"
        surface_md_values = md_values[:, i]
        surface_picks[column_name] = surface_md_values

    return surface_picks


def main():
    description = "Create wellbore_overview"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("field_name", help="Enter field name")
    args = parser.parse_args()
    print(args)

    logging.getLogger("").setLevel(level=logging.WARNING)

    field = args.field_name.upper()
    omnia_env = ".omniaapi"
    home = os.path.expanduser("~")
    env_path = os.path.expanduser(os.path.join(home, omnia_env))

    smda_provider = ProviderImplFile(env_path, "SMDA")
    ssdl_provider = ProviderImplFile(env_path, "SSDL")
    pdm_provider = ProviderImplFile(env_path, "PDM")

    # Get wellbore uuids and names for all smda_wellbores in a selected field
    all_wellbores = smda_provider.get_smda_wellbores(field, None)
    smda_wellbores = all_wellbores[all_wellbores["completion_date"].isnull() == False]

    johan_sverdrup_pick_names = [
        "Draupne Fm. 1 JS Top",
        "VIKING GP. Base",
        "HEGRE GP. Top",
    ]

    grane_pick_names = ["Sele Fm. Base"]

    troll_pick_names = ["Sognefjord Fm. Top"]

    field_picks = {
        "JOHAN SVERDRUP": johan_sverdrup_pick_names,
        "GRANE": grane_pick_names,
        "TROLL": troll_pick_names,
    }

    wellbore_pick_names = field_picks.get(field.upper())

    surface_names = [
        "Draupne Fm. 1 JS Top",
        "VIKING GP. Base",
        "HEGRE GP. Top",
    ]

    surface_pick_names = surface_names

    # Get selected wellbore picks
    start = time.time()
    wellbore_picks = smda_provider.get_wellbore_picks(
        field_name=field, pick_identifiers=wellbore_pick_names, interpreter="STAT"
    )

    pd.set_option("display.max_rows", None)
    wellbore_picks_df = wellbore_picks.dataframe.sort_values(
        by=["unique_wellbore_identifier", "md"]
    )
    # print(wellbore_picks_df)

    for wellbore_pick in wellbore_pick_names:
        md_values_list = []

        for _index, row in smda_wellbores.iterrows():
            md_value = None
            wellbore_name = row["unique_wellbore_identifier"]

            try:
                selected_pick = wellbore_picks_df[
                    (wellbore_picks_df["unique_wellbore_identifier"] == wellbore_name)
                    & (wellbore_picks_df["pick_identifier"] == wellbore_pick)
                    & (wellbore_picks_df["obs_no"] == 1)
                ]
                md_value = selected_pick["md"].to_list()[0]
            except:
                # print(
                #     "WARNING: Pick name",
                #     wellbore_pick,
                #     "not found for wellbore",
                #     wellbore_name,
                # )
                pass

            md_values_list.append(md_value)

        smda_wellbores[wellbore_pick] = md_values_list

    print("Extracting surfaces from SUMO ...")
    env = "prod"
    sumo_case_id = "f37593b1-4e3b-684e-46a1-4211b09cf197"
    sumo_names, depth_surfaces = get_sumo_depth_surfaces(
        env, sumo_case_id, surface_names, aggregations=["mean"]
    )

    # Load all trajectories for drilled wellbores
    all_trajectories = smda_provider.drilled_trajectories(
        field_name=field,
    )
    # Extract picks for the first wellbore crossing
    surface_picks = get_surface_picks(
        all_trajectories.dataframe, sumo_names, depth_surfaces
    )
    print(surface_picks)

    # Get all PDM wellbore names
    pdm_wellbores = pdm_provider.get_pdm_wellbores(field_name=field, pdm_wellbores=None)

    if pdm_wellbores.dataframe.empty:
        print("ERROR: No production wells received from PDM, execution stopped")
        exit()

    for index, row in pdm_wellbores.dataframe.iterrows():
        pdm_well_name = row["WB_UWBI"]

        try:
            wellbore = smda_wellbores.loc[
                smda_wellbores["unique_wellbore_identifier"] == pdm_well_name
            ]

            drill_start_date = wellbore["drill_start_date"].to_numpy()[0]

            # Check if wellbore is drilled
            if drill_start_date is None:
                wellbore_name = wellbore["unique_wellbore_identifier"].to_numpy()[0]
                total_depth_driller_tvd = wellbore[
                    "total_depth_driller_tvd"
                ].to_numpy()[0]
                print(
                    "WARNING: PDM wellbore:",
                    wellbore_name,
                    "probably not drilled, total_depth_tvd =",
                    str(total_depth_driller_tvd),
                )
        except:
            wellbore_name = wellbore["unique_wellbore_identifier"].to_numpy()[0]
            print(wellbore_name, " is not in SMDA")

    if not pdm_wellbores.dataframe.empty:
        field_uuid = smda_wellbores["field_uuid"].unique()[0]
        print("\nGet completion data for all ssdl_wellbores ...")
        completions = ssdl_provider.get_wellbore_completions(field_uuid)

        if completions:
            # print(completions.dataframe)
            completion_info = completions.dataframe[
                [
                    "wellbore_id",
                    "wellbore_uuid",
                    "well_id",
                    "symbol_name",
                    "description",
                    "md_top",
                    "md_bottom",
                ]
            ]
            completion_wellbores = completion_info["wellbore_id"].unique().tolist()

            screen_intervals = completion_info[
                completion_info["symbol_name"].str.contains(
                    "Screen", case=False, na=False
                )
            ].copy()

            screen_tops = screen_intervals.groupby("wellbore_id")["md_top"].min()
            screen_bottoms = screen_intervals.groupby("wellbore_id")["md_bottom"].max()

        # Get perforation data for alle smda_wellbores
        print("\nGet perforation data for all ssdl_wellbores ...")
        perforations = ssdl_provider.get_wellbore_perforations(
            field_uuid=field_uuid,
        )

        if perforations:
            # print(perforations.dataframe)
            perforation_info = perforations.dataframe[
                [
                    "wellbore_id",
                    "wellbore_uuid",
                    "well_id",
                    "status",
                    "md_top",
                    "md_bottom",
                ]
            ]
            perforation_tops = perforation_info.groupby("wellbore_id")["md_top"].min()
            perforation_bottoms = perforation_info.groupby("wellbore_id")[
                "md_bottom"
            ].max()
            perforation_wellbores = perforation_info["wellbore_id"].unique().tolist()

        # Add SSDL and PDM names to wellbore overview
        all_pdm_names = []
        all_ssdl_names = []
        all_screen_tops = []
        all_screen_bottoms = []
        all_perforation_tops = []
        all_perforation_bottoms = []

        for _index, wellbore in smda_wellbores.iterrows():
            wellbore_name = wellbore["unique_wellbore_identifier"]
            ssdl_name = None
            screen_top = None
            screen_bottom = None
            perforation_top = None
            perforation_bottom = None

            if wellbore_name in completion_wellbores:
                ssdl_info = completion_info[
                    completion_info["wellbore_id"] == wellbore_name
                ]
                ssdl_name = ssdl_info["well_id"].unique()[0]
                screen_top = screen_tops.get(wellbore_name, default=None)
                screen_bottom = screen_bottoms.get(wellbore_name, default=None)

            if wellbore_name in perforation_wellbores:
                ssdl_info = perforation_info[
                    perforation_info["wellbore_id"] == wellbore_name
                ]
                ssdl_name = ssdl_info["well_id"].unique()[0]
                perforation_top = perforation_tops.get(wellbore_name, default=None)
                perforation_bottom = perforation_bottoms.get(
                    wellbore_name, default=None
                )

            if ssdl_name and "Y" in ssdl_name:
                ind = ssdl_name.index("Y")
                ssdl_name = ssdl_name[0 : ind + 1]

            try:
                pdm_well = pdm_wellbores[
                    pdm_wellbores["WB_UWBI"].str.contains(ssdl_name)
                ]
                pdm_well_name = pdm_well["WB_UWBI"].to_numpy()[0]
            except:
                pdm_well_name = None

            all_pdm_names.append(pdm_well_name)
            all_ssdl_names.append(ssdl_name)
            all_screen_tops.append(screen_top)
            all_screen_bottoms.append(screen_bottom)
            all_perforation_tops.append(perforation_top)
            all_perforation_bottoms.append(perforation_bottom)

    smda_wellbores["well_id"] = all_ssdl_names
    smda_wellbores["WB_UWBI"] = all_pdm_names
    smda_wellbores["Screen_top"] = all_screen_tops
    smda_wellbores["Screen_bottom"] = all_screen_bottoms
    smda_wellbores["Perforation_top"] = all_perforation_tops
    smda_wellbores["Perforation_bottom"] = all_perforation_bottoms

    end = time.time()
    print("Create wellbore overview:", end - start)

    pd.set_option("display.max_rows", None)
    print(smda_wellbores.sort_values(by=["unique_wellbore_identifier"]))


if __name__ == "__main__":
    main()
