import os
import numpy as np
import pandas as pd
import numpy.ma as ma
import argparse
from typing import Optional
from io import BytesIO
import xtgeo
import xtgeo.cxtgeo._cxtgeo as _cxtgeo
import logging
import matplotlib.pyplot as plt
from scipy.interpolate import interp1d

from fmu.sumo.explorer import Explorer

from webviz_4d._providers.wellbore_provider._provider_impl_file import (
    ProviderImplFile,
)


def get_surface_picks(
    wellbores_df: pd.DataFrame(),
    surface_names: list,
    surfaces: list,
):
    """get Surface picks (stolen and adjusted from xtgeo)"""

    surface_picks = pd.DataFrame()

    if surfaces is None:
        print("WARNING: No surfaces loaded")
        return surface_picks

    wellbore_names = wellbores_df["unique_wellbore_identifier"].unique()

    md_values = []
    tvd_values = []
    x_values = []
    y_values = []
    surface_pick_names = []
    wellbores = []

    for wellbore in wellbore_names:
        wellbore_df = wellbores_df[
            wellbores_df["unique_wellbore_identifier"] == wellbore
        ]

        xcor = wellbore_df["easting"].values
        ycor = wellbore_df["northing"].values
        zcor = wellbore_df["tvd_msl"].values
        mcor = wellbore_df["md"].values

        for i in range(0, len(surface_names)):
            surface_name = surface_names[i]
            surface = surfaces[i]

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
                tvd_msl_value = zres[0]
                x_value = xres[0]
                y_value = yres[0]
            else:
                md_value = np.nan
                tvd_msl_value = np.nan
                x_value = np.nan
                y_value = np.nan

            wellbores.append(wellbore)
            surface_pick_names.append(surface_name)
            x_values.append(x_value)
            y_values.append(y_value)
            tvd_values.append(tvd_msl_value)
            md_values.append(md_value)

    surface_picks = pd.DataFrame()
    surface_picks["unique_wellbore_identifier"] = wellbores
    surface_picks["surface_pick"] = surface_pick_names
    surface_picks["easting"] = x_values
    surface_picks["northing"] = y_values
    surface_picks["tvd_msl"] = tvd_values
    surface_picks["md"] = md_values

    return surface_picks


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


def open_surface_with_xtgeo(sumo_bytestring):
    if sumo_bytestring:
        surface = xtgeo.surface_from_file(sumo_bytestring)
    else:
        surface = None
        print("WARNING: non-existing sumo_bytestring")

    return surface


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


def get_fencespec(wellbore_name, wellbore_df, sampling, nextend, tvdmin):
    """Create a XTGeo fence spec from polyline coordinates"""
    poly = xtgeo.Polygons()
    poly.dataframe = wellbore_df[["easting", "northing", "tvd_msl"]]
    poly.dataframe.columns = ["X_UTME", "Y_UTMN", "Z_TVDSS"]
    poly.dataframe["POLY_ID"] = 1
    poly.dataframe["NAME"] = "polyline"
    # print(poly.dataframe)

    if tvdmin is not None:
        poly.dataframe = poly.dataframe[poly.dataframe[poly.zname] >= tvdmin]
        poly.dataframe.reset_index(drop=True, inplace=True)

    try:
        fence = poly.get_fence(distance=sampling, nextend=nextend, asnumpy=True)
    except ValueError as error:
        print("ERROR:", error)
        print("     Wellbore:", wellbore_name)
        fence = None

    if isinstance(fence, bool):
        fence = None

    return fence


def create_fence_dataframe(fence, start_md):
    x_fence = []
    y_fence = []
    md_fence = []
    tvd_fence = []
    dist_fence = []

    i = 0
    for item in fence:
        x_val = item[0]
        y_val = item[1]
        tvd_val = item[2]
        hor_dist = item[3]

        if i == 0:
            md_val = start_md
        else:
            a = np.array((x_val, y_val, tvd_val))
            b = np.array((x_fence[-1], y_fence[-1], tvd_fence[-1]))
            md_val = np.linalg.norm(a - b) + md_fence[-1]  # Calculate MD along fence

        x_fence.append(x_val)
        y_fence.append(y_val)
        md_fence.append(md_val)
        tvd_fence.append(tvd_val)
        dist_fence.append(hor_dist)
        i = i + 1

    fence_df = pd.DataFrame()
    fence_df["x"] = x_fence
    fence_df["y"] = y_fence
    fence_df["md"] = md_fence
    fence_df["tvd"] = tvd_fence
    fence_df["hor_dist"] = dist_fence

    return fence_df


def main():
    description = "Plot wellbore xsection"
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

    # Get wellbore uuids and names for all smda_wellbores in a selected field
    all_wellbores = smda_provider.get_smda_wellbores(field, None)
    smda_wellbores = all_wellbores[all_wellbores["completion_date"].isnull() == False]

    surface_names = [
        "Draupne Fm. 1 JS Top",
        "VIKING GP. Base",
        "HEGRE GP. Top",
    ]

    wellbore_pick_names = surface_names
    surface_pick_names = surface_names

    wellbore_picks = smda_provider.get_wellbore_picks(
        field_name=field, pick_identifiers=wellbore_pick_names, interpreter="STAT"
    )

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
                print(
                    "WARNING: Pick name",
                    wellbore_pick,
                    "not found for wellbore",
                    wellbore_name,
                )
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

    print(wellbore_picks_df)
    print(surface_picks)

    wellbore_names = (
        wellbore_picks_df["unique_wellbore_identifier"].values.tolist()
        + surface_picks["unique_wellbore_identifier"].values.tolist()
    )
    myset = set(wellbore_names)
    wellbore_names = sorted(myset)

    pd.set_option("display.max_rows", None)

    for surface_name in surface_names:
        print(surface_name)

        selected_wellbore_picks = wellbore_picks_df[
            wellbore_picks_df["pick_identifier"] == surface_name
        ]
        selected_surface_picks = surface_picks[
            surface_picks["surface_pick"] == surface_name
        ]

        wellbore_picks_mds = []
        wellbore_picks_tvds = []

        surface_picks_mds = []
        surface_picks_tvds = []

        difference_mds = []
        difference_tvds = []

        for wellbore in wellbore_names:
            try:
                wellbore_pick = selected_wellbore_picks[
                    selected_wellbore_picks["unique_wellbore_identifier"] == wellbore
                ]
                wellbore_pick_md = wellbore_pick["md"].values[0]
                wellbore_pick_tvd = wellbore_pick["tvd_msl"].values[0]
            except:
                wellbore_pick_md = None
                wellbore_pick_tvd = None
            try:
                surface_pick = selected_surface_picks[
                    selected_surface_picks["unique_wellbore_identifier"] == wellbore
                ]
                surface_pick_md = surface_pick["md"].values[0]
                surface_pick_tvd = surface_pick["tvd_msl"].values[0]
            except:
                surface_pick_md = None
                surface_pick_tvd = None

            if wellbore_pick_md and surface_pick_md:
                difference_md = wellbore_pick_md - surface_pick_md
            else:
                difference_md = None

            if wellbore_pick_tvd and surface_pick_tvd:
                difference_tvd = wellbore_pick_tvd - surface_pick_tvd
            else:
                difference_tvd = None

            wellbore_picks_mds.append(wellbore_pick_md)
            wellbore_picks_tvds.append(wellbore_pick_tvd)

            surface_picks_mds.append(surface_pick_md)
            surface_picks_tvds.append(surface_pick_tvd)

            difference_mds.append(difference_md)
            difference_tvds.append(difference_tvd)

        picks_overview = pd.DataFrame()
        picks_overview["unique_wellbore_identifier"] = wellbore_names
        picks_overview["wellbore_pick_md"] = wellbore_picks_mds
        picks_overview["wellbore_pick_tvd"] = wellbore_picks_tvds
        picks_overview["surface_pick_md"] = surface_picks_mds
        picks_overview["surface_pick_tvd"] = surface_picks_tvds
        picks_overview["md_differences"] = difference_mds
        picks_overview["tvd_differences"] = difference_tvds
        print(picks_overview)

    sampling = 20
    nextend = 0
    tvd_depth = [1800, 2000]

    for _index, row in all_wellbores.iterrows():
        wellbore_name = row["unique_wellbore_identifier"]
        # print(wellbore_name)
        trajectories = all_trajectories.dataframe
        wellbore_trajectory = trajectories[
            trajectories["unique_wellbore_identifier"] == wellbore_name
        ]

        if (
            wellbore_trajectory["tvd_msl"].max()
            > tvd_depth[0]
            # and wellbore_name == "NO 16/2-G-3 H"
        ):
            tvd_value = 1200
            wellbore_tvd = wellbore_trajectory["tvd_msl"].values
            ind = min(
                range(len(wellbore_tvd)),
                key=lambda i: abs(wellbore_tvd[i] - tvd_value),
            )
            selected_row = wellbore_trajectory.iloc[ind]
            start_md = selected_row["md"]
            start_tvd = selected_row["tvd_msl"]

            last_row = wellbore_trajectory.iloc[-1]
            last_md = last_row["md"]

            fence = get_fencespec(
                wellbore_name, wellbore_trajectory, sampling, nextend, start_tvd
            )

            fence_df = create_fence_dataframe(fence, start_md)
            x_fence = fence_df["x"].values
            y_fence = fence_df["y"].values
            md_fence = fence_df["md"].values
            tvd_fence = fence_df["tvd"].values
            dist_fence = fence_df["hor_dist"].values

            # Check first TVD values
            if abs(tvd_fence[0] - start_tvd) > 1:
                print(
                    "WARNING: Large TVD difference",
                    wellbore_name,
                    start_tvd,
                    tvd_fence[0],
                )

            # Don't plot (almost) vertical wells
            if dist_fence[-1] > 500:
                # Create horizontal interpolator (from md to horizontal distance)
                dist_interp = interp1d(md_fence[nextend:], dist_fence[nextend:])

                surface_lines = []

                for surface in depth_surfaces:
                    surface_line = surface.get_randomline(fence, hincrement=sampling)
                    surface_lines.append(surface_line)

                title = "Wellbore " + wellbore_name + " (tvdmin=" + str(start_tvd) + ")"

                fig = plt.figure()
                fig.set_figheight(8)
                fig.set_figwidth(15)
                plt.title(title)
                plt.plot(
                    dist_fence[nextend:],
                    tvd_fence[nextend:],
                    color="black",
                    marker="*",
                    label="Well trajectory",
                )

                # for i in range(0, len(dist)):
                #     plt.text(dist[i], tvd[i], str(i))

                for index, surface_line in enumerate(surface_lines):
                    x_surface = []
                    y_surface = []

                    for item in surface_line:
                        x = item[0]
                        y = item[1]
                        x_surface.append(x)
                        y_surface.append(y)

                    plt.plot(
                        x_surface[nextend:],
                        y_surface[nextend:],
                        label=sumo_names[index],
                    )
                ax = plt.gca()
                plt.grid(True)
                ax.set_ylim([tvd_depth[1], tvd_depth[0]])
                plt.xlabel("Horizontal distance [m] ")
                plt.ylabel("TVD_MSL [m]")

                selected_wellbore_picks = wellbore_picks_df[
                    wellbore_picks_df["unique_wellbore_identifier"] == wellbore_name
                ]

                for pick_name in wellbore_pick_names:
                    selected_pick = selected_wellbore_picks[
                        selected_wellbore_picks["pick_identifier"] == pick_name
                    ]
                    pick = selected_pick[selected_pick["obs_no"] == 1]

                    if not pick.empty:
                        wb_pick_md = pick["md"].values[0]
                        wb_pick_tvd = pick["tvd_msl"].values[0]

                        try:
                            wb_dist_est = dist_interp(wb_pick_md) - dist_fence[0]
                        except ValueError as error:
                            print("WARNING:")
                            print("    Wellbore:", wellbore_name)
                            print(
                                "    Wellbore pick",
                                pick_name,
                                wb_pick_md,
                                wb_pick_tvd,
                            )
                            print("   ", error)
                            wb_dist_est = None

                        if wb_dist_est is not None:
                            # print("Wellbore pick", pick_name, wb_pick_md, wb_pick_tvd)
                            plt.plot(
                                wb_dist_est,
                                wb_pick_tvd,
                                "*",
                                label="Wellbore: " + pick_name,
                            )

                selected_surface_picks = surface_picks[
                    surface_picks["unique_wellbore_identifier"] == wellbore_name
                ]

                for pick_name in surface_pick_names:
                    selected_pick = selected_surface_picks[
                        selected_surface_picks["surface_pick"] == pick_name
                    ]

                    if not selected_pick.empty:
                        surf_pick_md = selected_pick["md"].values[0]

                        if not np.isnan(surf_pick_md):
                            surf_pick_tvd = selected_pick["tvd_msl"].values[0]

                            try:
                                surf_dist_est = (
                                    dist_interp(surf_pick_md) - dist_fence[0]
                                )

                                # print(
                                #     "Surface pick",
                                #     pick_name,
                                #     "{:.2f}".format(surf_pick_md),
                                #     "{:.2f}".format(surf_pick_tvd),
                                # )

                            except ValueError as error:
                                print("WARNING:")
                                print("    Wellbore:", wellbore_name)
                                print(
                                    "    Surface pick",
                                    pick_name,
                                    surf_pick_md,
                                    surf_pick_tvd,
                                )
                                print("   ", error)
                                surf_dist_est = None

                            if surf_dist_est is not None:
                                # print("Wellbore pick", pick_name, pick_md, pick_tvd)
                                plt.plot(
                                    surf_dist_est,
                                    surf_pick_tvd,
                                    "*",
                                    label="Surface: " + pick_name,
                                )

                plt.legend()
                # plt.show()
                png_file = wellbore_name.replace(" ", "_").replace("/", "_") + ".png"
                plt.savefig(png_file)
                plt.close()


if __name__ == "__main__":
    main()
