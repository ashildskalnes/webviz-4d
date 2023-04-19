import os
import glob
import argparse
import pandas as pd
import re

from fmu.sumo.explorer import Explorer
from fmu.sumo.explorer.timefilter import TimeType, TimeFilter
from webviz_4d._datainput.common import read_config

valid_statistics = ["mean", "std", "p10", "p50", "p90", "min", "max"]


def find_number(surfacepath, txt):
    """Return the first number found in a part of a filename (surface)"""
    filename = str(surfacepath)
    number = None
    index = str(filename).find(txt)

    if index > 0:
        i = index + len(txt) + 1
        j = filename[i:].find(os.sep)

        if j == -1:  # Statistics
            j = filename[i:].find("--")

        number = filename[i : i + j]

    return number


def get_metadata(surface_file):
    delimiter = "--"
    ind = []
    blocked_dirs = ["tmp_rec", "tmp_prm", "maps_backup", "difference_from_reference"]

    real_id = None
    iter_name = None
    time_lapse = False

    for blocked_dir in blocked_dirs:
        if blocked_dir in surface_file:
            map_type = None

            return map_type, iter_name, real_id, time_lapse

    if "observations" in surface_file and not "realization" in surface_file:
        map_type = "observed"
    elif "realization" in surface_file:
        map_type = "realization"
        real_id = find_number(surface_file, "realization")

        real_ind = surface_file.find("realization") + 1
        ind1 = surface_file.find(os.sep, real_ind)
        ind2 = surface_file.find(os.sep, ind1 + 1)

        if ind1 > 0 and ind2 > 0:
            iter_name = surface_file[ind1 + 1 : ind2]
    else:
        map_type = "aggregated"

    for m in re.finditer(delimiter, str(surface_file)):
        ind.append(m.start())

    if len(ind) >= 2 and len(str(surface_file)) > ind[1] + 19:
        date1 = str(surface_file)[ind[1] + 2 : ind[1] + 10]
        date2 = str(surface_file)[ind[1] + 11 : ind[1] + 19]

        if date1[0:1] in "12" and date2[0:1] in "12":
            time_lapse = True

    return map_type, iter_name, real_id, time_lapse


def get_settings(config_file):
    config_file = os.path.abspath(config_file)
    config_folder = os.path.dirname(config_file)

    config = read_config(config_file)
    settings_file = config.get("shared_settings").get("settings_file")
    settings_file = os.path.join(config_folder, settings_file)
    settings = read_config(settings_file)

    return settings, config_folder


def get_absolute_paths(sumo_objects, fmu_dir):
    absolute_paths = []

    for sumo_object in sumo_objects:
        full_path = os.path.join(
            fmu_dir, sumo_object.metadata.get("file").get("relative_path")
        )
        abs_path = os.path.abspath(full_path)
        absolute_paths.append(abs_path)

    return absolute_paths


def compare_surface_collections(metadata, surfaces, sumo_surfaces, fmu_dir, mode):
    """mode == "sumo" => collect the files that are on disk but not in sumo
    mode == "meta" => collect the files that are on disk but not in the metadata file
    """
    missing_files = []

    if mode == "sumo":
        if len(surfaces) > len(sumo_surfaces):
            sumo_filepaths = get_absolute_paths(sumo_surfaces, fmu_dir)

            for surface in surfaces:
                if surface not in sumo_filepaths:
                    missing_files.append(surface)
                    # print("WARNING:", surface, "not found in SUMO")
    elif mode == "meta":
        if len(surfaces) > len(metadata.index):
            metadata_filenames = metadata["filename"].to_list()
            for surface in surfaces:
                if surface not in metadata_filenames:
                    missing_files.append(surface)
                    # print("WARNING:", surface, "not found in metadata file")
    else:
        print("ERROR: Unknown mode:", mode)

    return missing_files


def main():
    description = "Compare surfaces on disk and sumo"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("config_file", help="config file")
    parser.add_argument("sumo_id", help="sumo_id")
    parser.add_argument("--full", help="Scan mode (optional, default=False)")

    args = parser.parse_args()
    print(args)

    # map_types_list = ["observed", "realizations", "aggregated"]

    config_file = args.config_file
    sumo_id = args.sumo_id
    scan_mode = args.full

    config_file = os.path.abspath(config_file)
    config_folder = os.path.dirname(config_file)

    config = read_config(config_file)
    shared_settings = config.get("shared_settings")

    surface_metadata_file = shared_settings.get("surface_metadata_file")
    surface_metadata_file = os.path.join(config_folder, surface_metadata_file)

    fmu_dir = shared_settings["fmu_directory"]

    sumo = Explorer(env="prod")
    my_case = sumo.cases.filter(uuid=sumo_id)[0]

    # Some case info
    print("SUMO:")
    print(f"  {my_case.name}: {my_case.uuid}")
    print(" ", my_case.field)
    print(" ", my_case.status)
    print(" ", my_case.user)

    if scan_mode:
        txt = "Full"
    else:
        txt = "realization-0"

    print("Searching for all surface files in", fmu_dir, txt, "...")

    all_surface_files = glob.glob(
        fmu_dir + os.sep + "**" + os.sep + "*.gri", recursive=True
    )
    print("All surface files in", fmu_dir, len(all_surface_files))

    observed_surfaces = []
    observed_timelapse_surfaces = []
    realization_surfaces = []
    realization_timelapse_surfaces = []
    aggregated_surfaces = []
    aggregated_timelapse_surfaces = []
    iter_names = []

    for surface_file in all_surface_files:
        status = True
        map_type, iter_name, real_id, time_lapse = get_metadata(surface_file)

        if map_type == "observed":
            observed_surfaces.append(surface_file)

            if time_lapse:
                observed_timelapse_surfaces.append(surface_file)
        elif map_type == "realization" and "observations" not in surface_file:
            if not scan_mode:
                status = bool(real_id == "0")

            if status:
                realization_surfaces.append(surface_file)

                if time_lapse:
                    realization_timelapse_surfaces.append(surface_file)

                    if iter_name not in iter_names:
                        iter_names.append(iter_name)

        elif map_type == "aggregated":
            if "numreal" not in surface_file:
                aggregated_surfaces.append(surface_file)

                if time_lapse:
                    aggregated_timelapse_surfaces.append(surface_file)

    print(
        "  Observed surfaces:",
        len(observed_surfaces),
        "TL",
        len(observed_timelapse_surfaces),
    )
    print(
        "  Realization surfaces:",
        len(realization_surfaces),
        "TL",
        len(realization_timelapse_surfaces),
    )
    print(
        "  Aggregated surfaces:",
        len(aggregated_surfaces),
        "TL",
        len(aggregated_timelapse_surfaces),
    )
    print("  Iterations:", sorted(iter_names))

    print("Loading metadata from:", surface_metadata_file)
    surface_metadata = pd.read_csv(surface_metadata_file, low_memory=False)

    observed_metadata = surface_metadata[surface_metadata["map_type"] == "observed"]
    print("  Observed timelapse surfaces", len(observed_metadata.index))

    missing_observed_tl_files = compare_surface_collections(
        observed_metadata, observed_timelapse_surfaces, None, fmu_dir, "meta"
    )
    print("    Missing observed TL surfaces", len(missing_observed_tl_files))

    realization_metadata = surface_metadata[surface_metadata["map_type"] == "simulated"]

    # If scan_mode == False, search only for realization-0
    real_string = "realization"
    if not scan_mode:
        real_string = real_string + "-0"

    realization_metadata = realization_metadata[
        realization_metadata["fmu_id.realization"].str.contains(real_string)
    ]
    print("  Realization timelapse surfaces", len(realization_metadata.index))

    missing_realization_tl_files = compare_surface_collections(
        realization_metadata, realization_timelapse_surfaces, None, fmu_dir, "meta"
    )
    print("    Missing realization TL surfaces", len(missing_realization_tl_files))

    aggregated_metadata = surface_metadata[
        ~surface_metadata["fmu_id.realization"].str.contains("realization")
    ]
    print("  Aggregated timelapse surfaces", len(aggregated_metadata.index))

    missing_aggregated_tl_files = compare_surface_collections(
        aggregated_metadata, aggregated_timelapse_surfaces, None, fmu_dir, "meta"
    )
    print("    Missing Aggregated TL surfaces", len(missing_aggregated_tl_files))

    time = TimeFilter(
        time_type=TimeType.INTERVAL,
    )

    print("Sumo surfaces")
    # Observed surfaces
    observed_sumo_surfaces = my_case.surfaces.filter(stage="case")
    observed_sumo_timelapse_surfaces = observed_sumo_surfaces.filter(time=time)
    print(
        "Observed surfaces",
        len(observed_sumo_surfaces),
        "TL",
        len(observed_sumo_timelapse_surfaces),
    )

    missing_observed_sumo_files = compare_surface_collections(
        None, observed_surfaces, observed_sumo_surfaces, fmu_dir, "sumo"
    )
    print("  Missing observed Sumo surfaces", len(missing_observed_sumo_files))

    missing_observed_sumo_TL_files = compare_surface_collections(
        None,
        observed_timelapse_surfaces,
        observed_sumo_timelapse_surfaces,
        fmu_dir,
        "sumo",
    )
    print("  Missing observed Sumo TL surfaces", len(missing_observed_sumo_TL_files))

    # Realization surfaces
    if scan_mode:
        realization_sumo_surfaces = my_case.surfaces.filter(stage="realization")
    else:
        realization_sumo_surfaces = my_case.surfaces.filter(realization=["0"])

    realization_sumo_timelapse_surfaces = realization_sumo_surfaces.filter(time=time)
    print(
        "Realization surfaces",
        len(realization_sumo_surfaces),
        "TL",
        len(realization_sumo_timelapse_surfaces),
    )

    missing_realization_sumo_files = compare_surface_collections(
        None, realization_surfaces, realization_sumo_surfaces, fmu_dir, "sumo"
    )
    print("  Missing realization Sumo surfaces", len(missing_realization_sumo_files))

    missing_realization_sumo_TL_files = compare_surface_collections(
        None,
        realization_timelapse_surfaces,
        realization_sumo_timelapse_surfaces,
        fmu_dir,
        "sumo",
    )
    print(
        "  Missing realization Sumo TL surfaces", len(missing_realization_sumo_TL_files)
    )

    # Aggregated surfaces
    aggregated_sumo_surfaces = my_case.surfaces.filter(stage="iteration")
    aggregated_sumo_timelapse_surfaces = aggregated_sumo_surfaces.filter(time=time)
    print(
        "  Aggregated surfaces",
        len(aggregated_sumo_surfaces),
        "TL",
        len(aggregated_sumo_timelapse_surfaces),
    )

    missing_aggregated_sumo_files = compare_surface_collections(
        None, aggregated_surfaces, aggregated_sumo_surfaces, fmu_dir, "sumo"
    )
    print("  Missing aggregated Sumo surfaces", len(missing_aggregated_sumo_files))

    missing_aggregated_sumo_TL_files = compare_surface_collections(
        None,
        aggregated_timelapse_surfaces,
        aggregated_sumo_timelapse_surfaces,
        fmu_dir,
        "sumo",
    )
    print(
        "  Missing aggregated Sumo TL surfaces", len(missing_aggregated_sumo_TL_files)
    )


if __name__ == "__main__":
    main()
