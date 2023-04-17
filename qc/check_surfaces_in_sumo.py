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


def get_relative_paths(sumo_objects):
    relative_paths = []

    for sumo_object in sumo_objects:
        relative_paths.append(sumo_object.metadata.get("file").get("relative_path"))

    return relative_paths



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

    print("Searching for all surface files in", fmu_dir, scan_mode, "...")

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

    print("  Observed surfaces:", len(observed_surfaces))
    print("     timelapse:", len(observed_timelapse_surfaces))
    print("  Realization surfaces:", len(realization_surfaces))
    print("     timelapse:", len(realization_timelapse_surfaces))
    print("  Aggregated surfaces:", len(aggregated_surfaces))
    print("     timelapse:", len(aggregated_timelapse_surfaces))
    print("  Iterations:", sorted(iter_names))

    print("Loading metadata from:", surface_metadata_file)
    surface_metadata = pd.read_csv(surface_metadata_file, low_memory=False)
    meta_filenames = surface_metadata["filename"].to_list()

    observed_metadata = surface_metadata[surface_metadata["map_type"] == "observed"]
    print("  Observed timelapse surfaces", len(observed_metadata.index))

    if len(observed_metadata.index) != len(observed_timelapse_surfaces):
        for surface in observed_timelapse_surfaces:
            if surface not in meta_filenames:
                print("WARNING:", surface, "not found in metadata file")

    realization_metadata = surface_metadata[surface_metadata["map_type"] == "simulated"]

    realization_metadata = realization_metadata[
        realization_metadata["fmu_id.realization"].str.contains("realization")
    ]
    print("  Realization timelapse surfaces", len(realization_metadata.index))

    if len(realization_metadata.index) != len(realization_timelapse_surfaces):
        for surface in realization_timelapse_surfaces:
            if surface not in meta_filenames:
                print("WARNING:", surface, "not found in metadata file")

    aggregated_metadata = surface_metadata[
        ~surface_metadata["fmu_id.realization"].str.contains("realization")
    ]
    print("  Aggregated timelapse surfaces", len(aggregated_metadata.index))

    if len(aggregated_metadata.index) != len(aggregated_timelapse_surfaces):
        for surface in aggregated_timelapse_surfaces:
            if surface not in meta_filenames:
                print("WARNING:", surface, "not found in metadata file")

    time = TimeFilter(
        time_type=TimeType.INTERVAL,
    )

    print("Sumo surfaces")

    observed_sumo_surfaces = my_case.surfaces.filter(stage="case")
    print("  Observed surfaces", len(observed_sumo_surfaces))

    if len(observed_surfaces) > len(observed_sumo_surfaces):
        sumo_filepaths = get_relative_paths(observed_sumo_surfaces)

        for surface in observed_surfaces:
            if surface not in sumo_filepaths:
                print("WARNING:", surface, "not found in SUMO")

    observed_sumo_timelapse_surfaces = observed_sumo_surfaces.filter(time=time)
    print("    Observed timelapse surfaces", len(observed_sumo_timelapse_surfaces))

    realization_sumo_surfaces = my_case.surfaces.filter(stage="realization")
    print("  Realization surfaces", len(realization_sumo_surfaces))

    if len(realization_surfaces) > len(realization_sumo_surfaces):
        sumo_filepaths = get_relative_paths(realization_sumo_surfaces)

        for surface in realization_surfaces:
            if surface not in sumo_filepaths:
                print("WARNING:", surface, "not found in SUMO")

    realization_sumo_timelapse_surfaces = realization_sumo_surfaces.filter(time=time)
    print(
        "    Realization timelapse surfaces",
        len(realization_sumo_timelapse_surfaces),
    )

    aggregated_sumo_surfaces = my_case.surfaces.filter(stage="iteration")
    print("  Aggregated surfaces", len(aggregated_sumo_surfaces))

    aggregated_sumo_timelapse_surfaces = aggregated_sumo_surfaces.filter(time=time)
    print(
        "    Aggregated timelapse surfaces",
        len(aggregated_sumo_timelapse_surfaces),
    )

    if len(aggregated_surfaces) > len(aggregated_sumo_surfaces):
        sumo_filepaths = get_relative_paths(aggregated_sumo_surfaces)

        for surface in aggregated_sumo_surfaces:
            if surface not in sumo_filepaths:
                print("WARNING:", surface, "not found in SUMO")


if __name__ == "__main__":
    main()
