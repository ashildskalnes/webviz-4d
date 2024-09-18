import os
import time
import argparse
from pprint import pprint

from fmu.sumo.explorer import Explorer

from webviz_4d._datainput.common import read_config
from webviz_4d._datainput._sumo import (
    create_sumo_lists,
    load_sumo_observed_metadata,
    get_sumo_interval_list,
    get_selected_surface,
)


def main():
    """Load metadata for all observed timelapse maps from a FMU case in Sumo"""
    description = "Compile metadata for all observed 4D maps"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("config_file", help="Enter path to the configuration file")

    args = parser.parse_args()
    config_file = args.config_file

    config = read_config(config_file)
    config_file = os.path.abspath(config_file)
    config_folder = os.path.dirname(config_file)
    config_folder = os.path.abspath(config_folder)

    shared_settings = config.get("shared_settings")
    sumo = shared_settings.get("sumo")

    field_name = shared_settings.get("field_name")
    interval_mode = shared_settings.get("interval_mode", "normal")

    sumo = shared_settings.get("sumo")
    case_name = sumo.get("case_name")
    env = sumo.get("env_name")

    sumo_explorer = Explorer(env=env)
    cases = sumo_explorer.cases.filter(name=case_name)

    if len(cases) == 1:
        my_case = cases[0]
    else:
        print("ERROR: Number of selected cases =", len(cases))
        print("       Execution stopped")
        exit(1)

    field_name = my_case.field
    print("SUMO case:", my_case.name, field_name)
    print("Searching for observed timelapse maps in Sumo ...")

    metadata = load_sumo_observed_metadata(my_case)
    print(metadata)

    selection_list = create_sumo_lists(metadata, interval_mode)
    pprint(selection_list)

    map_type = "observed"
    name = "draupne_fm_1"
    seismic = "amplitude"
    attribute = "min"
    interval = "2023-09-15-2020-10-01"
    difference = "NotTimeshifted"
    interval_list = get_sumo_interval_list(interval)
    time1 = interval_list[0]
    time2 = interval_list[1]

    selected_row = metadata[
        (metadata["name"] == name)
        & (metadata["seismic"] == seismic)
        & (metadata["attribute"] == attribute)
        & (metadata["difference"] == difference)
        & (metadata["time.t1"] == time1)
        & (metadata["time.t2"] == time2)
    ]

    tagname = selected_row["tagname"].values[0]
    print("tagname", tagname)

    ensemble = None
    real = None

    surface = get_selected_surface(
        case=my_case,
        map_type=map_type,
        surface_name=name,
        attribute=tagname,
        time_interval=interval_list,
        iteration_name=ensemble,
        realization=real,
    )

    print(surface)


if __name__ == "__main__":
    main()
