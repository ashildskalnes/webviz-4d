import os
import time
import argparse
from pprint import pprint

from fmu.sumo.explorer import Explorer

from webviz_4d._datainput.common import read_config, print_metadata
from webviz_4d._datainput._sumo import (
    create_sumo_lists,
    load_sumo_observed_metadata,
    get_sumo_interval_list,
    get_selected_surface,
    get_sumo_tagname,
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
    print_metadata(metadata)

    selection_list = create_sumo_lists(metadata, interval_mode)
    pprint(selection_list)

    attribute = "max"
    name = "draupne_fm_1"
    map_type = "observed"
    seismic = "amplitude"
    difference = "NotTimeshifted"
    interval = "2023-05-15-2022-05-15"

    interval_list = get_sumo_interval_list(interval)
    tagname = get_sumo_tagname(
        metadata, name, seismic, attribute, difference, interval_list
    )

    ensemble = None
    real = None

    print("Loading surface from SUMO")
    print(" - tagname", tagname)

    start_time = time.time()
    surface = get_selected_surface(
        case=my_case,
        map_type=map_type,
        surface_name=name,
        attribute=tagname,
        time_interval=interval_list,
        iteration_name=ensemble,
        realization=real,
    )
    print(" --- %s seconds ---" % (time.time() - start_time))
    number_cells = surface.nrow * surface.ncol
    print(f"Surface size: {surface.nrow} x {surface.ncol} = {number_cells}")

    print(surface)


if __name__ == "__main__":
    main()
