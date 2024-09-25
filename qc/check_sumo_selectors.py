import os
import argparse
import json

from fmu.sumo.explorer import Explorer
from webviz_4d._datainput._sumo import load_sumo_observed_metadata, create_sumo_lists

from webviz_4d._datainput.common import read_config


def main():
    description = "Compile metadata for SUMO surfaces"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("config_file")

    args = parser.parse_args()

    config_file = args.config_file
    config_file = os.path.abspath(config_file)

    config = read_config(config_file)
    shared_settings = config.get("shared_settings")
    sumo_name = shared_settings.get("sumo").get("case_name")

    sumo = Explorer(env="prod")
    my_case = sumo.cases.filter(name=sumo_name)[0]
    print(f"{my_case.name}: {my_case.uuid}")

    # Some case info
    print(my_case.field)
    print(my_case.status)
    print(my_case.user)

    # Create selectors
    interval_mode = shared_settings.get("interval_mode")
    surface_metadata = load_sumo_observed_metadata(my_case)
    selection_list = create_sumo_lists(surface_metadata, interval_mode)
    print((json.dumps(selection_list, indent=2)))


if __name__ == "__main__":
    main()
