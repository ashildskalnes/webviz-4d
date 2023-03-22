import os
import argparse
import json

from fmu.sumo.explorer import Explorer
from webviz_4d._datainput._sumo import (
    create_selector_lists,
)

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
    sumo_name = shared_settings.get("sumo_name")

    sumo = Explorer(env="prod")
    my_case = sumo.cases.filter(name=sumo_name)[0]
    print(f"{my_case.name}: {my_case.uuid}")

    # Some case info
    print(my_case.field)
    print(my_case.status)
    print(my_case.user)

    # Create selectors
    mode = "timelapse"
    selectors = create_selector_lists(my_case, mode)
    print((json.dumps(selectors, indent=2)))


if __name__ == "__main__":
    main()
