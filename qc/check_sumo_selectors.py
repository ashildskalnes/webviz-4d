import pandas as pd
import argparse
import json
from fmu.sumo.explorer import Explorer
from webviz_4d._datainput._sumo import (
    create_selector_lists,
)


def main():
    description = "Compile metadata for SUMO surfaces"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("sumo_name")
    parser.add_argument("mode", nargs="?", default="timelapse")
    args = parser.parse_args()

    mode = args.mode
    sumo_name = args.sumo_name
    sumo = Explorer(env="prod")

    my_case = sumo.cases.filter(name=sumo_name)[0]
    print(f"{my_case.name}: {my_case.uuid}")

    # Some case info
    print(my_case.field)
    print(my_case.status)
    print(my_case.user)

    # Create selectors
    selectors = create_selector_lists(my_case, mode)
    print((json.dumps(selectors, indent=2)))


if __name__ == "__main__":
    main()
