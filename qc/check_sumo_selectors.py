import pandas as pd
import argparse
import json
from fmu.sumo.explorer import Explorer
import fmu.sumo.explorer._utils as explorer_utils
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

    my_case = sumo.get_case_by_name(sumo_name)
    print(f"{my_case.name}: {my_case.sumo_id}")

    # Some case info
    print(my_case.field_name)
    print(my_case.status)
    print(my_case.user)

    # Create selectors
    selectors = create_selector_lists(my_case, mode)

    print((json.dumps(selectors, indent=2)))

    # realization_list = selectors["simulated"]["realization"]

    # if "aggregated" in selectors.keys():
    #     aggregations = selectors["aggregated"]["aggregation"]
    #     realization_list = realization_list + aggregations

    # print("aggregations")
    # print(aggregations)


if __name__ == "__main__":
    main()
