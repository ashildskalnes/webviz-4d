import pandas as pd
import argparse
import logging
from fmu.sumo.explorer import Explorer
from webviz_4d._datainput._metadata import get_realization_id
from webviz_4d._datainput._sumo import (
    create_selector_lists,
    get_polygon_name,
    print_sumo_objects,
)
from webviz_4d._datainput._polygons import get_fault_polygon_tag, load_sumo_polygons


def main():
    logging.getLogger("").setLevel(level=logging.WARNING)
    description = "Connect SUMO polygons with availble surface names"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("sumo_name")
    parser.add_argument("default_name")
    args = parser.parse_args()

    sumo_name = args.sumo_name
    default_name = args.default_name
    sumo = Explorer(env="prod")

    my_case = sumo.cases.filter(name=sumo_name)[0]
    print(f"{my_case.name}: {my_case.uuid}")

    # Some case info
    print(my_case.field)
    print(my_case.status)
    print(my_case.user)

    realization = "realization-1"
    real_id = get_realization_id(realization)

    iter_name = my_case.iterations[0].get("name")

    print("Create selectors lists:")
    selectors = create_selector_lists(my_case, "timelapse")

    if selectors:
        simulated_names = selectors.get("simulated").get("name")
        observed_names = selectors.get("observed").get("name")
        all_names = simulated_names + observed_names
        set_res = set(all_names)
        list_res = list(set_res)
        surface_names = list(filter(lambda item: item is not None, list_res))

        # Search for polygon with same name as the actual surface
        for surface_name in surface_names:
            sumo_polygons = my_case.polygons.filter(
                iteration=iter_name, realization=real_id
            )

            if len(sumo_polygons) > 0:
                polygon_name = get_polygon_name(
                    sumo_polygons, surface_name, default_name
                )

                if polygon_name is None:
                    polygon_name = "VOLANTIS GP. Top"

                print(surface_name, ":", polygon_name)

                sumo_polygons = my_case.polygons.filter(
                    name=polygon_name, iteration=iter_name, realization=real_id
                )

                polygon_layers = load_sumo_polygons(sumo_polygons, None)

                for layer in polygon_layers:
                    layer_name = layer["name"]
                    print("  ", layer_name)

        fault_polygon_tag = get_fault_polygon_tag(sumo_polygons)
        print("Sumo fault tag name", fault_polygon_tag)


if __name__ == "__main__":
    main()
