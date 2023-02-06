import pandas as pd
import argparse
import glob
from fmu.sumo.explorer import Explorer
import fmu.sumo.explorer._utils as explorer_utils
from webviz_4d._datainput._polygons import load_sumo_polygons
from webviz_4d._datainput._sumo import print_sumo_objects


def main():
    description = "Test SUMO polygons"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("sumo_name")
    parser.add_argument("polygon_name")
    args = parser.parse_args()

    sumo_name = args.sumo_name
    polygon_name = args.polygon_name
    sumo = Explorer(env="prod")

    my_case = sumo.cases.filter(name=sumo_name)[0]
    print(f"{my_case.name}: {my_case.id}")

    # Some case info
    print(my_case.field)
    print(my_case.status)
    print(my_case.user)

    # Load polygons from sumo
    iter_id = 0
    real_id = 0

    sumo_polygons = my_case.get_objects(
        object_type="polygons",
        object_names=[polygon_name],
        tag_names=[],
        iteration_ids=[iter_id],
        realization_ids=[real_id],
    )

    print_sumo_objects(sumo_polygons)

    polygon_layers = load_sumo_polygons(sumo_polygons, sumo, None)

    if polygon_layers:
        for polygon_layer in polygon_layers:
            print(polygon_layer["name"])


if __name__ == "__main__":
    main()
