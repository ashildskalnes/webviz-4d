import os
import argparse

from fmu.sumo.explorer import Explorer

from webviz_4d._datainput.common import read_config
from webviz_4d._datainput._polygons import load_sumo_polygons
from webviz_4d._datainput._sumo import print_sumo_objects


def main():
    description = "Test SUMO polygons"
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
    print(my_case.iterations)

    # Load polygons from sumo
    iter_name = my_case.iterations[0].get("name")
    real_id = 0

    sumo_polygons = my_case.polygons.filter(iteration=iter_name, realization=real_id)

    print_sumo_objects(sumo_polygons)

    polygon_layers = load_sumo_polygons(sumo_polygons, None)

    if polygon_layers:
        for polygon_layer in polygon_layers:
            print(polygon_layer["name"])


if __name__ == "__main__":
    main()
