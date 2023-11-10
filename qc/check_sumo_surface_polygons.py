import os
import pandas as pd
import argparse
import logging
from fmu.sumo.explorer import Explorer

from webviz_4d._datainput.common import (
    read_config,
)
from webviz_4d._datainput._metadata import get_realization_id

from webviz_4d._datainput._sumo import (
    create_selector_lists,
    get_polygon_name,
    print_sumo_objects,
)
from webviz_4d._datainput._polygons import load_sumo_polygons

from webviz_4d._datainput._sumo import (
    create_selector_lists,
)


def main():
    logging.getLogger("").setLevel(level=logging.WARNING)
    description = "Connect SUMO polygons with availble surface names"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("config_file")

    args = parser.parse_args()

    config_file = args.config_file
    config_file = os.path.abspath(config_file)

    config = read_config(config_file)

    shared_settings = config.get("shared_settings")
    sumo_name = shared_settings.get("sumo_name")

    env = "prod"
    sumo = Explorer(env=env)
    my_case = sumo.cases.filter(name=sumo_name)[0]
    print("SUMO case:", my_case.name)

    top_res_surface_info = shared_settings.get("top_reservoir")
    print(top_res_surface_info)

    realization = "realization-0"
    real_id = get_realization_id(realization)
    iter_name = my_case.iterations[0].get("name")
    sumo_polygons = my_case.polygons.filter(iteration=iter_name, realization=real_id)
    print_sumo_objects(sumo_polygons)

    print("Create selectors lists:")
    selectors = create_selector_lists(my_case, "timelapse")

    if selectors:
        simulated_names = selectors.get("simulated").get("name")
        observed_names = selectors.get("observed").get("name")
        all_names = simulated_names + observed_names
        set_res = set(all_names)
        list_res = list(set_res)
        surface_names = list(filter(lambda item: item is not None, list_res))

        # Search for associated polygon defined by the usage status
        # related to zones and observed/aggregated surfaces
        map_types = ["simulated", "observed", "aggregated"]

        top_res_surface_settings = config.get("shared_settings").get("top_reservoir")
        ensemble = iter_name
        real = realization

        default_polygon_name = top_res_surface_settings.get("name")
        default_polygon_iter = top_res_surface_settings.get("iter")
        default_polygon_real = top_res_surface_settings.get("real")

        polygon_usage = top_res_surface_settings.get("polygon_usage")

        zones_settings = polygon_usage.get("zones")

        for map_type in map_types:
            print("Map_type", map_type)

            for surface_name in surface_names:
                print("Surface = ", surface_name)

                polygon_layers = None
                iteration = None
                realization = None

                if len(sumo_polygons) > 0:
                    polygon_name = get_polygon_name(sumo_polygons, surface_name)

                    if polygon_name is None and zones_settings:
                        polygon_name = default_polygon_name

                    if polygon_name:
                        if map_type == "simulated":
                            if "realization" in real:
                                iteration = ensemble
                                realization = real
                            else:
                                aggregated_settings = polygon_usage.get("aggregated")

                                if aggregated_settings:
                                    iteration = ensemble
                                    realization = default_polygon_real
                        else:  # map_type == observed
                            observed_settings = polygon_usage.get("observed")

                            if observed_settings:
                                iteration = default_polygon_iter
                                realization = default_polygon_real

                        if iteration:
                            real_id = get_realization_id(realization)

                            polygons = sumo_polygons.filter(
                                name=polygon_name,
                                iteration=iteration,
                                realization=real_id,
                            )

                            if len(polygons) > 0:
                                polygon_layers = load_sumo_polygons(polygons, None)

                                for layer in polygon_layers:
                                    layer_name = layer["name"]
                                    print("         ", layer_name)
                            else:
                                print("WARNING: No SUMO polygons loaded")


if __name__ == "__main__":
    main()
