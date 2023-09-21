import pandas as pd
import argparse
import logging
from fmu.sumo.explorer import Explorer
from webviz_4d._datainput._sumo import print_sumo_objects, get_sumo_top_res_surface
from webviz_4d._datainput.common import read_config


def main():
    logging.getLogger("").setLevel(level=logging.WARNING)
    description = "Check SUMO Top res surface"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("config_file")
    args = parser.parse_args()

    config_file = args.config_file
    config = read_config(config_file)
    shared_settings = config.get("shared_settings")
    surface_info = shared_settings.get("top_res_surface")
    sumo_name = shared_settings.get("sumo_name")
    sumo = Explorer(env="prod", keep_alive="15m")

    my_case = sumo.cases.filter(name=sumo_name)[0]
    print(f"{my_case.name}: {my_case.uuid}")

    top_res_surface = get_sumo_top_res_surface(my_case, surface_info)

    if top_res_surface:
        print(top_res_surface)


if __name__ == "__main__":
    main()
