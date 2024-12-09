import os
import time
import argparse
from pprint import pprint

from webviz_4d._datainput.common import read_config, print_metadata
from webviz_4d._datainput._fmu import load_fmu_metadata, create_fmu_lists


def main():
    """Load metadata for all observed timelapse maps from a FMU case on /project"""
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
    fmu = shared_settings.get("fmu")

    if fmu:
        fmu_dir = fmu.get("directory")
        field_name = shared_settings.get("field_name")
        observed_maps = shared_settings.get("observed_maps")
        map_dir = observed_maps.get("map_directories")[0]

        interval_mode = shared_settings.get("interval_mode", "normal")

        metadata = load_fmu_metadata(fmu_dir, map_dir, field_name)
        print_metadata(metadata)

        print()
        print("Create auto4d selection lists ...")
        selectors = create_fmu_lists(metadata, interval_mode)
        pprint(selectors)

    else:
        print("ERROR fmu not found in configuration file")


if __name__ == "__main__":
    main()
