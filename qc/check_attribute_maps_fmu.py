import os
import numpy as np
import time
import argparse
from pprint import pprint
import xtgeo

from webviz_4d._datainput.common import read_config, print_metadata
from webviz_4d._datainput._fmu import load_fmu_metadata, create_fmu_lists


def get_fmu_filename(data, ensemble, real, map_type, surface_metadata):
    selected_interval = data["date"]
    name = data["name"]
    attribute = data["attr"]

    time2 = selected_interval[0:10]
    time1 = selected_interval[11:]

    surface_metadata.replace(np.nan, "", inplace=True)

    try:
        selected_metadata = surface_metadata[
            (surface_metadata["difference"] == real)
            & (surface_metadata["seismic"] == ensemble)
            & (surface_metadata["map_type"] == map_type)
            & (surface_metadata["time1"] == time1)
            & (surface_metadata["time2"] == time2)
            & (surface_metadata["name"] == name)
            & (surface_metadata["attribute"] == attribute)
        ]

        path = selected_metadata["filename"].values[0]

    except:
        path = ""
        print("WARNING: selected map not found. Selection criteria are:")
        print(map_type, real, ensemble, name, attribute, time1, time2)
        print(selected_metadata)

    return path


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

        data_source = "FMU"
        attribute = "max"
        name = "total"
        map_type = "observed"
        seismic = "amplitude"
        difference = "NotTimeshifted"
        interval = "2021-05-15-2020-10-01"

        ensemble = seismic
        real = difference
        data = {"attr": attribute, "name": name, "date": interval}

        data_source = "FMU"
        surface_file = get_fmu_filename(data, ensemble, real, map_type, metadata)
        # print("DEBUG surface_file", surface_file)

        if os.path.isfile(surface_file):
            surface = xtgeo.surface_from_file(surface_file)
            print(surface)
            surface.quickplot(title=surface_file)

    else:
        print("ERROR fmu not found in configuration file")


if __name__ == "__main__":
    main()
