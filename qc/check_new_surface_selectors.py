import os
import pandas as pd
import numpy as np
import argparse
from pprint import pprint
import time
import xtgeo

from webviz_4d._datainput.common import read_config
from webviz_4d._datainput._osdu import (
    get_osdu_metadata_selectors,
    load_surface_from_osdu_new,
)
from webviz_4d._providers.osdu_provider._provider_impl_file import DefaultOsduService


def main():
    DESCRIPTION = ""
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument("config_file", help="Enter name of configuration file")
    args = parser.parse_args()

    config_file = args.config_file
    config = read_config(config_file)
    field_name = config.get("shared_settings").get("field_name")

    surface_viewer = config["layout"][0]["content"][1]["content"][0]["content"][0][
        "SurfaceViewer4D"
    ]
    selectors = surface_viewer.get("selectors")
    pprint(selectors, sort_dicts=False)

    osdu_service = DefaultOsduService()
    all_metadata, selection_list = get_osdu_metadata_selectors(
        config,
        osdu_service,
        selectors,
        field_name,
    )
    print(all_metadata)
    print(selection_list)

    map_idx = 0

    map_defaults = [
        surface_viewer["map1_defaults"],
        surface_viewer["map2_defaults"],
        surface_viewer["map3_defaults"],
    ]

    data = {
        "attr": "RMS",
        "name": "FullReservoirEnvelope",
        "date": "2022-05-15-2020-09-30",
    }
    ensemble = "Amplitude"
    real = "NotTimeshifted"
    map_default = map_defaults[0]

    fixed_keys = list(map_default.keys())
    fixed_values = list(map_default.values())
    fixed_dict = dict(zip(fixed_keys, fixed_values))

    print("DEBUG map_defaults:")
    print(map_defaults)

    metadata_fixed = all_metadata[
        (all_metadata[fixed_keys[0]] == fixed_values[0])
        & (all_metadata[fixed_keys[1]] == fixed_values[1])
        & (all_metadata[fixed_keys[2]] == fixed_values[2])
        & (all_metadata[fixed_keys[3]] == fixed_values[3])
        & (all_metadata[fixed_keys[4]] == fixed_values[4])
    ]

    print(metadata_fixed[["map_name", "interval"]])

    surface, map_name = load_surface_from_osdu_new(
        map_idx,
        all_metadata,
        selectors,
        data,
        ensemble,
        real,
    )

    print(map_name)
    print(surface)


if __name__ == "__main__":
    main()
