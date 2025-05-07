import os
import pandas as pd
import numpy as np
import argparse
from pprint import pprint
import time
import xtgeo

from webviz_4d._datainput.common import read_config
from webviz_4d._datainput._auto4d import get_auto4d_metadata_selectors
from webviz_4d._datainput._maps import load_surface_from_file_new


def main():
    DESCRIPTION = ""
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument("config_file", help="Enter name of configuration file")
    args = parser.parse_args()

    config_file = args.config_file
    config = read_config(config_file)
    surface_viewer = config["layout"][0]["content"][1]["content"][0]["content"][0][
        "SurfaceViewer4D"
    ]
    selectors = surface_viewer.get("selectors")
    pprint(selectors, sort_dicts=False)

    map_default = {
        "data_source": "auto4d_file",
        "map_type": "observed",
        "difference_type": "AttributeOfDifference",
        "coverage": "Full",
        "map_dim": "4D",
    }

    all_metadata, selection_list = get_auto4d_metadata_selectors(
        config, selectors, map_default
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
        "date": "2022-05-15-2021-05-15",
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

    surface, map_name = load_surface_from_file_new(
        map_idx,
        selectors,
        metadata_fixed,
        map_default,
        data,
        ensemble,
        real,
    )

    print(map_name)
    print(surface)


if __name__ == "__main__":
    main()
