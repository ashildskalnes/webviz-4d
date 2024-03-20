import os
import sys
import argparse
import json
import numpy as np
import pandas as pd
from pprint import pprint

if sys.platform == "win32":
    from webviz_4d._datainput._osdu import (
        Config,
        DefaultOsduService,
        extract_osdu_metadata,
        create_osdu_lists,
    )

from webviz_4d._datainput.common import (
    read_config,
)
from webviz_4d._datainput._auto4d import create_auto4d_lists, load_auto4d_metadata


def main():
    """Load metadata for all timelapse maps from OW"""
    description = "Compile metadata for all auto4d maps"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("config_file", help="Enter path to the configuration file")

    args = parser.parse_args()
    config_file = args.config_file

    config = read_config(config_file)
    config_file = os.path.abspath(config_file)
    config_folder = os.path.dirname(config_file)
    config_folder = os.path.abspath(config_folder)

    shared_settings = config.get("shared_settings")
    interval_mode = shared_settings.get("interval_mode", "normal")
    auto4d = shared_settings.get("auto4d")
    osdu = shared_settings.get("osdu")

    if auto4d:
        auto4d_dir = auto4d.get("directory")
        acquisition_dates = auto4d.get("acquisition_dates")
        selections = auto4d.get("selections")
        mdata_version = auto4d.get("metadata_version")
        file_ext = ".a4dmeta"
        metadata = load_auto4d_metadata(
            auto4d_dir, file_ext, mdata_version, selections, acquisition_dates
        )
        # Create selectors
        selectors = create_auto4d_lists(metadata, interval_mode)

    if osdu:
        acquisition_dates = []
        mdata_version = osdu.get("metadata_version")
        osdu_service = DefaultOsduService()  # type: ignore
        metadata = extract_osdu_metadata(osdu_service, mdata_version)
        print(metadata[["name", "attribute", "seismic", "time.t1","time.t2","coverage"]])
        
        # Create selectors
        selectors = create_osdu_lists(metadata, interval_mode)

    print("Selectors")
    pprint(selectors)


if __name__ == "__main__":
    main()