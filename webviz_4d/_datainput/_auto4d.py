import os
import glob
import argparse
import json
import numpy as np
import pandas as pd
from pprint import pprint

from webviz_4d._datainput.common import (
    read_config,
)


def load_metadata(auto4d_dir):
    metadata = pd.DataFrame()
    file_ext = ".json"
    json_files = glob.glob(auto4d_dir + "/.*" + file_ext)

    names = []
    attributes = []
    times1 = []
    times2 = []
    filenames = []

    headers = [
        "data.name",
        "data.attribute",
        "data.time.t1",
        "data.time.t2",
        "filename",
    ]

    for json_file in json_files:
        # Opening JSON file
        with open(json_file) as json_file:
            json_data = json.load(json_file)

            surface = json_data.get("OW Top Horizon")
            names.append(surface[3:11])

            name = json_data.get("OW Horizon name")
            filename = os.path.join(auto4d_dir, name + ".gri")
            filenames.append(filename)

            monitor_date = json_data.get("Monitor reference date")
            date_reformat = (
                monitor_date[6:10] + "-" + monitor_date[3:5] + "-" + monitor_date[0:2]
            )
            times2.append(date_reformat)

            base_date = json_data.get("Base reference date")
            date_reformat = (
                base_date[6:10] + "-" + base_date[3:5] + "-" + base_date[0:2]
            )
            times1.append(date_reformat)

            seismic_content = json_data.get("Seismic content")
            horizon_content = json_data.get("Horizon content")
            attribute = seismic_content + "_" + horizon_content
            attributes.append(attribute)

    zipped_list = list(
        zip(
            names,
            attributes,
            times1,
            times2,
            filenames,
        )
    )

    metadata = pd.DataFrame(zipped_list, columns=headers)
    metadata.fillna(value=np.nan, inplace=True)

    metadata["fmu_id.realization"] = "---"
    metadata["fmu_id.iteration"] = "---"
    metadata["map_type"] = "observed"
    metadata["statistics"] = ""

    print("Metadata overview")
    print(metadata)

    return metadata


def create_auto4d_lists(metadata, interval_mode):
    selectors = {
        "data.attribute": "attribute",
        "data.name": "name",
        "interval": "interval",
        "fmu_id.iteration": "iteration",
        "fmu_id.realization": "realization",
    }

    map_types = ["observed"]
    map_dict = {}

    for map_type in map_types:
        map_dict[map_type] = {}
        map_type_dict = {}

        map_type_metadata = metadata[metadata["map_type"] == map_type]

        intervals_df = map_type_metadata[["data.time.t1", "data.time.t2"]]
        intervals = []

        for key, value in selectors.items():
            selector = key
            map_type_metadata = metadata[metadata["map_type"] == map_type]
            map_type_dict[value] = {}

            if selector == "interval":
                for _index, row in intervals_df.iterrows():
                    t1 = row["data.time.t1"]
                    t2 = row["data.time.t2"]

                    if interval_mode == "normal":
                        interval = t2 + "-" + t1
                    else:
                        interval = t1 + "-" + t2

                    if interval not in intervals:
                        intervals.append(interval)

                # sorted_intervals = sort_intervals(intervals)
                sorted_intervals = intervals

                map_type_dict[value] = sorted_intervals
            else:
                items = list(map_type_metadata[selector].unique())
                items.sort()

                map_type_dict[value] = items

        map_dict[map_type] = map_type_dict

    return map_dict


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
    auto4d_dir = shared_settings.get("auto4d_directory")
    interval_mode = shared_settings.get("interval_mode", "normal")
    # markdown_file = os.path.join(config_folder, "content.md")

    # metadata_file = shared_settings.get("surface_metadata_file")
    # metadata_file = os.path.join(config_folder, metadata_file)

    # print("Surface metadata_file", metadata_file)

    # if os.path.isfile(metadata_file):
    #     os.remove(metadata_file)
    #     print("  - file removed")

    metadata = load_metadata(auto4d_dir)

    # Create selectors
    selectors = create_auto4d_lists(metadata, interval_mode)
    pprint(selectors)


if __name__ == "__main__":
    main()
