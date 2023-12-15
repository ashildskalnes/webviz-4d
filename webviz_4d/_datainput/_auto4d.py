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


def load_metadata(auto4d_dir, file_ext, acquisition_dates):
    metadata = pd.DataFrame()

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

    if file_ext == ".json":
        metadata_files = glob.glob(auto4d_dir + "/.*" + file_ext)

        for metadata_file in metadata_files:
            # Opening JSON file
            with open(metadata_file) as metadata_file:
                metadata = json.load(metadata_file)
                name = metadata.get("OW Horizon name")
                surface_name = metadata.get("OW Top Horizon")
                monitor_date = metadata.get("Monitor reference date")
                base_date = metadata.get("Base reference date")
                seismic_content = metadata.get("Seismic content")
                horizon_content = metadata.get("Horizon content")

                filename = os.path.join(auto4d_dir, name + ".map")
                time1 = base_date[6:10] + "-" + base_date[3:5] + "-" + base_date[0:2]
                time2 = (
                    monitor_date[6:10]
                    + "-"
                    + monitor_date[3:5]
                    + "-"
                    + monitor_date[0:2]
                )
                attribute = seismic_content + "_" + horizon_content

                names.append(surface_name[3:11])
                filenames.append(filename)
                attributes.append(attribute)
                times1.append(time1)
                times2.append(time2)
    elif file_ext == ".a4dmeta":
        acquisitions = acquisition_dates
        metadata_files = glob.glob(auto4d_dir + "/*" + file_ext)

        for metadata_file in metadata_files:
            with open(metadata_file) as metadata_file:
                metadata = json.load(metadata_file)
                name = metadata.get("output_file").replace(".map", "")
                name_parts = name.split("_")
                surface_name = metadata.get("OSDU_Top_Horizon")
                horizon_content = name_parts[-1]
                process_info = metadata.get("Process_inputs")
                segy = process_info.get("segy")
                segy_name = segy.get("name")
                segy_name_parts = segy_name.split("_")

                interval_4d = segy_name_parts[2]
                time1 = str(acquisitions.get(interval_4d[6:10]))
                time2 = str(acquisitions.get(interval_4d[:4]))
                seismic_content = segy_name_parts[3]

                attribute = seismic_content + "_" + horizon_content
                filename = os.path.join(auto4d_dir, name + ".map")

                names.append(surface_name[3:11])
                filenames.append(filename)
                attributes.append(attribute)
                times1.append(time1)
                times2.append(time2)
    else:
        print("ERROR: Unsupported file extension", file_ext)
        return metadata

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
    auto4d = shared_settings.get("auto4d")
    auto4d_dir = auto4d.get("folder")
    interval_mode = shared_settings.get("interval_mode", "normal")

    # My metadata format
    # file_ext = ".json"
    # dates = None
    # metadata = load_metadata(auto4d_dir, file_ext, dates)

    # Auto4d metadata format
    file_ext = ".a4dmeta"
    dates_file = "js_acquisition_dates.yaml"
    dates = read_config(os.path.join(auto4d_dir, dates_file))
    acquisitions = dates.get("acquisitions")
    metadata = load_metadata(auto4d_dir, file_ext, acquisitions)

    # Create selectors
    selectors = create_auto4d_lists(metadata, interval_mode)
    pprint(selectors)


if __name__ == "__main__":
    main()
