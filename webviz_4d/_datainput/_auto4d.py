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


def load_auto4d_metadata(
    auto4d_dir, file_ext, md_version, selections, acquisition_dates
):
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

    metadata_files = glob.glob(auto4d_dir + "/*" + file_ext)

    if file_ext == ".a4dmeta":
        for metadata_file in metadata_files:
            selection_status_list = []

            # Opening metadata file
            with open(metadata_file) as meta_file:
                metadata = json.load(meta_file)

                # Check metadata version
                metadata_version = metadata.get("MetadataVersion")

                if metadata_version is None:
                    print("ERROR: Metadata version not found", metadata_file)
                    status = False
                elif metadata_version != md_version:
                    print("ERROR: Wrong metadata version", metadata_file)
                    print(
                        "       Expected version, Actual version",
                        md_version,
                        metadata_version,
                    )
                    status = False
                else:
                    if selections:
                        for key, value in selections.items():
                            map_value = metadata.get("AttributeMap").get(key)

                            if map_value in value:
                                status = True
                            else:
                                status = False

                            selection_status_list.append(status)

                        if False in selection_status_list:
                            status = False
                    else:
                        status = True

                if status:
                    name = metadata.get("AttributeMap").get("Name")
                    window_mode = metadata.get("CalculationWindow").get("WindowMode")

                    if window_mode == "BetweenHorizons":
                        surface_name = metadata.get("CalculationWindow").get(
                            "TopHorizonName"
                        )
                    elif window_mode == "AroundHorizon":
                        surface_name = metadata.get("CalculationWindow").get(
                            "HorizonName"
                        )
                    else:
                        print("WARNING: WindowMode not supported", window_mode)
                        surface_name = "Dummy"

                    seismic_content = metadata.get("AttributeMap").get(
                        "SeismicTraceContent"
                    )
                    horizon_content = metadata.get("AttributeMap").get("AttributeType")
                    difference_type = metadata.get("AttributeMap").get(
                        "SeismicDifference"
                    )
                    base_seismic = metadata.get("SeismicProcessingTraces").get(
                        "BaseSeismicTraces"
                    )
                    time1 = acquisition_dates.get(base_seismic[:7])
                    monitor_seismic = metadata.get("SeismicProcessingTraces").get(
                        "MonitorSeismicTraces"
                    )
                    time2 = acquisition_dates.get(monitor_seismic[:7])

                    filename = os.path.join(auto4d_dir, name + ".gri")
                    attribute = (
                        seismic_content + "_" + difference_type + "_" + horizon_content
                    )

                    names.append(surface_name)
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
    auto4d_dir = auto4d.get("directory")
    interval_mode = shared_settings.get("interval_mode", "normal")
    selections = auto4d.get("selections")

    acquisition_dates = auto4d.get("acquisition_dates")
    md_version = auto4d.get("metadata_version")

    # Auto4d metadata format and version
    file_ext = ".a4dmeta"

    metadata = load_auto4d_metadata(
        auto4d_dir, file_ext, md_version, selections, acquisition_dates
    )

    # Create selectors
    selectors = create_auto4d_lists(metadata, interval_mode)

    print("Selectors")
    pprint(selectors)


if __name__ == "__main__":
    main()
