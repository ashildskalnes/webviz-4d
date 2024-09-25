import os
import time
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
    auto4d_dir, file_ext, mdata_version, selections, acquisition_dates
):
    all_metadata = pd.DataFrame()
    surface_names = []
    attributes = []
    times1 = []
    times2 = []
    seismic_contents = []
    coverages = []
    differences = []
    filenames = []
    field_names = []

    headers = [
        "name",
        "attribute",
        "time.t1",
        "time.t2",
        "seismic",
        "coverage",
        "difference",
        "filename",
        "field_name",
    ]

    # Search for all metadata files
    start_time = time.time()
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
                elif metadata_version != mdata_version:
                    print("ERROR: Wrong metadata version", metadata_file)
                    print(
                        "       Expected version, Actual version",
                        mdata_version,
                        metadata_version,
                    )
                    status = False
                else:
                    if selections:
                        for key, value in selections.items():
                            map_value = metadata.get(key)

                            if map_value == value:
                                status = True
                            else:
                                status = False

                            selection_status_list.append(status)

                        if False in selection_status_list:
                            status = False
                    else:
                        status = True

                if status:
                    name = metadata.get("Name")
                    field_name = metadata.get("FieldName")
                    attribute_type = metadata.get("AttributeExtractionType")
                    seismic_content = metadata.get("SeismicTraceAttribute")
                    coverage = metadata.get("SeismicCoverage")
                    difference = metadata.get("SeismicDifferenceType")

                    if type(difference) is float:
                        difference = "---"

                    seismic_traces = metadata.get("SeismicTraceDataSourceNames")
                    seismic_horizons = metadata.get("HorizonSourceNames")
                    seismic_horizon = seismic_horizons[0]
                    time1 = str(acquisition_dates.get(seismic_traces[1]))
                    time2 = str(acquisition_dates.get(seismic_traces[0]))
                    filename = os.path.join(auto4d_dir, name + ".gri")

                    surface_names.append(seismic_horizon)
                    attributes.append(attribute_type)
                    times1.append(time1)
                    times2.append(time2)
                    seismic_contents.append(seismic_content)
                    coverages.append(coverage)
                    differences.append(difference)
                    filenames.append(filename)
                    field_names.append(field_name)

    else:
        print("ERROR: Unsupported file extension", file_ext)
        return all_metadata

    print(" --- %s seconds ---" % (time.time() - start_time))
    print()

    zipped_list = list(
        zip(
            surface_names,
            attributes,
            times1,
            times2,
            seismic_contents,
            coverages,
            differences,
            filenames,
            field_names,
        )
    )

    all_metadata = pd.DataFrame(zipped_list, columns=headers)
    all_metadata.fillna(value=np.nan, inplace=True)
    all_metadata["map_type"] = "observed"

    print("DEBUG auto4d_metadata")
    print(all_metadata)

    return all_metadata


def create_auto4d_lists(metadata, interval_mode):
    # Metadata 0.4.2
    selectors = {
        "name": "name",
        "interval": "interval",
        "attribute": "attribute",
        "seismic": "seismic",
        "difference": "difference",
    }

    map_types = ["observed"]
    map_dict = {}

    for map_type in map_types:
        map_dict[map_type] = {}
        map_type_dict = {}

        map_type_metadata = metadata[metadata["map_type"] == map_type]

        intervals_df = map_type_metadata[["time.t1", "time.t2"]]
        intervals = []

        for key, value in selectors.items():
            selector = key
            map_type_metadata = metadata[metadata["map_type"] == map_type]
            map_type_dict[value] = {}

            if selector == "interval":
                for _index, row in intervals_df.iterrows():
                    t1 = str(row["time.t1"])
                    t2 = str(row["time.t2"])

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
