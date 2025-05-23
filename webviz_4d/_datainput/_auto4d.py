import os
import glob
import json
import numpy as np
import pandas as pd
from pprint import pprint


standard_metadata = {
    "strat_name": "StratigraphicZone",
    "extraction_type": "AttributeExtractionType",
    "interval": "interval",
    "content": "SeismicTraceAttribute",
    "difference_type": "AttributeDifferenceType",
    "difference": "SeismicDifferenceType",
    "coverage": "SeismicCoverage",
    "map_type_dimension": "MapTypeDimension",
    "map_name": "Name",
}
extra_metadata = [
    "data_source" "field_name",
    "object_name",
    "filename",
    "id",
    "url",
]


def load_auto4d_metadata_new(auto4d_dir, file_ext, mdata_version, acquisition_dates):

    all_metadata = pd.DataFrame()

    # Search for all metadata files
    metadata_files = glob.glob(auto4d_dir + "/*" + file_ext)

    if file_ext == ".a4dmeta":
        metadata_list = []
        for metadata_file in metadata_files:
            # Opening metadata file
            try:
                f = open(metadata_file)
                metadata = json.load(f)
            except:
                metadata = None

            if metadata:
                # Check metadata version
                metadata_version = metadata.get("MetadataVersion")

                if metadata_version is None:
                    print("ERROR: Metadata version not found", metadata_file)
                elif metadata_version != mdata_version:
                    print("ERROR: Wrong metadata version", metadata_file)
                    print(
                        "       Expected version, Actual version",
                        mdata_version,
                        metadata_version,
                    )
                else:
                    map_name = metadata.get("Name")
                    difference = metadata.get("SeismicDifferenceType")

                    if type(difference) is float:
                        difference = "---"

                    seismic_traces = metadata.get("SeismicTraceDataSourceNames")
                    time1 = str(acquisition_dates.get(seismic_traces[1][:7]))
                    time2 = str(acquisition_dates.get(seismic_traces[0][:7]))
                    interval = time2 + "-" + time1

                    filename = os.path.join(auto4d_dir, map_name + ".gri")

                    map_dict = {
                        "map_name": map_name,
                        "name": metadata.get("StratigraphicZone"),
                        "field_name": metadata.get("FieldName"),
                        "attribute": metadata.get("AttributeExtractionType"),
                        "dates": [time1, time2],
                        "time1": time1,
                        "time2": time2,
                        "interval": interval,
                        "seismic": metadata.get("SeismicTraceAttribute"),
                        "coverage": metadata.get("SeismicCoverage"),
                        "difference": difference,
                        "filename": filename,
                        "field_name": metadata.get("FieldName"),
                        "bin_grid_name": metadata.get("SeismicBinGridName"),
                        "strat_zone": metadata.get("StratigraphicZone"),
                        "map_dim": metadata.get("MapTypeDimension"),
                        "diff_type": metadata.get("AttributeDifferenceType"),
                    }

                    metadata_list.append(map_dict)
    else:
        print("ERROR: Unsupported file extension", file_ext)
        return all_metadata

    all_metadata = pd.DataFrame(metadata_list)

    return all_metadata


def load_auto4d_metadata_selectors(
    auto4d_dir, file_ext, mdata_version, acquisition_dates
):
    # print("config", config)
    # surface_viewer = config["layout"][0]["content"][1]["content"][0]["content"][0][
    #     "SurfaceViewer4D"
    # ]
    # selectors = surface_viewer.get("selectors")

    all_metadata = pd.DataFrame()

    # Search for all metadata files
    metadata_files = glob.glob(auto4d_dir + "/*" + file_ext)

    if file_ext == ".a4dmeta":
        metadata_list = []
        for metadata_file in metadata_files:
            # Opening metadata file
            try:
                f = open(metadata_file)
                metadata = json.load(f)
            except:
                metadata = None

            if metadata:
                # Check metadata version
                metadata_version = metadata.get("MetadataVersion")

                if metadata_version is None:
                    print("ERROR: Metadata version not found", metadata_file)
                elif metadata_version != mdata_version:
                    print("ERROR: Wrong metadata version", metadata_file)
                    print(
                        "       Expected version, Actual version",
                        mdata_version,
                        metadata_version,
                    )
                else:
                    map_name = metadata.get("Name")
                    difference = metadata.get("SeismicDifferenceType")

                    if type(difference) is float:
                        difference = "---"

                    seismic_traces = metadata.get("SeismicTraceDataSourceNames")
                    time1 = str(acquisition_dates.get(seismic_traces[1][:7]))
                    time2 = str(acquisition_dates.get(seismic_traces[0][:7]))
                    interval = time2 + "-" + time1

                    filename = os.path.join(auto4d_dir, map_name + ".gri")

                    map_dict = {
                        "map_name": map_name,
                        "strat_zone": metadata.get("StratigraphicZone"),
                        "field_name": metadata.get("FieldName"),
                        "extraction_type": metadata.get("AttributeExtractionType"),
                        "dates": [time1, time2],
                        "time1": time1,
                        "time2": time2,
                        "interval": interval,
                        "content": metadata.get("SeismicTraceAttribute"),
                        "coverage": metadata.get("SeismicCoverage"),
                        "difference": difference,
                        "filename": filename,
                        "bin_grid_name": metadata.get("SeismicBinGridName"),
                        "map_dim": metadata.get("MapTypeDimension"),
                        "difference_type": metadata.get("AttributeDifferenceType"),
                    }

                    metadata_list.append(map_dict)
    else:
        print("ERROR: Unsupported file extension", file_ext)
        return all_metadata

    all_metadata = pd.DataFrame(metadata_list)
    all_metadata.fillna(value=np.nan, inplace=True)
    all_metadata["data_source"] = "auto4d_file"
    all_metadata["map_type"] = "observed"

    return all_metadata


def create_auto4d_lists(metadata, interval_mode, selectors):
    # Metadata 0.4.2
    # selectors = {
    #     "strat_zone": "name",
    #     "interval": "interval",
    #     "extraction_type": "attribute",
    #     "content": "seismic",
    #     "difference": "difference",
    # }

    map_types = ["observed", "simulated"]
    map_dict = {}

    for map_type in map_types:
        map_dict[map_type] = {}
        map_type_dict = {}

        map_type_metadata = metadata[metadata["map_type"] == map_type]

        intervals_df = map_type_metadata[["time1", "time2"]]
        intervals = []

        for key, value in selectors.items():
            selector = key
            map_type_metadata = metadata[metadata["map_type"] == map_type]
            map_type_dict[value] = {}

            if selector == "interval":
                for _index, row in intervals_df.iterrows():
                    t1 = str(row["time1"])
                    t2 = str(row["time2"])

                    if interval_mode == "normal":
                        interval = t2 + "-" + t1
                    else:
                        interval = t1 + "-" + t2

                    if interval not in intervals:
                        intervals.append(interval)

                # sorted_intervals = sort_intervals(intervals)
                sorted_intervals = sorted(intervals)

                map_type_dict[value] = sorted_intervals
            else:
                items = list(map_type_metadata[selector].unique())
                map_type_dict[value] = sorted(items)

        map_dict[map_type] = map_type_dict

    return map_dict


def create_selectors_list(metadata, selectors, interval_mode):
    # Metadata 0.4.2
    # selectors = {
    #     "strat_zone": "name",
    #     "interval": "interval",
    #     "extraction_type": "attribute",
    #     "content": "seismic",
    #     "difference": "difference",
    # }

    map_type_dict = {}

    intervals_df = metadata[["time1", "time2"]]
    intervals = []

    for key, value in selectors.items():
        selector = key

        if selector == "interval":
            for _index, row in intervals_df.iterrows():
                t1 = str(row["time1"])
                t2 = str(row["time2"])

                if interval_mode == "normal":
                    interval = t2 + "-" + t1
                else:
                    interval = t1 + "-" + t2

                if interval not in intervals:
                    intervals.append(interval)

            items = sorted(intervals)
        else:
            items = list(metadata[selector].unique())

        map_type_dict[selector] = sorted(items)

    return map_type_dict


def get_auto4d_filename(surface_metadata, data, ensemble, real, map_type, coverage):
    selected_interval = data["date"]
    name = data["name"]
    attribute = data["attr"]

    time2 = selected_interval[0:10]
    time1 = selected_interval[11:]

    surface_metadata.replace(np.nan, "", inplace=True)
    metadata = surface_metadata[surface_metadata["coverage"] == coverage]

    headers = [
        "attribute",
        "seismic",
        "difference",
        "time2",
        "time1",
        "map_name",
    ]

    print()
    print("Coverage", coverage)

    try:
        selected_metadata = metadata[
            (metadata["difference"] == real)
            & (metadata["seismic"] == ensemble)
            & (metadata["map_type"] == map_type)
            & (metadata["time1"] == time1)
            & (metadata["time2"] == time2)
            & (metadata["strat_zone"] == name)
            & (metadata["attribute"] == attribute)
        ]

        filepath = selected_metadata["filename"].values[0]
        path = filepath
        map_name = path.split("/")[-1]

    except:
        path = ""
        map_name = ""
        print("WARNING: Selected file not found in Auto4d directory")
        print("  Selection criteria are:")
        print("  -  ", map_type, name, attribute, time1, time2, ensemble, real)

    return path, map_name


def get_auto4d_filename_new(input_metadata, selectors, data, ensemble, real):
    metadata = input_metadata.copy()
    metadata.replace(np.nan, "", inplace=True)

    selected_keys = list(selectors.keys())

    selected_values = list(data.values())
    selected_values = selected_values + [ensemble, real]

    selection_dict = dict(zip(selected_keys, selected_values))

    try:
        selected_metadata = metadata[
            (metadata[selected_keys[0]] == selected_values[0])
            & (metadata[selected_keys[1]] == selected_values[1])
            & (metadata[selected_keys[2]] == selected_values[2])
            & (metadata[selected_keys[3]] == selected_values[3])
            & (metadata[selected_keys[4]] == selected_values[4])
        ]

        filepath = selected_metadata["filename"].values[0]
        path = filepath
        map_name = path.split("/")[-1]

    except:
        path = ""
        map_name = ""

        print("WARNING: Selected file not found in Auto4d directory")
        print("  Selection criteria are:")
        pprint(selection_dict)

    return path, map_name


def get_auto4d_metadata(config):
    shared_settings = config.get("shared_settings")
    metadata_version = shared_settings.get("metadata_version")
    interval_mode = shared_settings.get("interval_mode")

    auto4d_settings = shared_settings.get("auto4d_file")
    directory = auto4d_settings.get("directory")

    metadata_format = auto4d_settings.get("metadata_format")
    acquisition_dates = auto4d_settings.get("acquisition_dates")

    metadata = load_auto4d_metadata_new(
        directory, metadata_format, metadata_version, acquisition_dates
    )
    selection_list = create_auto4d_lists(metadata, interval_mode)

    return metadata, selection_list


def get_auto4d_metadata_selectors(config, selectors):
    shared_settings = config.get("shared_settings")
    metadata_version = shared_settings.get("metadata_version")
    interval_mode = shared_settings.get("interval_mode")

    auto4d_settings = shared_settings.get("auto4d_file")
    directory = auto4d_settings.get("directory")

    metadata_format = auto4d_settings.get("metadata_format")
    acquisition_dates = auto4d_settings.get("acquisition_dates")

    metadata = load_auto4d_metadata_selectors(
        directory,
        metadata_format,
        metadata_version,
        acquisition_dates,
    )
    selection_list = create_selectors_list(metadata, selectors, interval_mode)

    return metadata, selection_list
