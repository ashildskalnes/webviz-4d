import os
import glob
import time
import numpy as np
import pandas as pd
from pprint import pprint

from webviz_4d._datainput.common import read_config
from webviz_4d._datainput._auto4d import create_selectors_list


def load_fmu_metadata(fmu_dir, map_directory, field_name):
    all_metadata = pd.DataFrame()
    surface_names = []
    attributes = []
    times1 = []
    times2 = []
    intervals = []
    seismic_contents = []
    coverages = []
    differences = []
    filenames = []
    field_names = []
    map_names = []

    headers = [
        "name",
        "attribute",
        "time1",
        "time2",
        "interval",
        "seismic",
        "coverage",
        "difference",
        "filename",
        "field_name",
        "map_name",
    ]

    file_ext = ".yml"

    # Search for all metadata files
    start_time = time.time()
    metadata_files = glob.glob(fmu_dir + "/" + map_directory + ".*" + file_ext)

    for metadata_file in metadata_files:
        metadata = read_config(metadata_file)
        file = metadata.get("file")
        data = metadata.get("data")

        name = os.path.basename(file.get("absolute_path")).replace("gri", "")
        tag_name = data.get("tagname")
        tag_name_info = tag_name.split("_")
        attribute_type = tag_name_info[-1]
        seismic_content = tag_name_info[0]
        coverage = "Unknown"

        if seismic_content == "timestrain" or seismic_content == "timeshift":
            difference = "---"
        else:
            difference = "NotTimeshifted"

        horizon_items = name.split("--")
        seismic_horizon = horizon_items[0]
        times = data.get("time")
        time1 = str(times.get("t0").get("value"))[0:10]
        time2 = str(times.get("t1").get("value"))[0:10]
        interval = time2 + "-" + time1
        filename = file.get("absolute_path")

        surface_names.append(seismic_horizon)
        attributes.append(attribute_type)
        times1.append(time1)
        times2.append(time2)
        intervals.append(interval)
        seismic_contents.append(seismic_content)
        coverages.append(coverage)
        differences.append(difference)
        filenames.append(filename)
        field_names.append(field_name)
        map_names.append(name)

    zipped_list = list(
        zip(
            surface_names,
            attributes,
            times1,
            times2,
            intervals,
            seismic_contents,
            coverages,
            differences,
            filenames,
            field_names,
            map_names,
        )
    )

    all_metadata = pd.DataFrame(zipped_list, columns=headers)
    all_metadata.fillna(value=np.nan, inplace=True)
    all_metadata["map_type"] = "observed"

    return all_metadata


def load_fmu_metadata_selectors(fmu_dir, map_directory, field_name):
    all_metadata = pd.DataFrame()
    file_ext = ".yml"

    # Search for all metadata files
    metadata_list = []
    metadata_files = glob.glob(fmu_dir + "/" + map_directory + ".*" + file_ext)

    for metadata_file in metadata_files:
        metadata = read_config(metadata_file)
        file = metadata.get("file")
        data = metadata.get("data")

        name = os.path.basename(file.get("absolute_path")).replace("gri", "")
        tag_name = data.get("tagname")
        tag_name_info = tag_name.split("_")
        attribute_type = tag_name_info[-1]
        seismic_content = tag_name_info[0]
        coverage = "Unknown"
        bin_grid_name = "Unknown"
        map_dim = "4D"
        difference_type = "AttributeOfDifference"

        if seismic_content == "timestrain" or seismic_content == "timeshift":
            difference = "---"
        else:
            difference = "NotTimeshifted"

        horizon_items = name.split("--")
        seismic_horizon = horizon_items[0]
        times = data.get("time")
        time1 = str(times.get("t0").get("value"))[0:10]
        time2 = str(times.get("t1").get("value"))[0:10]
        interval = time2 + "-" + time1
        filename = file.get("absolute_path")

        map_dict = {
            "map_name": name,
            "strat_zone": seismic_horizon,
            "field_name": field_name,
            "extraction_type": attribute_type,
            "dates": [time1, time2],
            "time1": time1,
            "time2": time2,
            "interval": interval,
            "content": seismic_content,
            "coverage": coverage,
            "difference": difference,
            "filename": filename,
            "bin_grid_name": bin_grid_name,
            "map_dim": map_dim,
            "difference_type": difference_type,
        }

        metadata_list.append(map_dict)

    all_metadata = pd.DataFrame(metadata_list)
    all_metadata.fillna(value=np.nan, inplace=True)
    all_metadata["data_source"] = "fmu"
    all_metadata["map_type"] = "observed"

    return all_metadata


def create_fmu_lists(metadata, interval_mode):
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
                sorted_intervals = intervals

                map_type_dict[value] = sorted_intervals
            else:
                items = list(map_type_metadata[selector].unique())
                items.sort()

                map_type_dict[value] = items

        map_dict[map_type] = map_type_dict

    return map_dict


def get_fmu_filename(data, ensemble, real, map_type, metadata):
    selected_interval = data["date"]
    name = data["name"]
    attribute = data["attr"]

    time2 = selected_interval[0:10]
    time1 = selected_interval[11:]

    metadata.replace(np.nan, "", inplace=True)

    try:
        selected_metadata = metadata[
            (metadata["difference"] == real)
            & (metadata["seismic"] == ensemble)
            & (metadata["map_type"] == map_type)
            & (metadata["time1"] == time1)
            & (metadata["time2"] == time2)
            & (metadata["name"] == name)
            & (metadata["attribute"] == attribute)
        ]

        path = selected_metadata["filename"].values[0]
        map_name = path.split("/")[-1]
    except:
        path = ""
        map_name = None
        print("WARNING: selected map not found. Selection criteria are:")
        print(map_type, real, ensemble, name, attribute, time1, time2)
        # print(metadata)

    return path, map_name


def get_fmu_filename_new(input_metadata, selectors, data, ensemble, real):
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
        map_name = None
        print("WARNING: Selected file not found in Auto4d directory")
        print("  Selection criteria are:")
        pprint(selection_dict)

    return path, map_name


def get_fmu_metadata(config, field_name):
    shared_settings = config.get("shared_settings")
    interval_mode = shared_settings.get("interval_mode")

    fmu_settings = shared_settings.get("fmu")
    directory = fmu_settings.get("directory")

    directory = fmu_settings.get("directory")
    observed_maps = fmu_settings.get("observed_maps")
    map_dir = observed_maps.get("map_directories")[0]

    metadata = load_fmu_metadata(directory, map_dir, field_name)
    selection_list = create_fmu_lists(metadata, interval_mode)

    return metadata, selection_list


def get_fmu_metadata_selectors(config, selectors):
    shared_settings = config.get("shared_settings")
    interval_mode = shared_settings.get("interval_mode")

    field_name = shared_settings.get("field_name")
    fmu_settings = shared_settings.get("fmu")
    directory = fmu_settings.get("directory")

    directory = fmu_settings.get("directory")
    observed_maps = fmu_settings.get("observed_maps")
    map_dir = observed_maps.get("map_directories")[0]

    metadata = load_fmu_metadata_selectors(directory, map_dir, field_name)
    selection_list = create_selectors_list(metadata, selectors, interval_mode)

    return metadata, selection_list
