import os
import glob
import time
import numpy as np
import pandas as pd
import yaml


def read_yaml_file(yaml_file):
    """Return the content of a yaml file as a dict"""

    content = {}

    with open(yaml_file, "r") as stream:
        content = yaml.safe_load(stream)

    return content


def load_fmu_metadata(fmu_dir, map_directory, field_name):
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
    map_names = []

    headers = [
        "name",
        "attribute",
        "time1",
        "time2",
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
        metadata = read_yaml_file(metadata_file)
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
        filename = file.get("absolute_path")

        surface_names.append(seismic_horizon)
        attributes.append(attribute_type)
        times1.append(time1)
        times2.append(time2)
        seismic_contents.append(seismic_content)
        coverages.append(coverage)
        differences.append(difference)
        filenames.append(filename)
        field_names.append(field_name)
        map_names.append(name)

    print("Metadata loaded:")
    print(" --- %s seconds ---" % (time.time() - start_time))
    print(" --- ", len(surface_names), "files")

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
            map_names,
        )
    )

    all_metadata = pd.DataFrame(zipped_list, columns=headers)
    all_metadata.fillna(value=np.nan, inplace=True)
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
    except:
        path = ""
        print("WARNING: selected map not found. Selection criteria are:")
        print(map_type, real, ensemble, name, attribute, time1, time2)
        # print(metadata)

    return path


def get_fmu_metadata(config, field_name):
    shared_settings = config.get("shared_settings")
    interval_mode = shared_settings.get("interval_mode")

    fmu_settings = shared_settings.get("fmu")
    directory = fmu_settings.get("directory")

    print()
    print("Searching for seismic 4D attribute maps on disk:", directory, " ...")

    directory = fmu_settings.get("directory")
    observed_maps = fmu_settings.get("observed_maps")
    map_dir = observed_maps.get("map_directories")[0]

    metadata = load_fmu_metadata(directory, map_dir, field_name)
    selection_list = create_fmu_lists(metadata, interval_mode)

    return metadata, selection_list
