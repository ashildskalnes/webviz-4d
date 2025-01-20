import os
import time
import glob
import json
import numpy as np
import pandas as pd


def load_auto4d_metadata(
    auto4d_dir, file_ext, mdata_version, selections, acquisition_dates
):
    all_metadata = pd.DataFrame()
    names = []
    surface_names = []
    attributes = []
    dates = []
    times1 = []
    times2 = []
    seismic_contents = []
    coverages = []
    differences = []
    filenames = []
    field_names = []
    bin_grid_names = []
    strat_zones = []
    map_dims = []
    attribute_diff_types = []

    headers = [
        "map_name",
        "name",
        "attribute",
        "dates",
        "time1",
        "time2",
        "seismic",
        "coverage",
        "difference",
        "filename",
        "field_name",
        "bin_grid_name",
        "strat_zone",
        "diff_type",
    ]

    # Search for all metadata files
    start_time = time.time()
    metadata_files = glob.glob(auto4d_dir + "/*" + file_ext)

    if file_ext == ".a4dmeta":
        for metadata_file in metadata_files:
            selection_status_list = []

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
                    map_name = metadata.get("Name")
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
                    filename = os.path.join(auto4d_dir, map_name + ".gri")
                    bin_grid_name = metadata.get("SeismicBinGridName")
                    strat_zone = metadata.get("StratigraphicZone")
                    map_dim = metadata.get("MapTypeDimension")
                    diff_type = metadata.get("AttributeDifferenceType")

                    names.append(map_name)
                    surface_names.append(strat_zone)
                    attributes.append(attribute_type)
                    dates.append([time1, time2])
                    times1.append(time1)
                    times2.append(time2)
                    seismic_contents.append(seismic_content)
                    coverages.append(coverage)
                    differences.append(difference)
                    filenames.append(filename)
                    field_names.append(field_name)
                    bin_grid_names.append(bin_grid_name)
                    strat_zones.append(strat_zone)
                    map_dims.append(map_dim)
                    attribute_diff_types.append(diff_type)

    else:
        print("ERROR: Unsupported file extension", file_ext)
        return all_metadata

    print("Metadata loaded:")
    print(" --- %s seconds ---" % (time.time() - start_time))
    print(" --- ", len(names), "files")

    zipped_list = list(
        zip(
            names,
            surface_names,
            attributes,
            dates,
            times1,
            times2,
            seismic_contents,
            coverages,
            differences,
            filenames,
            field_names,
            bin_grid_names,
            strat_zones,
            attribute_diff_types,
        )
    )

    all_metadata = pd.DataFrame(zipped_list, columns=headers)
    all_metadata.fillna(value=np.nan, inplace=True)
    all_metadata["map_type"] = "observed"
    all_metadata["metadata_version"] = mdata_version
    all_metadata["source_id"] = ""
    all_metadata["map_dim"] = "4D"

    return all_metadata


def load_auto4d_metadata_new(auto4d_dir, file_ext, mdata_version, acquisition_dates):

    all_metadata = pd.DataFrame()

    metadata_headers = {
        "map_name": "Name",
        "name": "StratigraphicZone",
        "attribute": "AttributeExtractionType",
        "dates": "dates",
        "time1": "time1",
        "time2": "time2",
        "seismic": "SeismicTraceAttribute",
        "coverage": "SeismicCoverage",
        "difference": "SeismicDifferenceType",
        "filename": "filename",
        "field_name": "FieldName",
        "bin_grid_name": "SeismicBinGridName",
        "strat_zone": "StratigraphicColumn",
        "diff_type": "AttributeDifferenceType",
    }

    # Search for all metadata files
    start_time = time.time()
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
                    time1 = str(acquisition_dates.get(seismic_traces[1]))
                    time2 = str(acquisition_dates.get(seismic_traces[0]))

                    filename = os.path.join(auto4d_dir, map_name + ".gri")

                    map_dict = {
                        "map_name": map_name,
                        "name": metadata.get("StratigraphicZone"),
                        "field_name": metadata.get("FieldName"),
                        "attribute": metadata.get("AttributeExtractionType"),
                        "dates": [time1, time2],
                        "time1": time1,
                        "time2": time2,
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

    print("Metadata loaded:")
    print(" --- %s seconds ---" % (time.time() - start_time))
    print(" --- ", len(metadata_list), "files")

    all_metadata = pd.DataFrame(metadata_list)

    all_metadata.fillna(value=np.nan, inplace=True)
    all_metadata["map_type"] = "observed"
    all_metadata["metadata_version"] = mdata_version
    all_metadata["source_id"] = ""
    all_metadata["map_dim"] = "4D"

    return all_metadata


def create_auto4d_lists(metadata, interval_mode):
    # Metadata 0.4.2
    selectors = {
        "strat_zone": "name",
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
                sorted_intervals = sorted(intervals)

                map_type_dict[value] = sorted_intervals
            else:
                items = list(map_type_metadata[selector].unique())
                map_type_dict[value] = sorted(items)

        map_dict[map_type] = map_type_dict

    return map_dict


def get_auto4d_filename(surface_metadata, data, ensemble, real, map_type, coverage):
    selected_interval = data["date"]
    name = data["name"]
    attribute = data["attr"]

    time2 = selected_interval[0:10]
    time1 = selected_interval[11:]

    surface_metadata.replace(np.nan, "", inplace=True)
    metadata_coverage = surface_metadata[surface_metadata["coverage"] == coverage]

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
        selected_metadata = metadata_coverage[
            (metadata_coverage["difference"] == real)
            & (metadata_coverage["seismic"] == ensemble)
            & (metadata_coverage["map_type"] == map_type)
            & (metadata_coverage["time1"] == time1)
            & (metadata_coverage["time2"] == time2)
            & (metadata_coverage["strat_zone"] == name)
            & (metadata_coverage["attribute"] == attribute)
        ]

        filepath = selected_metadata["filename"].values[0]
        path = filepath
        map_name = path.split("/")[-1]

    except:
        path = ""
        print("WARNING: Selected file not found in Auto4d directory")
        print("  Selection criteria are:")
        print("  -  ", map_type, name, attribute, time1, time2, ensemble, real)

    return path, map_name


def get_auto4d_metadata(config):
    shared_settings = config.get("shared_settings")
    metadata_version = shared_settings.get("metadata_version")
    interval_mode = shared_settings.get("interval_mode")

    auto4d_settings = shared_settings.get("auto4d_file")
    directory = auto4d_settings.get("directory")

    metadata_format = auto4d_settings.get("metadata_format")
    acquisition_dates = auto4d_settings.get("acquisition_dates")

    print()
    print("Searching for seismic 4D attribute maps on disk:", directory, " ...")

    metadata = load_auto4d_metadata_new(
        directory, metadata_format, metadata_version, acquisition_dates
    )
    selection_list = create_auto4d_lists(metadata, interval_mode)

    return metadata, selection_list
