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
