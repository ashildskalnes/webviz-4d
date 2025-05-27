import io
import pandas as pd
import time
import numpy as np
from ast import literal_eval
from pprint import pprint
import xtgeo
import warnings

from webviz_4d._datainput.common import read_config, print_metadata
from webviz_4d._providers.osdu_provider._provider_impl_file import DefaultOsduService
from webviz_4d._datainput._maps import print_surface_info
from webviz_4d._datainput.well import get_cache_filename, load_cached_data
from webviz_4d._datainput._auto4d import create_selectors_list

warnings.filterwarnings("ignore")


def get_osdu_dataset_id(surface_metadata, data, ensemble, real, map_type, coverage):
    selected_interval = data["date"]
    name = data["name"]
    attribute = data["attr"]

    if selected_interval[0:10] > selected_interval[11:]:
        time2 = selected_interval[0:10]
        time1 = selected_interval[11:]
    else:
        time1 = selected_interval[0:10]
        time2 = selected_interval[11:]

    surface_metadata.replace(np.nan, "", inplace=True)
    metadata_coverage = surface_metadata[surface_metadata["coverage"] == coverage]

    map_name = None

    try:
        selected_metadata = metadata_coverage[
            (metadata_coverage["difference"] == real)
            & (metadata_coverage["seismic"] == ensemble)
            & (metadata_coverage["map_type"] == map_type)
            & (metadata_coverage["time1"] == time1)
            & (metadata_coverage["time2"] == time2)
            & (metadata_coverage["name"] == name)
            & (metadata_coverage["attribute"] == attribute)
        ]

        if len(selected_metadata) != 1:
            print("WARNING number of datasets =", len(selected_metadata))
            print(selected_metadata)

        dataset_id = selected_metadata["dataset_id"].values[0]
        map_name = selected_metadata["map_name"].values[0]

        return dataset_id, map_name
    except:
        dataset_id = None
        map_name = None

        print("WARNING: Selected map not found in OSDU. Selection criteria are:")
        print(map_type, real, ensemble, name, attribute, time1, time2)

    if dataset_id:
        dataset_id = literal_eval(dataset_id)

    return dataset_id, map_name


def get_correct_list(name, raw_metatadata_items):
    status = False

    metatadata_items = raw_metatadata_items

    if type(metatadata_items) is list and len(metatadata_items) == 1:
        items = metatadata_items[0]

        if "[" in metatadata_items[0]:
            new_names = items.replace("[", "").replace('"', "").replace("]", "")

        new_names = new_names.split(",")

        if type(new_names) is list and len(new_names) == 2:
            metatadata_items = new_names
        else:
            metatadata_items = [items]

    if type(metatadata_items) is str:
        names_split = metatadata_items.split(",")
        metatadata_items = names_split

    if len(metatadata_items) == 1:
        nameA = metatadata_items[0]
        nameB = ""

    else:
        nameA = metatadata_items[0]
        nameB = metatadata_items[1]

    if nameA == "" or nameB == "":
        metatadata_items = raw_metatadata_items

        if type(metatadata_items) is str:
            metatadata_items = literal_eval(metatadata_items)

        if len(metatadata_items) != 2:
            status = False
        else:
            status = True
            nameA = metatadata_items[0]
            nameB = metatadata_items[1]
    else:
        status = True
        nameA = metatadata_items[0]
        nameB = metatadata_items[1]

    if status:
        metatadata_items = [
            nameA.replace(" ", ""),
            nameB.replace(" ", ""),
        ]
    else:
        metatadata_items = []

    return metatadata_items


def get_osdu_metadata_attributes(horizons):
    metadata_dicts = []

    for horizon in horizons:
        if horizon:
            metadata_dicts.append(horizon.__dict__)

    maps_df = pd.DataFrame(metadata_dicts)
    columns = maps_df.columns
    new_columns = [col.replace("_", ".") for col in columns]
    maps_df.columns = new_columns

    maps_updated_df = maps_df.replace("", "---")

    return maps_updated_df


def convert_metadata(osdu_metadata):
    metadata_list = []

    for _index, row in osdu_metadata.iterrows():
        if "0.4.2" in row["MetadataVersion"]:
            name = row["Name"]
            strat_zone = row["StratigraphicZone"]
            field_name = row["FieldName"]
            extraction_type = row["AttributeExtractionType"]
            content = row["SeismicTraceAttribute"]
            coverage = row["SeismicCoverage"]
            difference = row["SeismicDifferenceType"]
            difference_type = row["AttributeDifferenceType"]
            bingrid_name = row["SeismicBinGridName"]
            map_dim = row["MapTypeDimension"]
            dataset_ids = row["DatasetIDs"]

            if type(dataset_ids) == str:
                dataset_ids = literal_eval(dataset_ids)

            if dataset_ids and len(dataset_ids) == 1:
                dataset_id = dataset_ids[0]
            elif dataset_ids and len(dataset_ids) == 2:
                dataset_id = dataset_ids[1]
            else:
                dataset_id = ""
        else:
            print("ERROR Metadata version not supported:", row["MetadataVersion"])

        name = row["Name"]

        if difference == "RawDifference" or difference == "NotTimeshifted":
            difference = "NotTimeshifted"
        elif difference == "TimeshiftedDifference" or difference == "Timeshifted":
            difference = "Timeshifted"
        elif difference == "":
            difference = "seismic_content"
        else:
            difference = "---"

        if "simulated" in name:
            map_type = "simulated"
        else:
            map_type = "observed"

        times = row["AcquisitionDates"]

        if type(times) == str:
            times = literal_eval(times)

        interval = times[1] + "-" + times[0]

        map_dict = {
            "map_name": name,
            "strat_zone": strat_zone,
            "field_name": field_name,
            "extraction_type": extraction_type,
            "dates": times,
            "time1": times[0],
            "time2": times[1],
            "interval": interval,
            "content": content,
            "coverage": coverage,
            "difference": difference,
            "filename": "",
            "bin_grid_name": bingrid_name,
            "map_dim": map_dim,
            "difference_type": difference_type,
            "tagname": "",
            "map_type": map_type,
            "dataset_id": dataset_id,
        }

        metadata_list.append(map_dict)

    all_metadata = pd.DataFrame(metadata_list)
    all_metadata.fillna(value=np.nan, inplace=True)
    all_metadata["data_source"] = "osdu"

    return all_metadata


def create_osdu_lists(metadata, interval_mode):
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
        map_type_metadata = map_type_metadata.where(~map_type_metadata.isna(), "")

        intervals_df = map_type_metadata[["time1", "time2"]]
        intervals = []

        for key, value in selectors.items():
            selector = key
            map_type_metadata = metadata[metadata["map_type"] == map_type]

            map_type_dict[value] = {}

            if selector == "interval":
                for _index, row in intervals_df.iterrows():
                    t1 = row["time1"]
                    t2 = row["time2"]

                    if type(t1) == str and type(t2) is str:
                        if interval_mode == "normal":
                            interval = t2 + "-" + t1
                        else:
                            interval = t1 + "-" + t2
                    else:  # Drogon data hack
                        t1 = "2018-01-01"
                        t2 = "2019-01-01"
                        interval = t2 + "-" + t1

                    if interval not in intervals:
                        intervals.append(interval)

                sorted_intervals = sorted(intervals)

                map_type_dict[value] = sorted_intervals
            else:
                items = list(map_type_metadata[selector].unique())
                cleaned_list = [x for x in items if str(x) != "nan"]

                # if len(cleaned_list) == 1:
                #     items = cleaned_list
                # else:
                #     items = cleaned_list.sort()
                map_type_dict[value] = sorted(cleaned_list)

        map_dict[map_type] = map_type_dict

    return map_dict


def get_osdu_metadata(config, osdu_service, field_name):
    print("Loading metadata for SeismicAttributeInterpretation objects in OSDU ...")

    shared_settings = config.get("shared_settings")
    metadata_version = shared_settings.get("metadata_version")
    interval_mode = shared_settings.get("interval_mode")

    # Check file cache
    api = "OSDU"
    mode = "meta"

    cache_file_name = get_cache_filename(field_name, api, mode)
    dataframe = load_cached_data(cache_file_name)

    if dataframe.empty:
        attribute_horizons = osdu_service.get_attribute_horizons(
            metadata_version=metadata_version, field_name=field_name
        )

        metadata = get_osdu_metadata_attributes(attribute_horizons)
        updated_metadata = osdu_service.update_reference_dates(metadata)

        if field_name:
            field_metadata = updated_metadata[
                updated_metadata["FieldName"] == field_name
            ]
            metadata = field_metadata.copy()
        else:
            metadata = updated_metadata

        metadata["map_type"] = "observed"
        dataframe = convert_metadata(metadata)

        dataframe.to_csv(cache_file_name)
        print(" - Storing metadata to file cache:", cache_file_name)

    selection_list = create_osdu_lists(dataframe, interval_mode)

    return dataframe, selection_list


def get_osdu_metadata_selectors(config, osdu_service, selectors, field_name):
    shared_settings = config.get("shared_settings")
    metadata_version = shared_settings.get("metadata_version")
    interval_mode = shared_settings.get("interval_mode")

    # Check file cache
    api = "OSDU"
    mode = "meta"

    cache_file_name = get_cache_filename(field_name, api, mode)
    dataframe = load_cached_data(cache_file_name)

    if dataframe.empty:
        attribute_horizons = osdu_service.get_attribute_horizons(
            metadata_version=metadata_version, field_name=field_name
        )

        metadata = get_osdu_metadata_attributes(attribute_horizons)
        updated_metadata = osdu_service.update_reference_dates(metadata)

        if field_name:
            field_metadata = updated_metadata[
                updated_metadata["FieldName"] == field_name
            ]
            metadata = field_metadata.copy()
        else:
            metadata = updated_metadata

        metadata["map_type"] = "observed"
        dataframe = convert_metadata(metadata)

        dataframe.to_csv(cache_file_name)
        print(" - Storing metadata to file cache:", cache_file_name)

    selection_list = create_selectors_list(dataframe, selectors, interval_mode)

    return dataframe, selection_list


def load_surface_from_osdu(
    map_idx, data_source, metadata, map_defaults, data, ensemble, real, coverage
):
    # Load surface from one of the data sources based on the selected metadata

    name = data["name"]
    attribute = data["attr"]
    map_type = map_defaults["map_type"]

    selected_interval = data["date"]
    time1 = selected_interval[0:10]
    time2 = selected_interval[11:]

    metadata_keys = [
        "map_index",
        "map_type",
        "surface_name",
        "attribute",
        "time_interval",
        "seismic",
        "difference",
    ]

    surface = None
    map_name = None

    if data_source == "osdu":
        tic = time.perf_counter()
        osdu_service = DefaultOsduService()
        dataset_id, map_name = get_osdu_dataset_id(
            metadata, data, ensemble, real, map_type, coverage
        )

        if dataset_id:
            dataset = osdu_service.get_horizon_map(file_id=dataset_id)
            blob = io.BytesIO(dataset.content)
            surface = xtgeo.surface_from_file(blob)
            toc = time.perf_counter()

            print_surface_info(map_idx, tic, toc, surface, map_name)
    else:
        metadata_values = [
            map_type,
            name,
            attribute,
            [time1, time2],
            ensemble,
            real,
        ]
        print("Selected map not found in", data_source)
        print("  Selection criteria:")

        for index, metadata in enumerate(metadata_keys):
            print("  - ", metadata, ":", metadata_values[index])

    return surface, map_name


def load_surface_from_osdu_new(
    map_idx,
    input_metadata,
    selectors,
    data,
    ensemble,
    real,
):
    metadata = input_metadata.copy()

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

        if len(selected_metadata) != 1:
            print("WARNING number of datasets =", len(selected_metadata))

            selected_metadata = selected_metadata.head(1)

        map_name = selected_metadata["map_name"].values[0]
        dataset_id = selected_metadata["dataset_id"].values[0]

        tic = time.perf_counter()
        osdu_service = DefaultOsduService()

        if dataset_id:
            dataset = osdu_service.get_horizon_map(file_id=dataset_id)
            blob = io.BytesIO(dataset.content)
            surface = xtgeo.surface_from_file(blob)
            toc = time.perf_counter()

    except:
        map_name = ""
        surface = None

        print("WARNING: Selected file not found in OSDU")
        print("  Selection criteria are:")
        pprint(selection_dict)

    if surface is not None:
        print_surface_info(map_idx, tic, toc, surface, map_name)
    else:
        print()
        print("Selected map not found in OSDU")

    return surface, map_name


def main():
    config_file = "/private/ashska/dev_311/my_forks/fields/johan_sverdrup/osdu_config/osdu_config_new.yaml"
    config = read_config(config_file)
    osdu_service = DefaultOsduService()
    field_name = ""

    print("Searching for attribute maps in OSDU Core ...")
    metadata, selection_list = get_osdu_metadata(config, osdu_service, field_name)

    print_metadata(metadata)
    print()
    pprint(selection_list)


if __name__ == "__main__":
    main()
