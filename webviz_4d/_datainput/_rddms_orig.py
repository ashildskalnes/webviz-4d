import numpy as np
import time
import pprint

from webviz_4d._providers.rddms_provider._provider_impl_file import DefaultRddmsService
from webviz_4d._datainput._osdu import (
    get_osdu_metadata_attributes,
    convert_metadata,
)
from webviz_4d._datainput._maps import print_surface_info


def get_rddms_dataset_id(surface_metadata, data, ensemble, real, map_type):
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

    items = [
        "difference",
        "seismic",
        "map_type",
        "time1",
        "time2",
        "name",
        "attribute",
        "map_type",
    ]
    print()
    print("Webviz-4D metadata")
    print(surface_metadata[items])

    try:
        selected_metadata = surface_metadata[
            (surface_metadata["difference"] == real)
            & (surface_metadata["seismic"] == ensemble)
            & (surface_metadata["map_type"] == map_type)
            & (surface_metadata["time1"] == time1)
            & (surface_metadata["time2"] == time2)
            & (surface_metadata["name"] == name)
            & (surface_metadata["attribute"] == attribute)
        ]

        print()
        print("Selected dataset info:")
        print(map_type, real, ensemble, name, attribute, time1, time2)

        if len(selected_metadata) > 1:
            print("WARNING number of datasets", len(selected_metadata))
            print(selected_metadata)

        dataset_id = selected_metadata["dataset_id"].values[0]
        return dataset_id
    except:
        dataset_id = None
        print("WARNING: Selected map not found in RDDMS. Selection criteria are:")
        print(map_type, real, ensemble, name, attribute, time1, time2)

    return dataset_id


def create_rddms_lists(metadata, interval_mode):
    selectors = {
        "name": "name",
        "interval": "interval",
        "attribute": "attribute",
        "seismic": "seismic",
        "difference": "difference",
    }

    map_types = ["observed", "simulated"]
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
                # items.sort()

                map_type_dict[value] = sorted(items)

        map_dict[map_type] = map_type_dict

    return map_dict


def get_rddms_metadata(config, osdu_service, rddms_service, dataspace, field_name):
    shared_settings = config.get("shared_settings")
    interval_mode = shared_settings.get("interval_mode")

    attribute_horizons = rddms_service.get_attribute_horizons(
        dataspace_name=dataspace, field_name=field_name
    )

    print("Checking the extracted metadata ...")

    metadata = get_osdu_metadata_attributes(attribute_horizons)
    updated_metadata = osdu_service.update_reference_dates(metadata)
    selected_field_metadata = updated_metadata[
        updated_metadata["FieldName"] == field_name
    ]
    selected_field_metadata["map_type"] = "observed"
    converted_metadata = convert_metadata(selected_field_metadata)
    selection_list = create_rddms_lists(converted_metadata, interval_mode)

    return converted_metadata, selection_list


def load_surface_from_rddms(
    map_idx,
    data_source,
    dataspace,
    metadata,
    map_defaults,
    data,
    ensemble,
    real,
    coverage,
):
    # Load surface from rddms

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

    if data_source == "rddms":
        rddms_service = DefaultRddmsService
        uuid_url = get_rddms_dataset_id(metadata, data, ensemble, real, map_type)

        if uuid_url is None:
            print("ERROR selected dataset not found")
            print("ensemble:", ensemble)
            print("real:", real)
            print("map_type:", map_type)
            pprint(data)
            return surface

        selected_metadata = metadata[metadata["dataset_id"] == uuid_url]
        uuid = selected_metadata["id"].values[0]
        horizon_name = selected_metadata["map_name"].values[0]

        if uuid:
            tic = time.perf_counter()
            surface = rddms_service.get_rddms_map(
                dataspace_name=dataspace,
                horizon_name=horizon_name,
                uuid=uuid,
                uuid_url=uuid_url,
            )
            toc = time.perf_counter()

    if surface is not None:
        print_surface_info(map_idx, tic, toc, surface)
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

    return surface
