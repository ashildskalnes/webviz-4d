import os
import io
import numpy as np
import pandas as pd
import time
import argparse
import warnings
from datetime import datetime
import xtgeo
from ast import literal_eval
from pprint import pprint
import prettytable as pt


from webviz_4d._datainput.common import read_config, print_metadata
from webviz_4d._providers.osdu_provider._provider_impl_file import DefaultOsduService
from webviz_4d._datainput._osdu import (
    get_osdu_metadata_attributes,
    convert_metadata,
    create_osdu_lists,
)

warnings.filterwarnings("ignore")

osdu_service = DefaultOsduService()


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

    headers = [
        "attribute",
        "seismic",
        "difference",
        "time2",
        "time1",
        "map_name",
    ]

    print("Coverage", coverage)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.max_rows", None)
    print(metadata_coverage[headers].sort_values(by="attribute"))

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

        print("Selected dataset info:")
        print(map_type, real, ensemble, name, attribute, time1, time2)

        if len(selected_metadata) != 1:
            print("WARNING number of datasets =", len(selected_metadata))
            print(selected_metadata)

        dataset_id = selected_metadata["dataset_id"].values[0]
        map_name = selected_metadata["map_name"].values[0]

        print(map_name, dataset_id)

        return dataset_id, map_name
    except:
        dataset_id = None
        print("WARNING: Selected map not found in OSDU. Selection criteria are:")
        print(map_type, real, ensemble, name, attribute, time1, time2)

    if dataset_id:
        dataset_id = literal_eval(dataset_id)

    return dataset_id, map_name


def main():
    description = "Check OSDU Core metadata"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("config_file")

    args = parser.parse_args()

    config_file = args.config_file
    config_file = os.path.abspath(config_file)
    config = read_config(config_file)
    config_folder = os.path.dirname(config_file)

    shared_settings = config.get("shared_settings")
    field_name = shared_settings.get("field_name")
    settings = shared_settings.get("osdu")
    metadata_version = settings.get("metadata_version")
    interval_mode = shared_settings.get("interval_mode")
    osdu = shared_settings.get("osdu")
    coverage = osdu.get("coverage")

    cache_file = "metadata_cache_" + metadata_version + ".csv"
    metadata_file_cache = os.path.join(config_folder, cache_file)

    # Delete cached data if existing
    try:
        os.remove(metadata_file_cache)
        print(f"File '{metadata_file_cache}' deleted successfully.")

    except FileNotFoundError:
        print(f"File '{metadata_file_cache}' not found.")

    if os.path.isfile(metadata_file_cache):
        print("Reading cached metadata from", metadata_file_cache)
        updated_metadata = pd.read_csv(metadata_file_cache)
    else:
        print("Extracting metadata from OSDU Core ...")
        print()

        attribute_horizons = osdu_service.get_attribute_horizons(
            metadata_version=metadata_version, field_name=field_name
        )

        # print("Number of attribute maps:", len(attribute_horizons))
        print("Checking the extracted metadata ...")

        if len(attribute_horizons) == 0:
            exit()

        metadata = get_osdu_metadata_attributes(attribute_horizons)

        updated_metadata = osdu_service.update_reference_dates(metadata)
        updated_metadata.to_csv(metadata_file_cache)

        print("Updated metadata stored to:", metadata_file_cache)

    field_names = updated_metadata["FieldName"].unique()

    for field_name in field_names:
        print("Field name:", field_name)
        selected_field_metadata = updated_metadata[
            updated_metadata["FieldName"] == field_name
        ]
        selected_field_metadata["map_type"] = "observed"

        converted_metadata = convert_metadata(selected_field_metadata)
        print_metadata(converted_metadata)
        print()

    selection_list = create_osdu_lists(converted_metadata, interval_mode)
    print()
    pprint(selection_list)
    print()

    # Extract a selected map
    field_name = "JOHAN SVERDRUP"
    data_source = "OSDU"
    attribute = "MaxPositive"
    name = "3D+TAasgard+JS+Z22+Merge_EQ20231_PH2DG3"
    map_type = "observed"
    seismic = "AMPLITUDE"
    difference = "NotTimeshifted"
    interval = "2021-05-17-2020-09-30"

    ensemble = seismic
    real = difference

    data = {"attr": attribute, "name": name, "date": interval}

    dataset_id, map_name = get_osdu_dataset_id(
        converted_metadata, data, ensemble, real, map_type, coverage
    )

    if dataset_id is not None:
        print()
        print("Loading surface from", data_source, map_name)
        start_time = time.time()
        dataset = osdu_service.get_horizon_map(file_id=dataset_id)
        blob = io.BytesIO(dataset.content)
        surface = xtgeo.surface_from_file(blob)
        print(" --- %s seconds ---" % (time.time() - start_time))

        print(surface)
        # surface.quickplot(title=original_name, colormap="rainbow_r")


if __name__ == "__main__":
    main()
