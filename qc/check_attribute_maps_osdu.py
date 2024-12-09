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


def get_osdu_dataset_id(surface_metadata, data, ensemble, real, map_type):
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
    ]

    map_name = None
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

    if type(dataset_id) == str:
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

    # print(updated_metadata)

    # data_viewer_columns = {
    #     "FieldName": "FieldName",
    #     "Name": "Name",
    #     "Zone": "StratigraphicZone",
    #     "MapDim": "MapTypeDimension",
    #     "SeismicAttribute": "SeismicTraceAttribute",
    #     "AttributeType": "AttributeExtractionType",
    #     "Coverage": "SeismicCoverage",
    #     "DifferenceType": "SeismicDifferenceType",
    #     "AttributeDiff": "AttributeDifferenceType",
    #     "Dates": "AcquisitionDates",
    #     "Version": "MetadataVersion",
    # }

    # standard_metadata = pd.DataFrame()
    # for key, value in data_viewer_columns.items():
    #     standard_metadata[key] = updated_metadata[value]

    # pd.set_option("display.max_rows", None)
    # print(standard_metadata)

    # output_file = os.path.join(
    #     config_folder, "standard_metadata_" + os.path.basename(config_file)
    # )
    # output_file = output_file.replace("yaml", "csv")
    # standard_metadata.to_csv(output_file)
    # print("Standard metadata written to", output_file)

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
    attribute = "Value"
    name = "3D+IUTU+JS+Z22+Merge_EQ20231_PH2DG3"
    map_type = "observed"
    seismic = "Timeshift"
    difference = "---"
    interval = "2022-08-28-2022-05-15"

    ensemble = seismic
    real = difference

    data = {"attr": attribute, "name": name, "date": interval}

    dataset_id, original_name = get_osdu_dataset_id(
        converted_metadata, data, ensemble, real, map_type
    )

    if dataset_id is not None:
        print()
        print("Loading surface from", data_source)
        start_time = time.time()
        dataset = osdu_service.get_horizon_map(file_id=dataset_id)
        blob = io.BytesIO(dataset.content)
        surface = xtgeo.surface_from_file(blob)
        print(" --- %s seconds ---" % (time.time() - start_time))

        print(surface)
        # surface.quickplot(title=original_name, colormap="rainbow_r")


if __name__ == "__main__":
    main()
