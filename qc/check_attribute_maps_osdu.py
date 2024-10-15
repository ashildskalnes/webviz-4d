import os
import io
import numpy as np
import pandas as pd
import time
import argparse
import warnings
from datetime import datetime
import xtgeo
from pprint import pprint


from webviz_4d._datainput.common import read_config
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
        "time.t1",
        "time.t2",
        "name",
        "attribute",
    ]
    print(surface_metadata[items])

    try:
        selected_metadata = surface_metadata[
            (surface_metadata["difference"] == real)
            & (surface_metadata["seismic"] == ensemble)
            & (surface_metadata["map_type"] == map_type)
            & (surface_metadata["time.t1"] == time1)
            & (surface_metadata["time.t2"] == time2)
            & (surface_metadata["name"] == name)
            & (surface_metadata["attribute"] == attribute)
        ]

        print("Selected dataset info:")
        print(map_type, real, ensemble, name, attribute, time1, time2)

        if len(selected_metadata) > 1:
            print("WARNING number of datasets", len(selected_metadata))
            print(selected_metadata)

        dataset_id = selected_metadata["dataset_id"].values[0]
        return dataset_id
    except:
        dataset_id = None
        print("WARNING: Selected map not found in OSDU. Selection criteria are:")
        print(map_type, real, ensemble, name, attribute, time1, time2)

    return dataset_id


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
    settings = shared_settings.get("osdu")
    metadata_version = settings.get("metadata_version")
    interval_mode = shared_settings.get("interval_mode")

    cache_file = "metadata_cache.csv"
    metadata_file_cache = os.path.join(config_folder, cache_file)

    if os.path.isfile(metadata_file_cache):
        print("  Reading cached metadata from", metadata_file_cache)
        metadata = pd.read_csv(metadata_file_cache)
    else:
        print("Extracting metadata from OSDU Core ...")
        start_time = time.time()
        attribute_horizons = osdu_service.get_attribute_horizons(
            metadata_version=metadata_version
        )
        print("Number of valid attribute maps:", len(attribute_horizons))

        metadata = get_osdu_metadata_attributes(attribute_horizons)
        # print(" --- %s seconds ---" % (time.time() - start_time))
        # print()

        updated_metadata = osdu_service.update_reference_dates(metadata)
        updated_metadata.to_csv(metadata_file_cache)
        print("Updated metadata stored to:", metadata_file_cache)

    updated_metadata = osdu_service.update_reference_dates(metadata)

    print(updated_metadata)

    data_viewer_columns = {
        "FieldName": "FieldName",
        "BinGridName": "SeismicBinGridName",
        "Name": "Name",
        "Zone": "StratigraphicZone",
        "MapTypeDim": "MapTypeDimension",
        "SeismicAttribute": "SeismicTraceAttribute",
        "AttributeType": "AttributeExtractionType",
        "Coverage": "SeismicCoverage",
        "DifferenceType": "SeismicDifferenceType",
        "AttributeDiff": "AttributeDifferenceType",
        "Dates": "AcquisitionDates",
        "Version": "MetadataVersion",
    }

    standard_metadata = pd.DataFrame()
    for key, value in data_viewer_columns.items():
        standard_metadata[key] = updated_metadata[value]

    pd.set_option("display.max_rows", None)
    print(standard_metadata)

    output_file = os.path.join(
        config_folder, "standard_metadata_" + os.path.basename(config_file)
    )
    output_file = output_file.replace("yaml", "csv")
    standard_metadata.to_csv(output_file)
    print("Standard metadata written to", output_file)

    converted_metadata = convert_metadata(updated_metadata)
    print()
    print(converted_metadata)

    output_file = os.path.join(
        config_folder, "metadata_" + os.path.basename(config_file)
    )
    output_file = output_file.replace("yaml", "csv")
    updated_metadata.to_csv(output_file)
    print("All metadata writen to", output_file)

    selection_list = create_osdu_lists(converted_metadata, interval_mode)

    print()
    pprint(selection_list)

    # Extract a selected map
    data_source = "OSDU"
    attribute = "MaxPositive"
    name = "FullReservoirEnvelope"
    map_type = "observed"
    seismic = "Amplitude"
    difference = "RawDifference"
    interval = "2021-05-17-2020-09-30"

    ensemble = seismic
    real = difference

    data = {"attr": attribute, "name": name, "date": interval}

    dataset_id = get_osdu_dataset_id(converted_metadata, data, ensemble, real, map_type)

    if dataset_id is not None:
        print("Loading surface from", data_source)
        start_time = time.time()
        dataset = osdu_service.get_horizon_map(file_id=dataset_id)
        blob = io.BytesIO(dataset.content)
        surface = xtgeo.surface_from_file(blob)
        print(" --- %s seconds ---" % (time.time() - start_time))

        print(surface)


if __name__ == "__main__":
    main()
