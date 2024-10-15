import os
import numpy as np
import pandas as pd
import argparse
import time
from pprint import pprint

from webviz_4d._datainput.common import read_config
from webviz_4d._providers.rddms_provider._provider_impl_file import DefaultRddmsService
from webviz_4d._providers.osdu_provider._provider_impl_file import DefaultOsduService
from webviz_4d._datainput._osdu import (
    get_osdu_metadata_attributes,
    convert_metadata,
    create_osdu_lists,
)

rddms_service = DefaultRddmsService()
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
    print()
    print("Webviz-4D metadata")
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
        print("WARNING: Selected map not found in OSDU. Selection criteria are:")
        print(map_type, real, ensemble, name, attribute, time1, time2)

    return dataset_id


def main():
    description = "Check RDDMS metadata"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("config_file")

    args = parser.parse_args()

    config_file = args.config_file
    config_file = os.path.abspath(config_file)
    config = read_config(config_file)
    config_folder = os.path.dirname(config_file)

    shared_settings = config.get("shared_settings")
    settings = shared_settings.get("rddms")
    selected_dataspace = settings.get("dataspace")
    metadata_version = settings.get("metadata_version")
    interval_mode = shared_settings.get("interval_mode")
    field_name = "JOHAN SVERDRUP"
    selections = None

    print("Searching for Dataspaces in RDDMS:")
    dataspaces = rddms_service.get_dataspaces()

    for dataspace in dataspaces:
        print("Dataspace:", dataspace)

    #     print(
    #         "Searching for seismic 4D attribute maps in dataspace",
    #         dataspace,
    #         metadata_version,
    #         " ...",
    #     )

    #     attribute_horizons = rddms_service.get_attribute_horizons(dataspace)
    #     print(" - Number of Grid2Representations:", len(attribute_horizons))

    #     for attribute_horizon in attribute_horizons:
    #         if attribute_horizon:
    #             print(
    #                 "   -",
    #                 attribute_horizon.FieldName,
    #                 attribute_horizon.Name,
    #                 attribute_horizon.id,
    #             )
    #     print()

    print("-----------------------------------------------------------------")

    selected_dataspace = "maap/demo"
    print(
        "Searching for seismic 4D attribute maps in RDDMS",
        selected_dataspace,
        metadata_version,
        field_name,
        " ...",
    )

    print("  Reading metadata from RDDMS ...")

    attribute_horizons = rddms_service.get_attribute_horizons(
        selected_dataspace, field_name
    )

    for attribute_horizon in attribute_horizons:
        if attribute_horizon:
            print(
                " -",
                attribute_horizon.FieldName,
                attribute_horizon.Name,
                attribute_horizon.id,
            )

    metadata = get_osdu_metadata_attributes(attribute_horizons)
    updated_metadata = osdu_service.update_reference_dates(metadata)
    # updated_metadata.to_csv(metadata_file_cache)

    data_viewer_columns = {
        "FieldName": "FieldName",
        "BinGridName": "SeismicBinGridName",
        "Name": "Name",
        "Zone": "StratigraphicZone",
        "MapTypeDim": "MapTypeDimension",
        "SeismicAttribute": "SeismicTraceAttribute",
        "AttributeType": "AttributeExtractionType",
        "Coverage": "SeismicCoverage",
        "DifferenceType": "AttributeDifferenceType",
        "AttributeDiff": "AttributeDifferenceType",
        "Dates": "AcquisitionDates",
        "Version": "MetadataVersion",
    }

    standard_metadata = pd.DataFrame()
    for key, value in data_viewer_columns.items():
        standard_metadata[key] = updated_metadata[value]

    pd.set_option("display.max_rows", None)
    print()
    print("Standard metadata:")
    print(standard_metadata)

    output_file = os.path.join(
        config_folder, "standard_metadata_" + os.path.basename(config_file)
    )
    output_file = output_file.replace("yaml", "csv")
    standard_metadata.to_csv(output_file)
    print()
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
    print("Webviz-4D selection list")
    pprint(selection_list)

    # Extract a selected map
    data_source = "RDDMS"
    attribute = "Average"
    name = "Total"
    map_type = "observed"
    seismic = "SWAT"
    difference = "NotTimeshifted"
    interval = "2021-05-17-2020-09-30"

    ensemble = seismic
    real = difference

    data = {"attr": attribute, "name": name, "date": interval}

    uuid = get_osdu_dataset_id(converted_metadata, data, ensemble, real, map_type)

    if uuid is not None:
        print()
        print("Loading surface from", data_source)
        print(" - uuid", uuid)

        start_time = time.time()
        surface = rddms_service.get_rddms_map(
            dataspace_name=selected_dataspace, uuid=uuid
        )
        print(" --- %s seconds ---" % (time.time() - start_time))
        number_cells = surface.nrow * surface.ncol
        print(f"Surface size: {surface.nrow} x {surface.ncol} = {number_cells}")

        print()
        print(surface)
        # surface.quickplot(seismic)


if __name__ == "__main__":
    main()
