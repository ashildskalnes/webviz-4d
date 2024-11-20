import os
import numpy as np
import pandas as pd
import argparse
import time
from pprint import pprint
import xtgeoviz.plot as xtgplot
import xtgeo


from webviz_4d._datainput.common import read_config
from webviz_4d._providers.rddms_provider._provider_impl_file import DefaultRddmsService
from webviz_4d._providers.osdu_provider._provider_impl_file import DefaultOsduService
from webviz_4d._datainput._osdu import (
    get_osdu_metadata_attributes,
    convert_metadata,
)

from webviz_4d._datainput._rddms import get_rddms_dataset_id, create_rddms_lists

rddms_service = DefaultRddmsService()
osdu_service = DefaultOsduService()


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
    field_name = shared_settings.get("field_name")

    print("Searching for Dataspaces in RDDMS:")
    dataspaces = rddms_service.get_dataspaces()

    for dataspace in dataspaces:
        print("Dataspace:", dataspace)

    print("-----------------------------------------------------------------")
    print(
        "Searching for seismic 4D attribute maps in RDDMS",
        selected_dataspace,
        metadata_version,
        field_name,
        " ...",
    )

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
                attribute_horizon.DatasetIDs,
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

    selection_list = create_rddms_lists(converted_metadata, interval_mode)

    print()
    print("Webviz-4D selection list")
    pprint(selection_list)

    # Extract a selected map
    data_source = "RDDMS"
    attribute = "Average"
    name = "Total"
    map_type = "simulated"
    seismic = "SWAT"
    difference = "NotTimeshifted"
    interval = "2021-05-17-2020-09-30"

    ensemble = seismic
    real = difference

    data = {"attr": attribute, "name": name, "date": interval}

    uuid_url = get_rddms_dataset_id(converted_metadata, data, ensemble, real, map_type)
    selected_metadata = converted_metadata[converted_metadata["dataset_id"] == uuid_url]
    uuid = selected_metadata["id"].values[0]
    horizon_name = selected_metadata["original_name"].values[0]

    if uuid is not None:
        print()
        print("Loading surface from", data_source)
        print(" - uuid", uuid)
        print(" - uuid_url", uuid_url)

        start_time = time.time()

        surface = rddms_service.get_rddms_map(
            dataspace_name=selected_dataspace,
            horizon_name=horizon_name,
            uuid=uuid,
            uuid_url=uuid_url,
        )

        print(" --- %s seconds ---" % (time.time() - start_time))
        number_cells = surface.nrow * surface.ncol
        print(f"Surface size: {surface.nrow} x {surface.ncol} = {number_cells}")

        print()
        print(surface)
        surface.quickplot()

        # rotation = "calculated"

        # start_time = time.time()
        # surface = rddms_service.get_rddms_map(
        #     dataspace_name=selected_dataspace,
        #     horizon_name=horizon_name,
        #     uuid=uuid,
        #     uuid_url=uuid_url,
        #     mode=rotation,
        # )

        # print(" --- %s seconds ---" % (time.time() - start_time))
        # number_cells = surface.nrow * surface.ncol
        # print(f"Surface size: {surface.nrow} x {surface.ncol} = {number_cells}")

        # print()
        # print(surface)
        # surface.quickplot(title=rotation)


if __name__ == "__main__":
    main()
