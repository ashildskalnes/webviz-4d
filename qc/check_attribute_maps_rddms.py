import os
import pandas as pd
import argparse
import time
from pprint import pprint

from webviz_4d._datainput.common import read_config, print_metadata
from webviz_4d._providers.rddms_provider._provider_impl_file import DefaultRddmsService
from webviz_4d._providers.osdu_provider._provider_impl_file import DefaultOsduService
from webviz_4d._datainput._osdu import (
    get_osdu_metadata_attributes,
    convert_metadata,
)

from webviz_4d._datainput._rddms import get_rddms_dataset_id, get_rddms_metadata

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
    metadata_version = shared_settings.get("metadata_version")
    settings = shared_settings.get("rddms")
    selected_dataspace = settings.get("dataspace")
    field_name = shared_settings.get("field_name")

    # print("Searching for Dataspaces in RDDMS:")
    # dataspaces = rddms_service.get_dataspaces()

    # for dataspace in dataspaces:
    #     print("Dataspace:", dataspace)

    print("-----------------------------------------------------------------")
    print(
        "Searching for seismic 4D attribute maps in RDDMS",
        selected_dataspace,
        metadata_version,
        field_name,
        " ...",
    )

    # cache_file = "metadata_cache_" + metadata_version + ".csv"
    # metadata_file_cache = os.path.join(config_folder, cache_file)

    # if os.path.isfile(metadata_file_cache):
    #     print("Reading cached metadata from", metadata_file_cache)
    #     metadata = pd.read_csv(metadata_file_cache)
    #     print(metadata[["Name", "SeismicTraceDataSourceNames"]])
    # else:
    print("Extracting metadata from OSDU RDDMS ...")
    print()

    attribute_horizons = rddms_service.get_attribute_horizons(
        dataspace_name=selected_dataspace, field_name=field_name
    )

    # print("Number of attribute maps:", len(attribute_horizons))
    print("Checking the extracted metadata ...")

    if len(attribute_horizons) == 0:
        exit()

    for attribute_horizon in attribute_horizons:
        if attribute_horizon:
            print(
                " -",
                attribute_horizon.FieldName,
                attribute_horizon.Name,
                attribute_horizon.SeismicTraceDataSourceNames,
                attribute_horizon.id,
                attribute_horizon.DatasetIDs,
            )

    metadata = get_osdu_metadata_attributes(attribute_horizons)
    # metadata.to_csv(metadata_file_cache)
    # print("Updated metadata stored to:", metadata_file_cache)

    updated_metadata = osdu_service.update_reference_dates(metadata)

    converted_metadata = convert_metadata(updated_metadata)
    print_metadata(converted_metadata)
    print()

    # selection_list = create_rddms_lists(converted_metadata, interval_mode)

    # print()
    # print("Webviz-4D selection list")
    # pprint(selection_list)

    # Get a selected map and display
    # data_source = "RDDMS"
    # attribute = "Value"
    # name = "3D+IUTU+JS+Z22+Merge_EQ20231_PH2DG3"
    # map_type = "observed"
    # seismic = "Timeshift"
    # difference = "---"
    # interval = "2023-05-16-2022-08-28"

    # data_source = "RDDMS"
    # attribute = "MaxPositive"
    # name = "FullReservoirEnvelope"
    # map_type = "observed"
    # seismic = "RelativeAcousticImpedance"
    # difference = "Timeshifted"
    # interval = "2023-09-20-2019-09-12"

    data_source = "RDDMS"
    attribute = "Average"
    name = "Total"
    map_type = "simulated"
    seismic = "SWAT"
    difference = "NotTimeshifted"
    interval = "2021-05-17-2020-09-30"

    # data_source = "rddms"
    # difference_type = "AttributeOfDifference"
    # coverage = "Full"
    # attribute = "Max"  # Customize
    # name = "Total"  # Customize
    # map_type = "observed"  # Customize
    # seismic = "Amplitude"  # Customize
    # difference = "NotTimeshifted"
    # interval = "2021-05-17-2020-09-30"

    ensemble = seismic
    real = difference

    data = {"attr": attribute, "name": name, "date": interval}
    uuid_url = get_rddms_dataset_id(converted_metadata, data, ensemble, real, map_type)

    if uuid_url is None:
        print("ERROR selected dataset not found")
        print("ensemble:", ensemble)
        print("real:", real)
        print("map_type:", map_type)
        pprint(data)
        exit()

    selected_metadata = converted_metadata[converted_metadata["dataset_id"] == uuid_url]
    uuid = selected_metadata["id"].values[0]
    horizon_name = selected_metadata["map_name"].values[0]

    if uuid is not None:
        print()
        print("Loading surface from", data_source)
        print(" - name:", horizon_name)
        print(" - uuid:", uuid)
        print(" - uuid_url:", uuid_url)

        start_time = time.time()

        surface = rddms_service.load_surface_from_rddms(
            dataspace_name=selected_dataspace,
            horizon_name=horizon_name,
            uuid=uuid,
            uuid_url=uuid_url,
        )

        if uuid == "4a236105-16ba-4f91-8a0d-5cc4a9508143":
            surface = rddms_service.get_auto4d_rddms_map(
                dataspace_name=selected_dataspace,
                horizon_name=horizon_name,
                uuid=uuid,
                uuid_url=uuid_url,
            )

        if surface:
            print(" --- %s seconds ---" % (time.time() - start_time))
            number_cells = surface.nrow * surface.ncol
            print(f"Surface size: {surface.nrow} x {surface.ncol} = {number_cells}")
            print()

            print(surface)
            surface.quickplot(title=horizon_name + " " + data_source)


if __name__ == "__main__":
    main()
