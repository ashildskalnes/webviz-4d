import os
import io
import numpy as np
import pandas as pd
import argparse
import time
from pprint import pprint
import xtgeoviz.plot as xtgplot
import xtgeo
from ast import literal_eval

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
    # print(surface_metadata[items])

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

        if len(selected_metadata) != 1:
            print("WARNING number of datasets =", len(selected_metadata))
            print(selected_metadata)

        dataset_id = selected_metadata["dataset_id"].values[0]
        map_name = selected_metadata["map_names"].values[0]

        print("  ", map_name, dataset_id)

        return dataset_id
    except:
        dataset_id = None
        print("WARNING: Selected map not found in OSDU. Selection criteria are:")
        print(map_type, real, ensemble, name, attribute, time1, time2)

    if type(dataset_id) == str:
        dataset_id = literal_eval(dataset_id)

    return dataset_id


def main():
    # Check attribute maps in Core and RDDMS
    field_name = "JOHAN SVERDRUP"
    metadata_version = "0.4.2"
    selected_dataspace = "auto4d/test-cloud2"

    # Extract one selected map
    # map_name = "4D_JS_FulRes_20au-19auM_DiffTS_0535_rms"
    # field_name = "JOHAN SVERDRUP"
    # data_source = "OSDU"
    # attribute = "RMS"
    # name = "FullReservoirEnvelope"
    # map_type = "observed"
    # seismic = "Amplitude"
    # difference = "Timeshifted"
    # interval = "2023-05-16-2022-05-15"

    map_name = "draupne_fm_1_js_top--depth_structural_model"
    field_name = "JOHAN SVERDRUP"
    data_source = "OSDU"
    attribute = "RMS"
    name = "FullReservoirEnvelope"
    map_type = "observed"
    seismic = "Amplitude"
    difference = "Timeshifted"
    interval = "2023-05-16-2022-05-15"

    ensemble = seismic
    real = difference

    data = {"attr": attribute, "name": name, "date": interval}

    print("Extracting 4D attribute from OSDU Core ...")
    attribute_horizons = osdu_service.get_attribute_horizons(
        metadata_version=metadata_version, field_name=field_name
    )

    print("Number of attribute maps:", len(attribute_horizons))
    print("Checking all metadata ...")

    if len(attribute_horizons) == 0:
        exit()

    metadata = get_osdu_metadata_attributes(attribute_horizons)

    # osdu_metadata = metadata[metadata["FieldName"] == field_name]
    updated_metadata = osdu_service.update_reference_dates(metadata)
    updated_metadata["map_type"] = "observed"

    osdu_converted_metadata = convert_metadata(updated_metadata)

    dataset_id = get_osdu_dataset_id(
        osdu_converted_metadata, data, ensemble, real, map_type
    )

    if dataset_id is not None:
        print("Loading surface from", data_source)
        start_time = time.time()
        dataset = osdu_service.get_horizon_map(file_id=dataset_id)
        blob = io.BytesIO(dataset.content)
        osdu_surface = xtgeo.surface_from_file(blob)

        print(" --- %s seconds ---" % (time.time() - start_time))
        print()

    print("Extracting 4D attribute from OSDU RDDMS ...")
    attribute_horizons = rddms_service.get_attribute_horizons(
        selected_dataspace, field_name
    )

    metadata = get_osdu_metadata_attributes(attribute_horizons)
    updated_metadata = osdu_service.update_reference_dates(metadata)
    updated_metadata["map_type"] = "observed"

    rddms_converted_metadata = convert_metadata(updated_metadata)

    uuid_url = get_rddms_dataset_id(
        rddms_converted_metadata, data, ensemble, real, map_type
    )

    if uuid_url is None:
        print("ERROR selecte dataset not found")
        print("ensemble:", ensemble)
        print("real:", real)
        print("map_type:", map_type)
        pprint(data)
        exit()

    selected_metadata = rddms_converted_metadata[
        rddms_converted_metadata["dataset_id"] == uuid_url
    ]
    uuid = selected_metadata["id"].values[0]
    horizon_name = selected_metadata["original_name"].values[0]

    if uuid is not None:
        print("Loading surface from RDDMS")
        start_time = time.time()

        rddms_surface = rddms_service.get_rddms_map(
            dataspace_name=selected_dataspace,
            horizon_name=horizon_name,
            uuid=uuid,
            uuid_url=uuid_url,
        )

        print(" --- %s seconds ---" % (time.time() - start_time))

    # Print and plot surfaces
    # print(osdu_surface)
    print(rddms_surface)

    # osdu_surface.quickplot(title=map_name + " Core")
    rddms_surface.quickplot(title=map_name + " RDDMS")


if __name__ == "__main__":
    main()
