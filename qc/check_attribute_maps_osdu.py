import os
import io
import numpy as np
import pandas as pd
import time
import warnings
from datetime import datetime
import xtgeo

from webviz_4d._providers.osdu_provider._provider_impl_file import DefaultOsduService
from webviz_4d._datainput._osdu import (
    get_osdu_metadata_attributes,
    convert_metadata,
)

warnings.filterwarnings("ignore")

osdu_service = DefaultOsduService()


def get_osdu_dataset_id(surface_metadata, data, ensemble, real, map_type):
    selected_interval = data["date"]
    name = data["name"]
    attribute = data["attr"]

    time2 = selected_interval[0:10]
    time1 = selected_interval[11:]

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
    # Search for 4D maps
    print("Searching for seismic 4D attribute maps in OSDU ...")
    osdu_key = "tags.AttributeMap.FieldName"
    field_name = "JOHAN SVERDRUP"

    print("Selected FieldName =", field_name)
    metadata_version = "0.3.3"
    coverage = "Full"
    settings_folder = (
        "/private/ashska/dev_311/my_forks/fields/johan_sverdrup/osdu_config"
    )

    cache_file = "metadata_cache_" + coverage + ".csv"
    metadata_file_cache = os.path.join(settings_folder, cache_file)

    if os.path.isfile(metadata_file_cache):
        print("  Reading cached metadata from", metadata_file_cache)
        metadata = pd.read_csv(metadata_file_cache)
        updated_metadata = metadata.loc[metadata["AttributeMap.Coverage"] == coverage]
    else:
        print("Extracting metadata from OSDU ...")
        start_time = time.time()
        attribute_horizons = osdu_service.get_attribute_horizons(osdu_key, field_name)

        metadata = get_osdu_metadata_attributes(attribute_horizons)
        print(" --- %s seconds ---" % (time.time() - start_time))
        print()

        selected_attribute_maps = metadata.loc[
            (
                (metadata["MetadataVersion"] == metadata_version)
                & (metadata["Name"] == metadata["AttributeMap.Name"])
                & (metadata["AttributeMap.FieldName"] == field_name)
            )
        ]

        updated_metadata = osdu_service.update_reference_dates(selected_attribute_maps)
        updated_metadata.to_csv(metadata_file_cache)

    print(updated_metadata)

    validA = updated_metadata.loc[updated_metadata["AcquisitionDateA"] != ""]
    attribute_metadata = validA.loc[validA["AcquisitionDateB"] != ""]

    webviz_metadata = convert_metadata(attribute_metadata)
    print(webviz_metadata)

    output_file = (
        "metadata_" + field_name.replace(" ", "_") + "_" + metadata_version + ".csv"
    )
    webviz_metadata.to_csv(output_file)

    print("Selected metadata written to:", output_file)

    # Extract a selected map
    data_source = "OSDU"
    attribute = "MaxPositive"
    name = "3D_TAasgard_JS_Z22_Merge_EQ20231_PH2DG3"
    map_type = "observed"
    seismic = "Amplitude"
    difference = "RawDifference"
    interval = "2021-05-17-2020-09-30"

    ensemble = seismic
    real = difference

    data = {"attr": attribute, "name": name, "date": interval}

    dataset_id = get_osdu_dataset_id(webviz_metadata, data, ensemble, real, map_type)

    if dataset_id is not None:
        print("Loading surface from", data_source)
        tic = time.perf_counter()
        dataset = osdu_service.get_horizon_map(file_id=dataset_id)
        blob = io.BytesIO(dataset.content)
        surface = xtgeo.surface_from_file(blob)
        toc = time.perf_counter()

        print(surface)


if __name__ == "__main__":
    main()
