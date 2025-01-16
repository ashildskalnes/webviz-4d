import os
import pandas as pd
import argparse
import warnings
from pprint import pprint
import prettytable as pt


from webviz_4d._datainput.common import read_config, print_metadata
from webviz_4d._providers.osdu_provider._provider_impl_file import DefaultOsduService
from webviz_4d._datainput._osdu import (
    get_osdu_metadata,
    get_osdu_dataset_id,
    load_selected_surface,
)

warnings.filterwarnings("ignore")


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
    metadata_version = shared_settings.get("metadata_version")
    interval_mode = shared_settings.get("interval_mode")
    osdu = shared_settings.get("osdu")
    coverage = osdu.get("coverage")

    cache_file = "metadata_cache_" + metadata_version + ".csv"
    metadata_file_cache = os.path.join(config_folder, cache_file)

    # # Delete cached data if existing
    # try:
    #     os.remove(metadata_file_cache)
    #     print(f"File '{metadata_file_cache}' deleted successfully.")

    # except FileNotFoundError:
    #     print(f"File '{metadata_file_cache}' not found.")

    if os.path.isfile(metadata_file_cache):
        print("Reading cached metadata from", metadata_file_cache)
        metadata = pd.read_csv(metadata_file_cache)
        selection_list = None
    else:
        print("Extracting metadata from OSDU Core ...")
        print()

        osdu_service = DefaultOsduService()

        metadata, selection_list = get_osdu_metadata(config, osdu_service, field_name)
        metadata.to_csv(metadata_file_cache)
        print("Metadata stored to:", metadata_file_cache)

    field_names = metadata["field_name"].unique()

    for field_name in field_names:
        print("Field name:", field_name)
        selected_field_metadata = metadata[metadata["field_name"] == field_name]
        selected_field_metadata["map_type"] = "observed"

        print_metadata(selected_field_metadata)

    print()

    if selection_list:
        pprint(selection_list)
        print()

    # Extract a selected map
    field_name = "JOHAN SVERDRUP"

    surface_viewer = (
        config.get("layout")[0]
        .get("content")[1]
        .get("content")[0]
        .get("content")[0]
        .get("SurfaceViewer4D")
    )
    map_defaults = surface_viewer.get("map1_defaults")

    print("Map defaults")
    pprint(map_defaults)

    attribute = map_defaults.get("attribute")
    name = map_defaults.get("name")
    interval = map_defaults.get("interval")
    seismic = map_defaults.get("seismic")
    difference = map_defaults.get("difference")
    map_type = map_defaults.get("map_type")

    ensemble = seismic
    real = difference

    data = {"attr": attribute, "name": name, "date": interval}

    data_source = map_defaults.get("data_source")

    print()
    print("Loading surface from", data_source)
    surface = load_selected_surface(
        data_source, metadata, map_defaults, data, ensemble, real, coverage
    )
    # start_time = time.time()
    # dataset = osdu_service.get_horizon_map(file_id=dataset_id)
    # blob = io.BytesIO(dataset.content)
    # surface = xtgeo.surface_from_file(blob)
    # print(" --- %s seconds ---" % (time.time() - start_time))

    print(surface)
    # surface.quickplot(title=original_name, colormap="rainbow_r")


if __name__ == "__main__":
    main()
