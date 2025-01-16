import os
import argparse
import warnings
from pprint import pprint

from webviz_4d._datainput.common import read_config, print_metadata
from webviz_4d._providers.osdu_provider._provider_impl_file import DefaultOsduService
from webviz_4d._datainput._auto4d import get_auto4d_metadata
from webviz_4d._datainput._sumo import get_sumo_metadata
from webviz_4d._datainput._fmu import get_fmu_metadata
from webviz_4d._datainput._maps import load_surface_from_file, load_surface_from_sumo
from webviz_4d._datainput._osdu import get_osdu_metadata, load_surface_from_osdu

warnings.filterwarnings("ignore")


def main():
    description = "Extract and check metadata from different sources"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("config_file")

    args = parser.parse_args()

    config_file = args.config_file
    config_file = os.path.abspath(config_file)
    config = read_config(config_file)
    shared_settings = config.get("shared_settings")
    field_name = shared_settings.get("field_name")

    surface_viewer = (
        config.get("layout")[0]
        .get("content")[1]
        .get("content")[0]
        .get("content")[0]
        .get("SurfaceViewer4D")
    )
    map_defaults = surface_viewer.get("map1_defaults")

    print("Map defaults:")
    pprint(map_defaults)

    data_source = map_defaults.get("data_source")

    if data_source == "auto4d_file":
        metadata, selection_list = get_auto4d_metadata(config)
    elif data_source == "fmu":
        metadata, selection_list = get_fmu_metadata(config, field_name)
    elif data_source == "sumo":
        metadata, selection_list, sumo_case = get_sumo_metadata(config, field_name)
    elif data_source == "osdu":
        osdu_service = DefaultOsduService()
        metadata, selection_list = get_osdu_metadata(config, osdu_service, field_name)
    else:
        print("ERROR Data source not supported:", data_source)
        exit

    print("Data source:", data_source)
    print_metadata(metadata)

    print()
    print("Selection list:")
    pprint(selection_list)

    # Extract a selected map
    map_idx = 0
    attribute = map_defaults.get("attribute")
    name = map_defaults.get("name")
    interval = map_defaults.get("interval")
    seismic = map_defaults.get("seismic")
    difference = map_defaults.get("difference")
    coverage = map_defaults.get("coverage")

    ensemble = seismic
    real = difference

    data = {"attr": attribute, "name": name, "date": interval}

    if data_source == "auto4d_file" or data_source == "fmu":
        surface = load_surface_from_file(
            map_idx, data_source, metadata, map_defaults, data, ensemble, real, coverage
        )
    elif data_source == "sumo":
        surface = load_surface_from_sumo(
            map_idx,
            data_source,
            sumo_case,
            metadata,
            map_defaults,
            data,
            ensemble,
            real,
        )
    elif data_source == "osdu":
        surface = load_surface_from_osdu(
            map_idx, data_source, metadata, map_defaults, data, ensemble, real, coverage
        )

    print(surface)
    print()


if __name__ == "__main__":
    main()
