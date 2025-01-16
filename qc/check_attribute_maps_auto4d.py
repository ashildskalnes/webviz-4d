import os
import argparse
import warnings
from pprint import pprint

from webviz_4d._datainput.common import read_config, print_metadata
from webviz_4d._providers.osdu_provider._provider_impl_file import DefaultOsduService
from webviz_4d._datainput._auto4d import get_auto4d_metadata
from webviz_4d._datainput._maps import load_selected_surface

warnings.filterwarnings("ignore")


def main():
    description = "Check auto4d metadata"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("config_file")

    args = parser.parse_args()

    config_file = args.config_file
    config_file = os.path.abspath(config_file)
    config = read_config(config_file)
    shared_settings = config.get("shared_settings")
    metadata_version = shared_settings.get("metadata_version")
    map_type = shared_settings.get("map_type")

    sumo_settings = shared_settings.get("sumo")
    osdu_settings = shared_settings.get("osdu")
    rddms_settings = shared_settings.get("rddms")
    auto4d_settings = shared_settings.get("auto4d_file")

    directory = auto4d_settings.get("directory")

    print("Searching for seismic 4D attribute maps on disk:", directory, " ...")

    metadata, selection_list = get_auto4d_metadata(config)

    print_metadata(metadata)

    print()
    print("auto4d selection list ...")
    pprint(selection_list)

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
    coverage = map_defaults.get("coverage")

    ensemble = seismic
    real = difference

    data = {"attr": attribute, "name": name, "date": interval}

    data_source = map_defaults.get("data_source")
    surface = None

    print()
    print("Loading surface from", data_source)

    surface = load_selected_surface(
        data_source, metadata, map_defaults, data, ensemble, real, coverage
    )

    print(surface)
    print()


if __name__ == "__main__":
    main()
