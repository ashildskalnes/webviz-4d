import os
import io
import numpy as np
import pandas as pd
import argparse
import xtgeo
import time

import warnings
from pprint import pprint

from fmu.sumo.explorer import Explorer

from webviz_4d._providers.osdu_provider._provider_impl_file import DefaultOsduService

from webviz_4d._datainput.common import read_config, print_metadata
from webviz_4d._datainput._auto4d import (
    get_auto4d_metadata,
    get_auto4d_filename,
)
from webviz_4d._datainput._fmu import (
    get_fmu_metadata,
    get_fmu_filename,
)

from webviz_4d._datainput._osdu import (
    get_osdu_metadata,
    get_osdu_dataset_id,
)

from webviz_4d._datainput._sumo import (
    create_sumo_lists,
    load_sumo_observed_metadata,
    get_sumo_interval_list,
    get_selected_surface,
    get_sumo_tagname,
)

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
    field_name = shared_settings.get("field_name")
    interval_mode = "normal"

    # Extract data sources and map defaults
    surface_viewer = config["layout"][0]["content"][1]["content"][0]["content"][0][
        "SurfaceViewer4D"
    ]

    map1_defaults = surface_viewer.get("map1_defaults")
    map2_defaults = surface_viewer.get("map2_defaults")
    map3_defaults = surface_viewer.get("map3_defaults")

    map_defaults = [map1_defaults, map2_defaults, map3_defaults]

    data_sources = []
    metadata_lists = []
    selection_lists = []

    for map_default in map_defaults:
        data_source = map_default.get("data_source")
        data_sources.append(data_source)

        print("Data source:", data_source)

        if data_source == "sumo":
            sumo = shared_settings.get("sumo")
            case_name = sumo.get("case_name")
            env = sumo.get("env_name")

            sumo_explorer = Explorer(env=env)
            cases = sumo_explorer.cases.filter(name=case_name)
            my_case = cases[0]

            field_name = shared_settings.get("field_name")
            metadata = load_sumo_observed_metadata(my_case)
            selection_list = create_sumo_lists(metadata, interval_mode)
        elif data_source == "auto4d_file":
            metadata, selection_list = get_auto4d_metadata(config)
        elif data_source == "fmu":
            metadata, selection_list = get_fmu_metadata(config, field_name)

        elif data_source == "osdu":
            osdu_service = DefaultOsduService()
            metadata, selection_list = get_osdu_metadata(
                config, osdu_service, field_name
            )
        else:
            print("ERROR: Data source not supported:", data_source)

        # print_metadata(metadata)
        metadata_lists.append(metadata)

        print()
        print("Selection list:")
        selection_lists.append(selection_list)
        pprint(selection_list)
        print()

    # Check the default maps
    for index, map_default in enumerate(map_defaults):
        print("Data source:", data_sources[index])
        data_source = map_default.get("data_source")
        ensemble = map_default.get("seismic")
        real = map_default.get("difference")
        attribute = map_default.get("attribute")
        name = map_default.get("name")
        interval = map_default.get("interval")
        map_type = map_default.get("map_type")
        coverage = map_default.get("coverage")

        data = {"attr": attribute, "name": name, "date": interval}

        metadata = metadata_lists[index]

        if data_source == "sumo":
            interval_list = get_sumo_interval_list(interval)
            tagname = get_sumo_tagname(
                metadata, name, ensemble, attribute, real, interval_list
            )

            surface = get_selected_surface(
                case=my_case,
                map_type=map_type,
                surface_name=name,
                attribute=tagname,
                time_interval=interval_list,
                iteration_name=None,
                realization=None,
            )

            if surface is not None:
                print(surface)
                print()
        elif data_source == "auto4d_file":
            path = get_auto4d_filename(
                metadata, data, ensemble, real, map_type, coverage
            )

            if os.path.isfile(path):
                surface = xtgeo.surface_from_file(path)
                print(surface)
                print()
        elif data_source == "fmu":
            surface_file = get_fmu_filename(data, ensemble, real, map_type, metadata)

            if os.path.isfile(surface_file):
                surface = xtgeo.surface_from_file(surface_file)
                print(surface)
                print()
        elif data_source == "osdu":
            dataset_id, map_name = get_osdu_dataset_id(
                metadata, data, ensemble, real, map_type, coverage
            )

            if dataset_id is not None:
                dataset = osdu_service.get_horizon_map(file_id=dataset_id)
                blob = io.BytesIO(dataset.content)
                surface = xtgeo.surface_from_file(blob)
                print(surface)


if __name__ == "__main__":
    main()
