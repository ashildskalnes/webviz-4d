import os
import time
import xtgeo

from webviz_4d._datainput._auto4d import get_auto4d_filename
from webviz_4d._datainput._fmu import get_fmu_filename
from webviz_4d._datainput._sumo import (
    get_sumo_interval_list,
    get_selected_surface,
    get_sumo_tagname,
)


def print_surface_info(map_idx, tic, toc, surface):
    print()
    print(f"Map number {map_idx+1}: downloaded the surface in {toc - tic:0.2f} seconds")
    number_cells = surface.nrow * surface.ncol
    print(f"  - Surface size: {surface.nrow} x {surface.ncol} = {number_cells}")
    print()


def load_surface(surface_path):
    if ".map" in str(surface_path):
        fformat = "ijxyz"
    else:
        fformat = "irap_binary"

    return xtgeo.surface_from_file(surface_path, fformat=fformat)


def load_surface_from_sumo(
    map_idx, data_source, sumo_case, metadata, map_defaults, data, ensemble, real
):
    name = data["name"]
    attribute = data["attr"]
    map_type = map_defaults["map_type"]

    if map_type == "observed":
        ens = ensemble
        realization = real
    else:
        ens = None
        realization = None

    selected_interval = data["date"]
    time1 = selected_interval[0:10]
    time2 = selected_interval[11:]

    metadata_keys = [
        "map_index",
        "map_type",
        "surface_name",
        "attribute",
        "time_interval",
        "seismic",
        "difference",
    ]

    surface = None
    map_name = None

    if data_source == "sumo":
        seismic = map_defaults.get("seismic")
        difference = map_defaults.get("difference")
        interval = map_defaults.get("interval")

        interval_list = get_sumo_interval_list(interval)

        tagname = get_sumo_tagname(
            metadata, name, seismic, attribute, difference, interval_list
        )
        tic = time.perf_counter()
        surface, map_name = get_selected_surface(
            case=sumo_case,
            map_type=map_type,
            surface_name=name,
            attribute=tagname,
            time_interval=interval_list,
            iteration_name=ens,
            realization=realization,
        )
        toc = time.perf_counter()

    if surface is not None:
        print_surface_info(map_idx, tic, toc, surface)
    else:
        metadata_values = [
            map_type,
            name,
            attribute,
            [time1, time2],
            ensemble,
            real,
        ]
        print()
        print("Selected map not found in", data_source)
        print("  Selection criteria:")

        for index, metadata in enumerate(metadata_keys):
            print("  - ", metadata, ":", metadata_values[index])

    return surface, map_name


def load_surface_from_file(
    map_idx, data_source, metadata, map_defaults, data, ensemble, real, coverage
):
    name = data["name"]
    attribute = data["attr"]
    map_type = map_defaults["map_type"]

    selected_interval = data["date"]
    time1 = selected_interval[0:10]
    time2 = selected_interval[11:]

    metadata_keys = [
        "map_index",
        "map_type",
        "surface_name",
        "attribute",
        "time_interval",
        "seismic",
        "difference",
    ]

    surface = None
    map_name = None

    if data_source == "auto4d_file":
        surface_file, map_name = get_auto4d_filename(
            metadata, data, ensemble, real, map_type, coverage
        )
        surface, tic, toc = read_surface_file(surface_file, data_source)
        map_name = surface_file.split("/")[-1]
    elif data_source == "fmu":
        surface_file, map_name = get_fmu_filename(
            data, ensemble, real, map_type, metadata
        )
        surface, tic, toc = read_surface_file(surface_file, data_source)
        map_name = surface_file.split("/")[-1]
    else:
        print("ERROR load_surface_from_file")
        print("  - Data source not supported:", data_source)

    if surface is not None:
        print_surface_info(map_idx, tic, toc, surface)
    else:
        metadata_values = [
            map_type,
            name,
            attribute,
            [time1, time2],
            ensemble,
            real,
        ]
        print()
        print("Selected map not found in", data_source)
        print("  Selection criteria:")

        for index, metadata in enumerate(metadata_keys):
            print("  - ", metadata, ":", metadata_values[index])

    return surface, map_name


def read_surface_file(surface_file, data_source):
    if os.path.isfile(surface_file):
        print()
        print("Loading surface from:", data_source)
        tic = time.perf_counter()
        surface = load_surface(surface_file)
        toc = time.perf_counter()
    else:
        surface = None

    return surface, tic, toc


def get_auto_scaling(attribute_settings, surface, attribute_type):
    min_max = [None, None]
    attribute_settings = attribute_settings
    settings = attribute_settings.get(attribute_type)

    if settings:
        auto_scaling = settings.get("auto_scaling", 10)
    else:
        auto_scaling = 10

    if attribute_settings and attribute_type in attribute_settings.keys():
        colormap_type = attribute_settings.get(attribute_type).get("type")
        surface_max_val = surface.values.max()
        surface_min_val = surface.values.min()

        scaled_value = abs(surface.values.std())

        if "mean" in attribute_type.lower():
            scaled_value = (abs(surface_min_val) + abs(surface_max_val)) / 2

        max_val = scaled_value * auto_scaling

        if colormap_type == "diverging":
            min_val = -max_val
        elif colormap_type == "positive":
            min_val = 0
        elif colormap_type == "negative":
            min_val = -max_val
            max_val = 0
        else:
            min_val = -max_val
        min_max = [min_val, max_val]

    return min_max
