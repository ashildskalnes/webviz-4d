import os
import numpy as np
import time
import xtgeo
from pprint import pprint

from webviz_4d._datainput._auto4d import get_auto4d_filename_new
from webviz_4d._datainput._fmu import get_fmu_filename_new
from webviz_4d._datainput._sumo import (
    get_sumo_interval_list,
    get_selected_surface,
    get_sumo_tagname,
)


def print_surface_info(map_idx, tic, toc, surface, map_name):
    print()
    print(f"Map number {map_idx+1}: downloaded the surface in {toc - tic:0.2f} seconds")
    number_cells = surface.nrow * surface.ncol
    print(f"  - Surface size: {surface.nrow} x {surface.ncol} = {number_cells}")
    print(f"  - Object name: {map_name}")
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
        print_surface_info(map_idx, tic, toc, surface, map_name)
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


def load_surface_from_sumo_new(
    map_idx, sumo_case, input_metadata, selectors, data, ensemble, real
):
    metadata = input_metadata.copy()
    metadata.replace(np.nan, "", inplace=True)

    selected_keys = list(selectors.keys())
    selected_values = list(data.values())
    selected_values = selected_values + [ensemble, real]

    selection_dict = dict(zip(selected_keys, selected_values))

    try:
        selected_metadata = metadata[
            (metadata[selected_keys[0]] == selected_values[0])
            & (metadata[selected_keys[1]] == selected_values[1])
            & (metadata[selected_keys[2]] == selected_values[2])
            & (metadata[selected_keys[3]] == selected_values[3])
            & (metadata[selected_keys[4]] == selected_values[4])
        ]

        print(selected_metadata)

        map_type = selected_metadata["map_type"].values[0]
        map_name = selected_metadata["map_name"].values[0]
        tagname = selected_metadata["tagname"].values[0]
        interval_list = selected_metadata["dates"].values[0]

        tic = time.perf_counter()
        surface, map_name = get_selected_surface(
            case=sumo_case,
            map_type=map_type,
            surface_name=map_name,
            attribute=tagname,
            time_interval=interval_list,
            iteration_name="",
            realization="",
        )
        toc = time.perf_counter()

    except:
        map_name = ""
        surface = None

        print("WARNING: Selected file not found in SUMO")
        print("  Selection criteria are:")
        pprint(selection_dict)

    if surface is not None:
        print_surface_info(map_idx, tic, toc, surface, map_name)
    else:
        print()
        print("Selected map not found in sumo")

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
        surface_file, map_name = get_auto4d_filename_new(
            metadata, data, ensemble, real, map_type, coverage
        )
        surface, tic, toc = read_surface_file(surface_file, data_source)
        map_name = surface_file.split("/")[-1]
    elif data_source == "fmu":
        surface_file, map_name = get_fmu_filename_new(
            data, ensemble, real, map_type, metadata
        )
        surface, tic, toc = read_surface_file(surface_file, data_source)
        map_name = surface_file.split("/")[-1]
    else:
        print("ERROR load_surface_from_file")
        print("  - Data source not supported:", data_source)

    if surface is not None:
        print_surface_info(map_idx, tic, toc, surface, map_name)
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

        for index, meta in enumerate(metadata_values):
            print("  - ", metadata_keys[index + 1], ":", meta)

    return surface, map_name


def load_surface_from_file_new(
    map_idx,
    selectors,
    metadata,
    map_defaults,
    data,
    ensemble,
    real,
):
    fixed_keys = list(map_defaults.keys())
    fixed_values = list(map_defaults.values())
    fixed_dict = dict(zip(fixed_keys, fixed_values))

    metadata_fixed = metadata[
        (metadata[fixed_keys[0]] == fixed_values[0])
        & (metadata[fixed_keys[1]] == fixed_values[1])
        & (metadata[fixed_keys[2]] == fixed_values[2])
        & (metadata[fixed_keys[3]] == fixed_values[3])
        & (metadata[fixed_keys[4]] == fixed_values[4])
    ]

    selection_columns = list(map_defaults.keys())
    selection_columns.append("filename")

    # print("DEBUG metadata_fixed")
    # print(metadata_fixed[selectors.keys()])

    data_source = map_defaults.get("data_source")

    surface = None
    map_name = None

    if data_source == "auto4d_file":
        surface_file, map_name = get_auto4d_filename_new(
            metadata_fixed, selectors, data, ensemble, real
        )
        surface, tic, toc = read_surface_file(surface_file, data_source)
        map_name = surface_file.split("/")[-1]
    elif data_source == "fmu":
        surface_file, map_name = get_fmu_filename_new(
            metadata_fixed, selectors, data, ensemble, real
        )
        surface, tic, toc = read_surface_file(surface_file, data_source)
        map_name = surface_file.split("/")[-1]
    else:
        print("ERROR load_surface_from_file")
        print("  - Data source not supported:", data_source)

    if surface is not None:
        print_surface_info(map_idx, tic, toc, surface, map_name)
    else:
        print()
        print("Selected map not found in", data_source)
        # print("  Selection criteria:")

    return surface, map_name


def read_surface_file(surface_file, data_source):
    if os.path.isfile(surface_file):
        tic = time.perf_counter()
        surface = load_surface(surface_file)
        toc = time.perf_counter()
    else:
        surface = None
        tic = ""
        toc = ""

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
