import os
import numpy as np
import pandas as pd
import argparse
import xtgeo
import time

import warnings
from pprint import pprint

from webviz_4d._datainput.common import read_config
from webviz_4d._datainput._auto4d import (
    load_auto4d_metadata,
    create_auto4d_lists,
)

warnings.filterwarnings("ignore")


def get_auto4d_filename(surface_metadata, data, ensemble, real, map_type):
    selected_interval = data["date"]
    name = data["name"]
    attribute = data["attr"]
    interval_mode = "normal"

    if interval_mode == "normal":
        time2 = selected_interval[0:10]
        time1 = selected_interval[11:]
    else:
        time1 = selected_interval[0:10]
        time2 = selected_interval[11:]

    surface_metadata.replace(np.nan, "", inplace=True)

    try:
        selected_metadata = surface_metadata[
            (surface_metadata["difference"] == real)
            & (surface_metadata["seismic"] == ensemble)
            & (surface_metadata["map_type"] == map_type)
            & (surface_metadata["time.t1"] == time1)
            & (surface_metadata["time.t2"] == time2)
            & (surface_metadata["strat_zone"] == name)
            & (surface_metadata["attribute"] == attribute)
        ]

        filepath = selected_metadata["filename"].values[0]
        path = filepath

    except:
        path = ""
        print("WARNING: Selected file not found in Auto4d directory")
        print("  Selection criteria are:")
        print("  -  ", map_type, name, attribute, time1, time2, ensemble, real)

    return path


def main():
    description = "Check auto4d metadata"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("config_file")

    args = parser.parse_args()

    config_file = args.config_file
    config_file = os.path.abspath(config_file)
    config = read_config(config_file)
    config_folder = os.path.dirname(config_file)

    shared_settings = config.get("shared_settings")
    auto4d_settings = shared_settings.get("auto4d")
    directory = auto4d_settings.get("directory")
    metadata_version = auto4d_settings.get("metadata_version")
    metadata_format = auto4d_settings.get("metadata_format")
    acquisition_dates = auto4d_settings.get("acquisition_dates")
    interval_mode = shared_settings.get("interval_mode")
    selections = None

    print("Searching for seismic 4D attribute maps on disk ...")

    attribute_metadata = load_auto4d_metadata(
        directory, metadata_format, metadata_version, selections, acquisition_dates
    )

    data_viewer_columns = {
        "FieldName": "field_name",
        "BinGridName": "bin_grid_name",
        "Name": "map_name",
        "Zone": "strat_zone",
        "MapTypeDim": "map_dim",
        "SeismicAttribute": "seismic",
        "AttributeType": "attribute",
        "Coverage": "coverage",
        "DifferenceType": "difference",
        "AttributeDiff": "diff_type",
        "Dates": "dates",
        "Version": "metadata_version",
    }

    standard_metadata = pd.DataFrame()
    for key, value in data_viewer_columns.items():
        standard_metadata[key] = attribute_metadata[value]

    pd.set_option("display.max_rows", None)
    print(standard_metadata)

    output_file = os.path.join(
        config_folder, "standard_metadata_" + os.path.basename(config_file)
    )
    output_file = output_file.replace("yaml", "csv")
    standard_metadata.to_csv(output_file)
    print("Standard metadata written to", output_file)

    output_file = os.path.join(
        config_folder, "metadata_" + os.path.basename(config_file)
    )
    output_file = output_file.replace("yaml", "csv")
    attribute_metadata.to_csv(output_file)
    print("All metadata writen to", output_file)

    print("Create auto4d selection lists ...")
    selection_list = create_auto4d_lists(attribute_metadata, interval_mode)

    pprint(selection_list)

    # Extract a selected map
    data_source = "Auto4d"
    attribute = "Value"
    name = "3D+IUTU+JS+Z22+Merge_EQ20231_PH2DG3"
    map_type = "observed"
    seismic = "Timeshift"
    difference = "---"
    interval = "2021-05-17-2020-09-30"

    ensemble = seismic
    real = difference

    data = {"attr": attribute, "name": name, "date": interval}

    path = get_auto4d_filename(attribute_metadata, data, ensemble, real, map_type)

    if os.path.isfile(path):
        print("Loading surface from", data_source)
        start_time = time.time()
        surface = xtgeo.surface_from_file(path)
        print(" --- %s seconds ---" % (time.time() - start_time))

        print(surface)


if __name__ == "__main__":
    main()
