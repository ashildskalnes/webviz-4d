import os
import numpy as np
import pandas as pd
import argparse
import xtgeo
import time

import warnings
from pprint import pprint

from webviz_4d._datainput.common import read_config, print_metadata
from webviz_4d._datainput._auto4d import (
    load_auto4d_metadata_new,
    create_auto4d_lists,
)

warnings.filterwarnings("ignore")


def get_auto4d_filename(surface_metadata, data, ensemble, real, map_type, coverage):
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
    metadata_coverage = surface_metadata[surface_metadata["coverage"] == coverage]

    headers = [
        "attribute",
        "seismic",
        "difference",
        "time2",
        "time1",
        "map_name",
    ]

    print("Coverage", coverage)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.max_rows", None)
    pd.set_option("display.width", None)
    print(metadata_coverage[headers].sort_values(by="attribute"))

    try:
        selected_metadata = metadata_coverage[
            (metadata_coverage["difference"] == real)
            & (metadata_coverage["seismic"] == ensemble)
            & (metadata_coverage["map_type"] == map_type)
            & (metadata_coverage["time1"] == time1)
            & (metadata_coverage["time2"] == time2)
            & (metadata_coverage["strat_zone"] == name)
            & (metadata_coverage["attribute"] == attribute)
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
    shared_settings = config.get("shared_settings")
    metadata_version = shared_settings.get("metadata_version")
    map_type = shared_settings.get("map_type")

    sumo_settings = shared_settings.get("sumo")
    osdu_settings = shared_settings.get("osdu")
    rddms_settings = shared_settings.get("rddms")
    auto4d_settings = shared_settings.get("auto4d_file")

    directory = auto4d_settings.get("directory")

    metadata_format = auto4d_settings.get("metadata_format")
    acquisition_dates = auto4d_settings.get("acquisition_dates")
    interval_mode = shared_settings.get("interval_mode")
    # selections = auto4d_settings.get("selections")
    # coverage = selections.get("SeismicCoverage")

    print("Searching for seismic 4D attribute maps on disk:", directory, " ...")

    # attribute_metadata = load_auto4d_metadata(
    #     directory, metadata_format, metadata_version, selections, acquisition_dates
    # )

    attribute_metadata = load_auto4d_metadata_new(
        directory, metadata_format, metadata_version, acquisition_dates
    )

    print_metadata(attribute_metadata)

    print()
    print("Create auto4d selection lists ...")
    selection_list = create_auto4d_lists(attribute_metadata, interval_mode)
    pprint(selection_list)

    # Extract a selected map
    selection = {
        "data_source": "auto4d_file",
        "attribute": "MaxPositive",
        "name": "FullReservoirEnvelope",
        "map_type": "observed",
        "seismic": "Amplitude",
        "difference": "NotTimeshifted",
        "interval": "2023-05-16-2022-05-15",
        "difference_type": "AttributeOfDifference",
        "coverage": "Full",
    }

    ensemble = selection.get("seismic")
    real = selection.get("difference")
    attribute = selection.get("attribute")
    name = selection.get("name")
    interval = selection.get("interval")
    map_type = selection.get("map_type")
    coverage = selection.get("coverage")

    data = {"attr": attribute, "name": name, "date": interval}

    path = get_auto4d_filename(
        attribute_metadata, data, ensemble, real, map_type, coverage
    )

    if os.path.isfile(path):
        print()
        print("Loading surface from disk")
        start_time = time.time()
        surface = xtgeo.surface_from_file(path)
        print(" --- %s seconds ---" % (time.time() - start_time))

        print(surface)
        print()


if __name__ == "__main__":
    main()
