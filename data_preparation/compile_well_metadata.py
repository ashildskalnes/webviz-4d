#!/usr/bin/env python3
import os
import glob
import argparse
import yaml
import pandas as pd
from pandas import json_normalize
from webviz_4d._datainput import common
from webviz_4d._datainput.surface import load_surface
from webviz_4d._datainput.well import load_well


def extract_metadata(directory):
    """ Read and compile well metadata (from yaml files) """
    well_info = []
    interval_info = []

    print("Reading metadata in", directory)
    yaml_files = glob.glob(directory + "/.*.w.yaml", recursive=True)

    for yaml_file in yaml_files:
        # print(yaml_file)
        with open(yaml_file, "r") as stream:
            data = yaml.safe_load(stream)
            # print('data',data)

            well_info.append(data[0])

            if len(data) > 1 and data[1]:
                for item in data[1:]:
                    interval_info.append(item)

    well_info_df = json_normalize(well_info)
    interval_df = json_normalize(interval_info)

    if not interval_df.empty:
        interval_df.sort_values(
            by=["interval.wellbore", "interval.mdTop"], inplace=True
        )

    return well_info_df, interval_df


def compile_data(surface, well_directory, wellbore_info, well_suffix):
    """ Extract MD of the well's intersection with the provided depth surface """
    rms_names = []
    well_names = []
    short_names = []
    depth_surfaces = []
    depth_picks = []

    points = None
    pick_name = None
    wellbore_pick_md = None

    if surface:
        pick_name = surface.name

    if not wellbore_info.empty:
        for _index, item in wellbore_info.iterrows():
            wellbore_name = item["wellbore.name"]
            rms_name = (
                wellbore_name.replace("/", "_").replace("NO ", "").replace(" ", "_")
            )
            well_name = common.get_wellname(wellbore_info, wellbore_name)
            well_names.append(well_name)

            wellbore_file = os.path.join(well_directory, rms_name) + well_suffix
            wellbore = load_well(wellbore_file)
            short_name = wellbore.shortwellname

            if surface:
                points = wellbore.get_surface_picks(surface)
                pick_name = surface.name

            if points and hasattr(points, "dataframe"):
                print(points.dataframe)
                wellbore_pick_md = points.dataframe["MD"].values[0]

            depth_surfaces.append(pick_name)
            depth_picks.append(wellbore_pick_md)

            rms_names.append(rms_name)
            short_names.append(short_name)

    else:  # Planned wells
        wellbore_names = []
        wellbore_types = []
        wellbore_fluids = []
        wellbore_files = glob.glob(str(well_directory) + "/*.w")
        # print("wellbore_files", wellbore_files)

        for wellbore_file in wellbore_files:
            wellbore = load_well(wellbore_file)
            wellbore_name = wellbore.name.split("/")[0]
            wellbore_names.append(wellbore_name)
            well_names.append(wellbore_name)
            short_names.append(wellbore_name)
            rms_names.append(wellbore_name)
            wellbore_types.append("planned")
            wellbore_fluids.append("")

            if surface:
                points = wellbore.get_surface_picks(surface)

            if points and hasattr(points, "dataframe"):
                print(points.dataframe)
                wellbore_pick_md = points.dataframe["MD"].values[0]

            depth_surfaces.append(pick_name)
            depth_picks.append(wellbore_pick_md)

        wellbore_info["wellbore.name"] = well_names
        wellbore_info["wellbore.type"] = wellbore_types
        wellbore_info["wellbore.fluids"] = wellbore_fluids

    wellbore_info["wellbore.well_name"] = well_names
    wellbore_info["wellbore.rms_name"] = rms_names
    wellbore_info["wellbore.short_name"] = short_names
    wellbore_info["wellbore.rms_name"] = rms_names
    wellbore_info["wellbore.pick_name"] = depth_surfaces
    wellbore_info["wellbore.pick_md"] = depth_picks

    return wellbore_info


def main():
    """ Compile metadata from all wells and extract top reservoir depths """
    description = "Compile metadata from all wells and extract top reservoir depths"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "config_file", help="Enter path to the WebViz-4D configuration file"
    )
    args = parser.parse_args()

    print(description)
    print(args)

    config_file = args.config_file
    config = common.read_config(config_file)

    try:
        well_directory = common.get_config_item(config, "wellfolder")
        well_directory = common.get_full_path(well_directory)
    except:
        well_directory = None
        print("ERROR: Well directory", well_directory, "not found")
        print("Execution stopped")

    print("Well directory", well_directory)

    if well_directory:
        try:
            settings_file = common.get_config_item(config, "settings")
            settings_file = common.get_full_path(settings_file)
            settings = common.read_config(settings_file)

            surface_file = settings["depth_maps"]["top_reservoir"]
            surface = load_surface(surface_file)
        except:
            surface_file = None
            surface = None
    else:
        print("ERROR: Well data not found in", well_directory)
        exit()

    print("Surface file", surface_file)

    WELLBORE_INFO_FILE = "wellbore_info.csv"
    INTERVALS_FILE = "intervals.csv"
    WELL_SUFFIX = ".w"

    wellbore_info, intervals = extract_metadata(well_directory)
    pd.set_option("display.max_rows", None)
    print(wellbore_info)

    wellbore_info = compile_data(surface, well_directory, wellbore_info, WELL_SUFFIX)

    wellbore_info.to_csv(os.path.join(well_directory, WELLBORE_INFO_FILE))
    intervals.to_csv(os.path.join(well_directory, INTERVALS_FILE))

    # print(intervals)

    print("Metadata stored to " + os.path.join(well_directory, WELLBORE_INFO_FILE))
    print(
        "Completion intervals stored to " + os.path.join(well_directory, INTERVALS_FILE)
    )

    planned_wells_dir = [f.path for f in os.scandir(well_directory) if f.is_dir()]

    for folder in planned_wells_dir:
        wellbore_info = pd.DataFrame()
        wellbore_info = compile_data(surface, folder, wellbore_info, WELL_SUFFIX)

        wellbore_info.to_csv(os.path.join(folder, WELLBORE_INFO_FILE))
        print(wellbore_info)
        print("Metadata stored to " + os.path.join(folder, WELLBORE_INFO_FILE))


if __name__ == "__main__":
    main()
