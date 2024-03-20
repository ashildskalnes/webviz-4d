from pathlib import Path
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as pl
from matplotlib import colors
from matplotlib import cm
from matplotlib.colors import ListedColormap

from webviz_4d.plugins._surface_viewer_4D._webvizstore import get_path
from webviz_4d._datainput.common import find_files


def get_custom_colormaps(colormaps_folder):
    if colormaps_folder is not None:
        colormap_files = [
            get_path(Path(fn)) for fn in json.load(find_files(colormaps_folder, ".csv"))
        ]
        print("Loading custom colormaps from:", colormaps_folder)
        load_custom_colormaps(colormap_files)


def load_custom_colormaps(csv_files):
    """Load custom colormaps (stored as csv files)"""

    for csv_file in csv_files:
        colormap_df = pd.read_csv(csv_file)
        array = np.empty([256, 3])

        array[:, 0] = colormap_df["red"].values
        array[:, 1] = colormap_df["green"].values
        array[:, 2] = colormap_df["blue"].values

        name = colormap_df["name"].unique()[0]

        if not name in pl.colormaps():
            color_map = colors.LinearSegmentedColormap.from_list(name, array)
            pl.register_cmap(cmap=color_map)

            color_map_r = color_map.reversed()
            pl.register_cmap(cmap=color_map_r)


def change_inferno():
    """Change the start of the inferno colorscale from black to gray"""
    red = 0.28125
    green = 0.2578
    blue = 0.28125
    n_values = 50

    new_inferno = change_colormap("inferno", red, green, blue, n_values)

    return new_inferno.colors


def change_colormap(name, red_start, green_start, blue_start, n_values):
    """Modify an existing colormap by changing the start color"""

    colormap = cm.get_cmap(name, 256)
    newcolors = colormap(np.linspace(0, 1, 256))
    # pylint: disable=no-member

    red_delta = (colormap.colors[n_values, 0] - red_start) / n_values
    green_delta = (colormap.colors[n_values, 1] - green_start) / n_values
    blue_delta = (colormap.colors[n_values, 2] - blue_start) / n_values

    for i in range(0, n_values):
        gray_color = np.array(
            [
                i * red_delta + red_start,
                i * green_delta + green_start,
                i * blue_delta + blue_start,
                1,
            ]
        )
        newcolors[i, :] = gray_color

    newcmp = ListedColormap(newcolors)

    return newcmp


def get_colormap_settings(configuration, attribute):
    colormap = None
    minval = None
    maxval = None

    try:
        attribute_dict = configuration[attribute]
        colormap = attribute_dict["colormap"]
        minval = attribute_dict["min_value"]
        minval = attribute_dict["max_value"]
    except:
        try:
            map_settings = configuration("map_settings")
            colormap = map_settings("default_colormap")
        except:
            print("No default colormaps found for ", attribute)

    return colormap, minval, maxval

def get_auto_colormap_settings(configuration, attribute):
    colormap = None
    colormap_type = None
    percentile = None

    try:
        attribute_dict = configuration[attribute]
        colormap = attribute_dict["color"]
        colormap_type = attribute_dict["type"]
        percentile = attribute_dict["percentile"]
    except:
        try:
            map_settings = configuration("map_settings")
            colormap = map_settings("default_colormap")
        except:
            print("No default colormaps found for ", attribute)

    return colormap, colormap_type, percentile
