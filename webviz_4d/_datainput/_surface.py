import os
import math
import numpy as np
import numpy.ma as ma

import xtgeo
from webviz_config.common_cache import CACHE

from .image_processing import array_to_png, get_colormap

from webviz_4d._datainput._sumo import (
    get_realization_surface,
    get_aggregated_surface,
)

from webviz_4d._datainput.image_processing import get_colormap


@CACHE.memoize(timeout=CACHE.TIMEOUT)
def load_surface(surface_path):
    return xtgeo.surface_from_file(surface_path)


@CACHE.memoize(timeout=CACHE.TIMEOUT)
def get_surface_arr(surface, unrotate=True, flip=True):
    if unrotate:
        surface.unrotate()
    x_coord, y_coord, z_coord = surface.get_xyz_values()
    if flip:
        x_coord = np.flip(x_coord.transpose(), axis=0)
        y_coord = np.flip(y_coord.transpose(), axis=0)
        z_coord = np.flip(z_coord.transpose(), axis=0)
    z_coord.filled(np.nan)
    return [x_coord, y_coord, z_coord]


@CACHE.memoize(timeout=CACHE.TIMEOUT)
def get_surface_fence(fence, surface):
    return surface.get_fence(fence)


@CACHE.memoize(timeout=CACHE.TIMEOUT)
def make_surface_layer(
    surface,
    name="surface",
    min_val=None,
    max_val=None,
    color="inferno",
    hillshading=False,
    min_max_df=None,
    unit="",
):
    """Make LayeredMap surface image base layer"""
    zvalues = get_surface_arr(surface)[2]
    bounds = [[surface.xmin, surface.ymin], [surface.xmax, surface.ymax]]

    if min_max_df is not None and not min_max_df.empty:
        lower_limit = min_max_df["lower_limit"].values[0]

        if lower_limit is not None and not math.isnan(lower_limit):
            min_val = lower_limit

        upper_limit = min_max_df["upper_limit"].values[0]

        if upper_limit is not None and not math.isnan(upper_limit):
            max_val = upper_limit

    # Flip color scale if min_val > max_val
    if min_val and max_val and min_val > max_val:
        if "_r" in color:
            color = color[:-2]
        else:
            color = color + "_r"

        min_val_orig = min_val
        min_val = max_val
        max_val = min_val_orig

    if min_val is not None:
        zvalues[(zvalues < min_val) & (ma.getmask(zvalues) == ma.nomask)] = min_val

        if np.nanmin(zvalues) > min_val:
            zvalues[0, 0] = ma.nomask
            zvalues.data[0, 0] = min_val

    if max_val is not None:
        zvalues[(zvalues > max_val) & (ma.getmask(zvalues) == ma.nomask)] = max_val

        if np.nanmax(zvalues) < max_val:
            zvalues[-1, -1] = ma.nomask
            zvalues.data[-1, -1] = max_val

    min_val = min_val if min_val is not None else np.nanmin(zvalues)
    max_val = max_val if max_val is not None else np.nanmax(zvalues)

    return {
        "name": name,
        "checked": True,
        "base_layer": True,
        "data": [
            {
                "type": "image",
                "url": array_to_png(zvalues.copy()),
                "colormap": get_colormap(color),
                "bounds": bounds,
                "allowHillshading": hillshading,
                "minvalue": f"{min_val:.2f}" if min_val is not None else None,
                "maxvalue": f"{max_val:.2f}" if max_val is not None else None,
                "unit": str(unit),
            }
        ],
    }


def get_top_res_surface(surface_info, sumo_case):
    if sumo_case:
        surface = get_sumo_top_res_surface(surface_info, sumo_case)
    else:
        surface = get_top_res_surface_file(surface_info)

    return surface


def get_top_res_surface_file(surface_info):
    if surface_info is not None:
        name = surface_info.get("name")
        print("Load top reservoir surface:", name)

        if os.path.isfile(name):
            return xtgeo.surface_from_file(name)
        else:
            print("ERROR: File not found")
            return None
    else:
        print("WARNING: Top reservoir surface not defined")
        return None


def get_sumo_top_res_surface(surface_info, sumo_case):
    if surface_info is not None:
        name = surface_info.get("name")
        tagname = surface_info.get("tag_name")
        iter_name = surface_info.get("iter")
        real = surface_info.get("real")
        time_interval = [False, False]

        print("Load top reservoir surface from SUMO:", name, tagname, iter_name, real)

        if "realization" in real:
            real_id = real.split("-")[1]
            surface = get_realization_surface(
                case=sumo_case,
                surface_name=name,
                attribute=tagname,
                time_interval=time_interval,
                iteration_name=iter_name,
                realization=real_id,
            )
        else:
            surface = get_aggregated_surface(
                case=sumo_case,
                surface_name=name,
                attribute=tagname,
                time_interval=time_interval,
                iteration_name=iter_name,
                operation=real,
            )

    if surface:
        return surface.to_regular_surface()
    else:
        print(
            "ERROR: Top reservoir surface not loaded from SUMO",
        )
        return None


def open_surface_with_xtgeo(surface):
    if surface:
        surface_object = surface.to_regular_surface()
    else:
        surface_object = None
        print("ERROR: non-existing surface")

    return surface_object
