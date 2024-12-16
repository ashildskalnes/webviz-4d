import numpy as np
import pandas as pd
from ast import literal_eval


import math
import numpy as np


# get angle to a vector. Returns an angle in [-180, +180]
# From Jon Magne Aagaard (AspenTech
def get_angle(dx, dy):
    if dx == 0.0 and dy == 0.0:  # NOSONAR This is perfectly fine here
        return 0.0

    r = math.sqrt(dx * dx + dy * dy)
    if dx > 0:  # 1st or 4th quadrant
        return math.degrees(math.asin(dy / r))
    if dy > 0:  # 2nd quadrant
        return math.degrees(math.pi - math.asin(dy / r))

    return math.degrees(math.pi + math.asin(-dy / r))  # 3rd quadrant


##########################################################
# Returns the grid size, spacing and rotation of a grid2d.
# We will assume a valid 2d grid
# From Jon Magne Aagaard (AspenTech)
##########################################################
def get_incs_and_angle(iinfo, jinfo, originx, originy, nj):
    i_offs = iinfo["Offset"]
    j_offs = jinfo["Offset"]
    i_spacing = iinfo["Spacing"]
    j_spacing = jinfo["Spacing"]

    iinc = i_spacing["Value"]
    jinc = j_spacing["Value"]
    idir = np.array([i_offs["Coordinate1"], i_offs["Coordinate2"]])
    jdir = np.array([j_offs["Coordinate1"], j_offs["Coordinate2"]])

    cross = np.cross(idir, jdir)
    angle = get_angle(idir[0], idir[1])

    # If the i j makes up a left handed system we just move the origin
    # and later shuffle the values accordingly
    right_handed = cross > 0
    if not right_handed:
        jdir_unit = jdir / np.linalg.norm(jdir)
        off = jdir_unit * (nj - 1) * jinc
        new_origin = np.array([originx, originy]) + off
        originx = float(new_origin[0])
        originy = float(new_origin[1])

    return (iinc, jinc, angle, originx, originy, right_handed)


# Important:
# If the resqml frame was left handed you need to shuffle any data around accordingly:
# if not right_handed:
#   a = np.reshape(a,(nj,ni))  # swap dims
#   a = np.flip(a,0)


# Dump from what I get from the open-etp-rest-api service (as part of a python dict)
# As you see I'm using the Point3dLatticeArray object here.
# the arguments iinfo and jinfo is the two dictionaries in the 'Offset' list.
# The originx and originy is just 'Coordinate1' and 'Coordinate2'.
# As one of you pointed out we have a thirds coordinate/dimension which we don't use here.

# 'Points': {'SupportingGeometry': {'Origin': {'Coordinate1': -10194.87109375,
#              'Coordinate2': 6634.9404296875,
#              'Coordinate3': 0,
#              '$type': 'resqml20.Point3d'},
#   'Offset': [{'Offset': {'Coordinate1': 0.00017903980381225274,
#                          'Coordinate2': -0.9999999839723742,
#                          'Coordinate3': 0,
#                          '$type': 'resqml20.Point3d'},
#               'Spacing': {'Value': 82.12829721475912,
#                           'Count': 89,
#                           '$type': 'resqml20.DoubleConstantArray'},
#               '$type': 'resqml20.Point3dOffset'},
#              {'Offset': {'Coordinate1': 0.9999999839723729,
#                          'Coordinate2': 0.00017903981083079037,
#                          'Coordinate3': 0,
#                          '$type': 'resqml20.Point3d'},
#               'Spacing': {'Value': 83.24235421976209,
#                           'Count': 125,
#                           '$type': 'resqml20.DoubleConstantArray'},
#               '$type': 'resqml20.Point3dOffset'}],
#   '$type': 'resqml20.Point3dLatticeArray'},
# }


def get_rddms_dataset_id(surface_metadata, data, ensemble, real, map_type):
    selected_interval = data["date"]
    name = data["name"]
    attribute = data["attr"]

    if selected_interval[0:10] > selected_interval[11:]:
        time2 = selected_interval[0:10]
        time1 = selected_interval[11:]
    else:
        time1 = selected_interval[0:10]
        time2 = selected_interval[11:]

    surface_metadata.replace(np.nan, "", inplace=True)

    items = [
        "difference",
        "seismic",
        "map_type",
        "time1",
        "time2",
        "name",
        "attribute",
        "map_type",
    ]
    print()
    print("Webviz-4D metadata")
    print(surface_metadata[items])

    try:
        selected_metadata = surface_metadata[
            (surface_metadata["difference"] == real)
            & (surface_metadata["seismic"] == ensemble)
            & (surface_metadata["map_type"] == map_type)
            & (surface_metadata["time1"] == time1)
            & (surface_metadata["time2"] == time2)
            & (surface_metadata["name"] == name)
            & (surface_metadata["attribute"] == attribute)
        ]

        print()
        print("Selected dataset info:")
        print(map_type, real, ensemble, name, attribute, time1, time2)

        if len(selected_metadata) > 1:
            print("WARNING number of datasets", len(selected_metadata))
            print(selected_metadata)

        dataset_id = selected_metadata["dataset_id"].values[0]
        return dataset_id
    except:
        dataset_id = None
        print("WARNING: Selected map not found in RDDMS. Selection criteria are:")
        print(map_type, real, ensemble, name, attribute, time1, time2)

    return dataset_id


def create_rddms_lists(metadata, interval_mode):
    selectors = {
        "name": "name",
        "interval": "interval",
        "attribute": "attribute",
        "seismic": "seismic",
        "difference": "difference",
    }

    map_types = ["observed", "simulated"]
    map_dict = {}

    for map_type in map_types:
        map_dict[map_type] = {}
        map_type_dict = {}

        map_type_metadata = metadata[metadata["map_type"] == map_type]
        map_type_metadata = map_type_metadata.where(~map_type_metadata.isna(), "")

        intervals_df = map_type_metadata[["time1", "time2"]]
        intervals = []

        for key, value in selectors.items():
            selector = key
            map_type_metadata = metadata[metadata["map_type"] == map_type]
            map_type_dict[value] = {}

            if selector == "interval":
                for _index, row in intervals_df.iterrows():
                    t1 = row["time1"]
                    t2 = row["time2"]

                    if type(t1) == str and type(t2) is str:
                        if interval_mode == "normal":
                            interval = t2 + "-" + t1
                        else:
                            interval = t1 + "-" + t2
                    else:  # Drogon data hack
                        t1 = "2018-01-01"
                        t2 = "2019-01-01"
                        interval = t2 + "-" + t1

                    if interval not in intervals:
                        intervals.append(interval)

                sorted_intervals = sorted(intervals)

                map_type_dict[value] = sorted_intervals
            else:
                items = list(map_type_metadata[selector].unique())
                # items.sort()

                map_type_dict[value] = sorted(items)

        map_dict[map_type] = map_type_dict

    return map_dict
