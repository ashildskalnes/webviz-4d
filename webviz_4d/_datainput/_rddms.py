import numpy as np
import pandas as pd
import time
import math
import numpy as np
from ast import literal_eval


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

    uuid = None
    uuid_url = None
    map_name = None

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

        if len(selected_metadata) > 1:
            print("WARNING number of datasets", len(selected_metadata))
            print(selected_metadata)

        uuid = selected_metadata["id"].values[0]
        uuid_url = selected_metadata["dataset_id"].values[0]
        map_name = selected_metadata["map_name"].values[0]
    except:
        print("WARNING: Selected map not found in RDDMS. Selection criteria are:")
        print(map_type, real, ensemble, name, attribute, time1, time2)

    return uuid, uuid_url, map_name


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


def get_osdu_metadata_attributes(horizons):
    metadata_dicts = []

    # print("Compiling all attribute data ...")
    start_time = time.time()

    for horizon in horizons:
        if horizon:
            metadata_dicts.append(horizon.__dict__)

    maps_df = pd.DataFrame(metadata_dicts)
    columns = maps_df.columns
    new_columns = [col.replace("_", ".") for col in columns]
    maps_df.columns = new_columns

    # print(" --- %s seconds ---" % (time.time() - start_time))
    # print()

    maps_updated_df = maps_df.replace("", "---")

    return maps_updated_df


def convert_metadata(osdu_metadata):
    ids = []
    surface_names = []
    attributes = []
    times1 = []
    times2 = []
    seismic_contents = []
    coverages = []
    differences = []
    datasets = []
    field_names = []
    map_types = []
    map_names = []
    zone_names = []

    headers = [
        "id",
        "name",
        "attribute",
        "time1",
        "time2",
        "seismic",
        "coverage",
        "difference",
        "dataset_id",
        "field_name",
        "map_type",
        "map_name",
    ]

    for _index, row in osdu_metadata.iterrows():
        if "0.4.2" in row["MetadataVersion"]:
            id = row["id"]
            field_name = row["FieldName"]
            attribute_type = row["AttributeExtractionType"]
            seismic_content = row["SeismicTraceAttribute"]
            coverage = row["SeismicCoverage"]
            difference = row["SeismicDifferenceType"]
            zone = row["StratigraphicZone"]
            dataset_ids = row["DatasetIDs"]

            if type(dataset_ids) == str:
                dataset_ids = literal_eval(dataset_ids)

            if dataset_ids and len(dataset_ids) == 1:
                dataset_id = dataset_ids[0]
            elif dataset_ids and len(dataset_ids) == 2:
                dataset_id = dataset_ids[1]
            else:
                dataset_id = "FullReservoirEnvelope"
        else:
            id = row["id"]
            field_name = row["AttributeMap.FieldName"]
            dataset_id = row["IrapBinaryID"]
            attribute_type = row["AttributeMap.AttributeType"]
            seismic_content = row["AttributeMap.SeismicTraceContent"]
            coverage = row["AttributeMap.Coverage"]
            difference = row["AttributeMap.SeismicDifference"]

            window_mode = row["CalculationWindow.WindowMode"]
            horizon_names = []

            if window_mode == "AroundHorizon":
                seismic_horizon = row["CalculationWindow.HorizonName"]
                seismic_horizon = seismic_horizon.replace("+", "_")
                horizon_names.append(seismic_horizon)
            elif window_mode == "BetweenHorizons":
                seismic_horizon = row["CalculationWindow.TopHorizonName"]
                seismic_horizon = seismic_horizon.replace("+", "_")
                horizon_names.append(horizon_names)

        name = row["Name"]

        if difference == "RawDifference" or difference == "NotTimeshifted":
            difference = "NotTimeshifted"
        elif difference == "TimeshiftedDifference" or difference == "Timeshifted":
            difference = "Timeshifted"
        elif difference == "":
            difference = "seismic_content"
        else:
            difference = "---"

        if "simulated" in name:
            map_type = "simulated"
        else:
            map_type = "observed"

        times = row["AcquisitionDates"]

        if type(times) == str:
            times = literal_eval(times)

        if times[0] != "" and times[0] != "":
            times1.append(times[0])
            times2.append(times[1])

            map_types.append(map_type)
            field_names.append(field_name)
            ids.append(id)
            datasets.append(dataset_id)
            attributes.append(attribute_type)
            seismic_contents.append(seismic_content)
            coverages.append(coverage)
            differences.append(difference)
            surface_names.append(zone)
            map_names.append(name)
        else:
            print(" - WARNING: No time interval information found:", name)

    zipped_list = list(
        zip(
            ids,
            surface_names,
            attributes,
            times1,
            times2,
            seismic_contents,
            coverages,
            differences,
            datasets,
            field_names,
            map_types,
            map_names,
        )
    )

    metadata = pd.DataFrame(zipped_list, columns=headers)
    metadata.fillna(value=np.nan, inplace=True)

    return metadata


def get_rddms_metadata(config, osdu_service, rddms_service, dataspace, field_name):
    shared_settings = config.get("shared_settings")
    interval_mode = shared_settings.get("interval_mode")

    attribute_horizons = rddms_service.get_attribute_horizons(
        dataspace_name=dataspace, field_name=field_name
    )

    metadata = get_osdu_metadata_attributes(attribute_horizons)
    updated_metadata = osdu_service.update_reference_dates(metadata)
    selected_field_metadata = updated_metadata[
        updated_metadata["FieldName"] == field_name
    ]
    selected_field_metadata["map_type"] = "observed"
    converted_metadata = convert_metadata(selected_field_metadata)
    selection_list = create_rddms_lists(converted_metadata, interval_mode)

    return converted_metadata, selection_list
