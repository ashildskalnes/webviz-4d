from pandas import concat
import numpy as np
from dotenv import load_dotenv
from pandas import DataFrame, json_normalize

from webviz_4d._providers.wellbore_provider._omnia import extract_omnia_session


def pozo_connect(omnia_path):
    return extract_omnia_session(omnia_path, "POZO")


def extract_planned_data(session, endpoint):
    plannedwells_df = DataFrame()
    response = session.get(endpoint, verify=True)

    if response.status_code == 200:
        results = response.json()
        plannedwells_df = DataFrame(results)
    else:
        print(
            f"Exception: connecting to {endpoint} {response.status_code} {response.reason}"
        )
    return plannedwells_df


def extract_plannedWell_position(selected_wells_df):
    frames = []

    for _i, planned_well in selected_wells_df.iterrows():
        well_name = planned_well["name"]
        field_name = planned_well["fieldName"]
        well_points = planned_well["wellPoints"]
        well_points = DataFrame(well_points)

        if not well_points.empty:
            md = well_points["measuredDepth"].to_numpy()
            position = json_normalize(well_points[["position"][0]])

            frame = DataFrame()
            frame["easting"] = position["x"].to_numpy()
            frame["northing"] = position["y"].to_numpy()
            frame["tvdmsl"] = -position["z"].to_numpy()
            frame["md"] = md
            frame["name"] = well_name
            frame["field_name"] = field_name

            frames.append(frame)

    planned_trajetories = concat(frames)

    return planned_trajetories
