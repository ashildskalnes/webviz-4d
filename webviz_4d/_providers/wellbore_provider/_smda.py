import pandas as pd

from webviz_4d._providers.wellbore_provider._omnia import extract_omnia_session


def smda_connect(omnia_path):
    return extract_omnia_session(omnia_path, "SMDA")


def extract_data(session, endpoint, selection, filter):
    extracted_df = pd.DataFrame()
    frames = []

    filter = filter.replace(" ", "%20").replace("/", "%2F")

    # if selection != "":
    #     endpoint = endpoint + "_items=9000" + "&_projection=" + selection + "&" + filter
    # else:
    #     endpoint = endpoint + "_items=9000" + "&" + filter
    endpoint = endpoint + "_items=9000" + "&" + filter
    response = session.get(endpoint)

    if response.status_code == 200:
        results = response.json()["data"]["results"]

        frames.append(pd.DataFrame(results))
        next = response.json()["data"]["next"]

        while next is not None:
            endpoint_next = endpoint + "&_next=" + next
            response = session.get(endpoint_next)
            next = response.json()["data"]["next"]

            try:
                results = response.json()["data"]["results"]
                frames.append(pd.DataFrame(results))
            except:
                try:
                    results = [response.json()]
                    next = response.json()["data"]["next"]
                    frames.append(pd.DataFrame(results))
                except:
                    results = pd.DataFrame()
                    print("WARNING: No valid data extracted:")
                    print("         endpoint:", endpoint)

        extracted_df = pd.concat(frames)

    elif response.status_code == 404:
        print(
            f"{str(response.status_code) } {endpoint} either does not exists or can not be found"
        )
    else:
        print(
            f"[WARNING:] Can not fetch data from endpont {endpoint}  ({ str(response.status_code)})-{response.reason} "
        )

    return extracted_df


def extract_metadata(smda_address, filter):
    endpoint = smda_address.api + "wellbores?"

    columns = [
        "uuid",
        "unique_wellbore_identifier",
        "unique_well_identifier",
        "parent_wellbore",
        "purpose",
        "status",
        "content",
        "field_identifier",
        "field_uuid",
        "drill_start_date",
        "total_depth_driller_tvd",
        "completion_date",
        "license_identifier",
    ]

    metadata = extract_data(smda_address.session, endpoint, "", filter)

    if not metadata.empty:
        metadata = metadata[columns]

    return metadata


def extract_trajectories(smda_address, filter):
    endpoint = smda_address.api + "wellbore-survey-samples?"
    columns = [
        "wellbore_uuid",
        "unique_wellbore_identifier",
        "easting",
        "northing",
        "tvd_msl",
        "md",
    ]

    selection = ""
    for column in columns:
        selection = selection + "," + column

    trajectories_df = extract_data(smda_address.session, endpoint, selection, filter)

    endpoint = smda_address.api + "wellbore-survey-headers?"
    selection = "projected_coordinate_system"
    survey_df = extract_data(smda_address.session, endpoint, selection, filter)

    if not survey_df.empty:
        crs = survey_df["projected_coordinate_system"][0]
    else:
        crs = None

    if not trajectories_df.empty:
        trajectories_df.sort_values(
            by=["unique_wellbore_identifier", "md"], inplace=True
        )

    return trajectories_df, crs


def extract_picks(smda_address, filter):
    endpoint = smda_address.api + "wellbore-picks?"
    columns = [
        "unique_wellbore_identifier",
        "pick_identifier",
        "pick_type",
        "interpreter",
        "md",
        "tvd_msl",
        "obs_no",
    ]
    selection = ""
    for column in columns:
        selection = selection + "," + column

    return extract_data(smda_address.session, endpoint, selection, filter)


# def extract_planned_data(session, endpoint, field_name):
#     filter = "field_identifier=" + field_name + "&" + "wellbore_status=planning"
#     metadata = extract_data(session, endpoint, "", filter)
#     # plannedwells_df = pd.DataFrame()
#     # response = session.get(endpoint, verify=True)

#     # if response.status_code == 200:
#     #     results = response.json()
#     #     plannedwells_df = pd.DataFrame(results)
#     # else:
#     #     print(
#     #         f"Exception: connecting to {endpoint} {response.status_code} {response.reason}"
#     #    )
#     return metadata


# def extract_plannedWell_position(selected_wells_df):
#     frames = []

#     for _i, planned_well in selected_wells_df.iterrows():
#         well_name = planned_well["unique_wellbore_identifier"]
#         field_name = planned_well["field_identifier"]
#         well_points = planned_well["wellPoints"]
#         well_points = pd.DataFrame(well_points)

#         if not well_points.empty:
#             md = well_points["measuredDepth"].to_numpy()
#             position = pd.json_normalize(well_points[["position"][0]])

#             frame = pd.DataFrame()
#             frame["easting"] = position["x"].to_numpy()
#             frame["northing"] = position["y"].to_numpy()
#             frame["tvdmsl"] = -position["z"].to_numpy()
#             frame["md"] = md
#             frame["name"] = well_name
#             frame["field_name"] = field_name

#             frames.append(frame)

#     planned_trajetories = pd.concat(frames)

#     return planned_trajetories


def extract_trajectories(smda_address, filter):
    endpoint = smda_address.api + "wellbore-survey-samples?"
    columns = [
        "wellbore_uuid",
        "unique_wellbore_identifier",
        "easting",
        "northing",
        "tvd_msl",
        "md",
    ]

    selection = ""
    for column in columns:
        selection = selection + "," + column

    trajectories_df = extract_data(smda_address.session, endpoint, selection, filter)

    endpoint = smda_address.api + "wellbore-survey-headers?"
    selection = "projected_coordinate_system"
    survey_df = extract_data(smda_address.session, endpoint, selection, filter)

    if not survey_df.empty:
        crs = survey_df["projected_coordinate_system"][0]
    else:
        crs = None

    if not trajectories_df.empty:
        trajectories_df.sort_values(
            by=["unique_wellbore_identifier", "md"], inplace=True
        )

    return trajectories_df, crs


def extract_planned_metadata(smda_address, orig_filter):
    endpoint = smda_address.api + "wellbores?"

    columns = [
        "unique_wellbore_identifier",
        "status",
        "field_identifier",
        "field_uuid",
        "update_date",
    ]

    filter = orig_filter + "&status=planning"

    metadata = extract_data(smda_address.session, endpoint, "", filter)

    if not metadata.empty:
        metadata = metadata[columns]

    endpoint = smda_address.api + "wellbore-plan-survey-headers?"
    filter = orig_filter + "&wellbore_status=planning"

    extra_metadata = extract_data(smda_address.session, endpoint, "", filter)
    extra_metadata = extra_metadata[["unique_wellbore_identifier", "design_name"]]

    all_metadata = metadata.loc[
        metadata["unique_wellbore_identifier"].isin(
            extra_metadata["unique_wellbore_identifier"].values
        )
    ]

    sorted_metadata = all_metadata.sort_values(by="unique_wellbore_identifier")
    extra_metadata.sort_values(by="unique_wellbore_identifier", inplace=True)

    sorted_metadata["design_name"] = list(extra_metadata["design_name"].values)

    return sorted_metadata


def extract_planned_trajectories(smda_address, metadata, wellbore_name):
    endpoint = smda_address.api + "wellbore-plan-survey-samples?"
    columns = [
        "wellbore_uuid",
        "unique_wellbore_identifier",
        "easting",
        "northing",
        "tvd_msl",
        "md",
    ]

    selection = ""

    for column in columns:
        selection = selection + "," + column

    if wellbore_name and wellbore_name != "":
        filter = "&unique_wellbore_identifier=" + wellbore_name
        trajectory_df = extract_data(smda_address.session, endpoint, selection, filter)
        trajectory_df = trajectory_df[columns]

        trajectories_df = [trajectory_df]

    else:
        frames = []

        for _index, row in metadata.iterrows():
            filter = "&unique_wellbore_identifier=" + row["unique_wellbore_identifier"]
            trajectory_df = extract_data(
                smda_address.session, endpoint, selection, filter
            )
            trajectory_df = trajectory_df[columns]
            frames.append(trajectory_df)

        trajectories_df = pd.concat(frames)

    return trajectories_df
