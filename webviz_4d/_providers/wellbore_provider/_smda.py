import pandas as pd

from webviz_4d._providers.wellbore_provider._omnia import extract_omnia_session


def smda_connect(omnia_path):
    return extract_omnia_session(omnia_path, "SMDA")


def get_short_wellname(wellname):
    """Well name on a short name form where blockname and spaces are removed.
    This should cope with both North Sea style and Haltenbanken style.
    E.g.: '31/2-G-5 AH' -> 'G-5AH', '6472_11-F-23_AH_T2' -> 'F-23AHT2'
    """
    newname = []
    first1 = False
    first2 = False
    for letter in wellname:
        if first1 and first2:
            newname.append(letter)
            continue
        if letter in ("_", "/"):
            first1 = True
            continue
        if first1 and letter == "-":
            first2 = True
            continue

    xname = "".join(newname)
    xname = xname.replace("_", "")
    xname = xname.replace(" ", "")
    return xname


def get_wellbore_aliases(token, api, wellbores):
    wellbore_aliases = []
    endpoint = api + "wellbore-alias?"

    filter = "&wellbore_name_set=multilateral%20well%20name"
    mlt_aliases_df = extract_data(token, endpoint, filter)

    mlt_aliases_list = mlt_aliases_df["unique_wellbore_identifier"].to_list()
    wellbore_names = wellbores["unique_wellbore_identifier"].to_list()

    for wellbore in wellbore_names:
        mlt_alias = ""

        if wellbore in mlt_aliases_list:
            mlt_alias = mlt_aliases_df[
                mlt_aliases_df["unique_wellbore_identifier"] == wellbore
            ]
            alias = mlt_alias["alias"].values[0]

        wellbore_aliases.append(alias)

    return wellbore_aliases


def extract_data(session, endpoint, filter):
    extracted_df = pd.DataFrame()
    frames = []

    filter = filter.replace(" ", "%20").replace("/", "%2F")

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
    alias_endpoint = smda_address.api + "wellbore-alias?"

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

    metadata = extract_data(smda_address.session, endpoint, filter)
    metadata.dropna(subset=["total_depth_driller_tvd"], inplace=True)

    alias_filter = "&wellbore_name_set=multilateral%20well%20name"
    alias_mlt_name_df = extract_data(smda_address.session, alias_endpoint, alias_filter)

    mlt_names = []
    mlt_short_names = []

    if not metadata.empty:
        metadata = metadata[columns]

        for indx, row in metadata.iterrows():
            mlt_name = ""
            mlt_short_name = ""

            wellbore_name = row["unique_wellbore_identifier"]

            selected_alias_meta = alias_mlt_name_df[
                alias_mlt_name_df["unique_wellbore_identifier"] == wellbore_name
            ]

            if not selected_alias_meta.empty:
                mlt_name = selected_alias_meta["alias"].values[0]
                mlt_short_name = get_short_wellname(mlt_name)

            mlt_names.append(mlt_name)
            mlt_short_names.append(mlt_short_name)

    metadata["mlt_name"] = mlt_names
    metadata["mlt_short_name"] = mlt_short_names

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

    trajectories_df = extract_data(smda_address.session, endpoint, filter)

    endpoint = smda_address.api + "wellbore-survey-headers?"
    selection = "projected_coordinate_system"
    survey_df = extract_data(smda_address.session, endpoint, filter)

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

    metadata = extract_data(smda_address.session, endpoint, filter)

    if not metadata.empty:
        metadata = metadata[columns]

    endpoint = smda_address.api + "wellbore-plan-survey-headers?"
    filter = orig_filter + "&wellbore_status=planning"

    extra_metadata = extract_data(smda_address.session, endpoint, filter)
    extra_metadata = extra_metadata[["unique_wellbore_identifier", "design_name"]]

    all_metadata = metadata.loc[
        metadata["unique_wellbore_identifier"].isin(
            extra_metadata["unique_wellbore_identifier"].values
        )
    ]

    sorted_metadata = all_metadata.sort_values(by="unique_wellbore_identifier")
    extra_metadata.sort_values(by="unique_wellbore_identifier", inplace=True)

    combined_metadata = sorted_metadata.merge(
        extra_metadata[["unique_wellbore_identifier", "design_name"]],
        on="unique_wellbore_identifier",
        how="left",
    )

    return combined_metadata


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
        trajectory_df = extract_data(smda_address.session, endpoint, filter)
        trajectory_df = trajectory_df[columns]

        trajectories_df = [trajectory_df]

    else:
        frames = []

        for _index, row in metadata.iterrows():
            status = False
            filter = "&unique_wellbore_identifier=" + row["unique_wellbore_identifier"]
            try:
                trajectory_df = extract_data(smda_address.session, endpoint, filter)
                trajectory_df = trajectory_df[columns]
                status = True
            except:
                print(row["unique_wellbore_identifier"], "  - trajectory not found")
                status = False

            if status:
                frames.append(trajectory_df)

        trajectories_df = pd.concat(frames)

    return trajectories_df
