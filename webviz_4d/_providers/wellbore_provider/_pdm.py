import pandas as pd
from pandas import DataFrame, json_normalize

from webviz_4d._providers.wellbore_provider._omnia import extract_omnia_session
import webviz_4d._providers.wellbore_provider.wellbore_provider as wb


def pdm_connect(omnia_path):
    return extract_omnia_session(omnia_path, "PDM")


def extract_pdm_data(session, endpoint, columns, filter):
    skip = 0
    top = 200000

    df_selected = None
    filter = filter.replace(" ", "%20").replace("/", "%2F")
    actual_endpoint = endpoint + "top=" + str(top) + "&" + filter
    # print(actual_endpoint)
    df_prod = DataFrame()
    frames = []

    response = session.get(actual_endpoint)

    if response.status_code == 200:
        results = response.json()
        df_prod = json_normalize(results)
        frames.append(df_prod)

        nrows = df_prod.shape[0]

        while nrows == top:
            skip = skip + top
            actual_endpoint = (
                endpoint + "skip=" + str(skip) + "&top=" + str(top) + "&" + filter
            )
            response = session.get(actual_endpoint)
            results = response.json()
            df_prod = json_normalize(results)
            # print(actual_endpoint)
            # print(df_prod)
            frames.append(df_prod)

            nrows = df_prod.shape[0]

    if not df_prod.empty:
        df_all = pd.concat(frames)
        df_selected = df_all[columns]
    else:
        df_selected = DataFrame()
        print("ERROR: No data returned from query:", endpoint)

    return df_selected


def extract_pdm_data_compact(session, endpoint, columns, filter):
    skip = 0
    top = 200000

    df_selected = None
    columns_string = ""

    for column in columns:
        columns_string = columns_string + column + ","

    columns_string = columns_string[:-1]

    filter = filter.replace(" ", "%20").replace("/", "%2F")
    actual_endpoint = (
        endpoint + "columns=" + columns_string + "&top=" + str(top) + "&" + filter
    )
    print(actual_endpoint)

    df_prod = DataFrame()
    frames = []

    response = session.get(actual_endpoint)

    if response.status_code == 200:
        results = response.json()
        df = json_normalize(results)[columns]

        for column in columns:
            df_prod[column] = df[column][0]

        frames.append(df_prod)

        nrows = df_prod.shape[0]

        while nrows == top:
            skip = skip + top
            actual_endpoint = (
                endpoint + "skip=" + str(skip) + "&top=" + str(top) + "&" + filter
            )
            response = session.get(actual_endpoint)
            results = response.json()
            df_prod = json_normalize(results)
            frames.append(df_prod)

            nrows = df_prod.shape[0]

    if not df_prod.empty:
        df_all = pd.concat(frames)
        df_selected = df_all[columns]
    else:
        df_selected = DataFrame()
        print("ERROR: No data returned from query:", actual_endpoint)

    return df_selected


def extract_production(pdm_address, filter, interval):
    valid_intervals = ["Day", "Month"]

    if interval not in valid_intervals:
        print("ERROR: Invalid production interval:", interval)
        print("  - valid intervals are", valid_intervals)

        return pd.DataFrame()

    endpoint = pdm_address.api + "/WellBoreProd" + interval + "?"
    columns = [
        "WB_UWBI",
        "WB_UUID",
        "GOV_WB_NAME",
        "WELL_UWI",
        "PROD_" + interval.upper(),
        "WB_OIL_VOL_SM3",
        "WB_GAS_VOL_SM3",
        "WB_WATER_VOL_M3",
        "GOV_FIELD_NAME",
    ]

    return extract_pdm_data(pdm_address.session, endpoint, columns, filter)


def extract_field_production(pdm_address, filter, interval):
    endpoint = pdm_address.api + "/WellBoreProd" + interval + "Compact?"
    prod_time = "PROD_" + interval.upper()
    columns = [
        "WB_UWBI",
        "WB_UUID",
        # "GOV_WB_NAME",
        # "WELL_UWI",
        prod_time,
        "WB_OIL_VOL_SM3",
        "WB_GAS_VOL_SM3",
        "WB_WATER_VOL_M3",
        # "GOV_FIELD_NAME",
    ]

    volumes = None
    dataframe = DataFrame()
    wb_uuids = []
    oil_volumes = []
    gas_volumes = []
    water_volumes = []
    volume_columns = [
        "WB_OIL_VOL_SM3",
        "WB_GAS_VOL_SM3",
        "WB_WATER_VOL_M3",
    ]
    oil_unit = "kSm3"
    gas_unit = "MSm3"
    water_unit = "kM3"
    prod_wellbores = []
    first_date = None
    second_date = None

    daily_df = extract_pdm_data_compact(pdm_address.session, endpoint, columns, filter)

    if daily_df.empty:
        print("ERROR: The daily production volumes are empty")
    else:
        pdm_well_names = daily_df["WB_UWBI"].unique()

        if interval == "Day":
            first_date = daily_df[prod_time].min()
            last_date = daily_df[prod_time].max()
            daily_df = daily_df[
                daily_df[prod_time] != last_date
            ]  # Do not include the last day

        for pdm_name in pdm_well_names:
            wellbore_volumes = daily_df[daily_df["WB_UWBI"] == pdm_name]

            if not wellbore_volumes.empty:
                volumes = wellbore_volumes[volume_columns].sum()
                oil_volumes.append(volumes["WB_OIL_VOL_SM3"] / 1000)
                gas_volumes.append(volumes["WB_GAS_VOL_SM3"] / 1000000)
                water_volumes.append(volumes["WB_WATER_VOL_M3"] / 1000)
                wb_uuid = wellbore_volumes["WB_UUID"].values[0]
                wb_uuids.append(wb_uuid)
                prod_wellbores.append(pdm_name)

        dataframe["WB_UWBI"] = prod_wellbores
        dataframe["WB_UUID"] = wb_uuids
        dataframe["OIL_VOL"] = oil_volumes
        dataframe["GAS_VOL"] = gas_volumes
        dataframe["WATER_VOL"] = water_volumes

        # first_date = first_date[:10]
        # second_date = last_date[:10]

    volumes = wb.ProductionVolumes(
        oil_unit,
        gas_unit,
        water_unit,
        first_date,
        second_date,
        dataframe,
    )

    return volumes


def extract_field_injection(pdm_address, filter, interval):
    endpoint = pdm_address.api + "/WellBoreInj" + interval + "Compact?"
    prod_time = "PROD_" + interval.upper()
    columns = [
        "WB_UWBI",
        "WB_UUID",
        # "GOV_WB_NAME",
        # "WELL_UWI",
        prod_time,
        "INJ_TYPE",
        "WB_INJ_VOL",
        # "GOV_FIELD_NAME",
    ]

    volumes = None
    dataframe = DataFrame()
    wb_uuids = []
    gas_volumes = []
    water_volumes = []
    co2_volumes = []
    gas_unit = "MSm3"
    water_unit = "kM3"
    co2_unit = "MSm3"
    prod_wellbores = []
    first_date = None
    second_date = None

    daily_df = extract_pdm_data_compact(pdm_address.session, endpoint, columns, filter)

    if daily_df.empty:
        print("ERROR: The daily injection volumes are empty")
    else:
        pdm_well_names = daily_df["WB_UWBI"].unique()

        first_date = daily_df[prod_time].min()
        last_date = daily_df[prod_time].max()
        daily_df = daily_df[
            daily_df[prod_time] != last_date
        ]  # Do not include the last day

        for pdm_name in pdm_well_names:
            wellbore_volumes = daily_df[daily_df["WB_UWBI"] == pdm_name]

            if not wellbore_volumes.empty:
                gi_vol = 0
                wi_vol = 0
                ci_vol = 0

                gi_volumes = wellbore_volumes[wellbore_volumes["INJ_TYPE"] == "GI"]

                wi_volumes = wellbore_volumes[wellbore_volumes["INJ_TYPE"] == "WI"]

                ci_volumes = wellbore_volumes[wellbore_volumes["INJ_TYPE"] == "CI"]

                if not gi_volumes.empty:
                    gi_vol = gi_volumes["WB_INJ_VOL"].sum() / 1000000

                if not wi_volumes.empty:
                    wi_vol = wi_volumes["WB_INJ_VOL"].sum() / 1000

                if not ci_volumes.empty:
                    ci_vol = ci_volumes["WB_INJ_VOL"].sum() / 1000000

                wb_uuid = wellbore_volumes["WB_UUID"].values[0]
                wb_uuids.append(wb_uuid)
                prod_wellbores.append(pdm_name)
                gas_volumes.append(gi_vol)
                water_volumes.append(wi_vol)
                co2_volumes.append(ci_vol)

        dataframe["WB_UWBI"] = prod_wellbores
        dataframe["WB_UUID"] = wb_uuids
        dataframe["GI_VOL"] = gas_volumes
        dataframe["WI_VOL"] = water_volumes
        dataframe["CI_VOL"] = co2_volumes

        first_date = first_date[:10]
        second_date = last_date[:10]

    volumes = wb.InjectionVolumes(
        gas_unit,
        water_unit,
        co2_unit,
        first_date,
        second_date,
        dataframe,
    )

    return volumes


def extract_injection(pdm_address, filter):
    endpoint = pdm_address.api + "/WellBoreInjDay?"
    columns = [
        "WB_UWBI",
        "WB_UUID",
        "GOV_WB_NAME",
        "WELL_UWI",
        "PROD_DAY",
        "INJ_TYPE",
        "WB_INJ_VOL",
        "GOV_FIELD_NAME",
    ]

    return extract_pdm_data(pdm_address.session, endpoint, columns, filter)


def extract_pdm_filter(
    field_name: str = None,
    wellbore_names: list = [],
    start_date: str = None,
    end_date: str = None,
    interval: str = None,
    field_uuid: str = None,
):
    filter = ""

    if wellbore_names:
        filter = filter + "WB_UWBI=" + wellbore_names[0]
        for i in range(1, len(wellbore_names)):
            filter = filter + "," + wellbore_names[i]
    elif field_uuid:
        filter = "FIELD_UUID=" + field_uuid
    else:
        filter = "GOV_FIELD_NAME=" + field_name

    if start_date:
        filter = filter + "&START_PROD_" + interval.upper() + "=" + start_date

    if end_date:
        filter = filter + "&END_PROD_" + interval.upper() + "=" + end_date

    return filter


def extract_pdm_wellbores(pdm_address, filter):
    endpoint = pdm_address.api + "/WellBoreMaster?"
    columns = [
        "WB_UWBI",
        "WB_START_DATE",
        "WB_END_DATE",
        "PURPOSE",
        "CONTENT",
        "FIELD_UUID",
    ]

    return extract_pdm_data(pdm_address.session, endpoint, columns, filter)


def extract_cumulative_volumes(
    wellbore_volumes, volume_col, first_date, second_date, interval
):
    column_header = "PROD_" + interval.upper()
    cum_vol = wellbore_volumes.loc[
        (wellbore_volumes[column_header] < second_date)
        & (wellbore_volumes[column_header] >= first_date),
        [volume_col],
    ].sum()

    return cum_vol[0]
