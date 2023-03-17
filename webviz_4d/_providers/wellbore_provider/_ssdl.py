from pandas import DataFrame, json_normalize

from webviz_4d._providers.wellbore_provider._omnia import extract_omnia_session


def ssdl_connect(omnia_path):
    return extract_omnia_session(omnia_path, "SSDL")


def extract_ssdl_data(session, endpoint, columns):
    df_selected = DataFrame()
    # print(endpoint)

    response = session.get(endpoint)

    if response.status_code == 200:
        results = response.json()
        df_ssdl = json_normalize(results)

        if not df_ssdl.empty:
            df_selected = df_ssdl[columns]
        else:
            df_selected = DataFrame()
            print("ERROR: No data returned from query:", endpoint)

    return df_selected


def extract_ssdl_wellbores(ssdl_address, filter):
    endpoint = ssdl_address.api + filter
    columns = ["unique_wellbore_identifier", "wellbore_uuid"]

    return extract_ssdl_data(ssdl_address.session, endpoint, columns)


def extract_default_model(ssdl_address, filter):
    endpoint = ssdl_address.api + filter
    columns = ["model_uuid", "model_identifier", "has_polygon", "default_flag"]

    models = extract_ssdl_data(ssdl_address.session, endpoint, columns)

    try:
        selected_model = models[
            (models["default_flag"] == True) & (models["has_polygon"] == 1)
        ]
    except:
        selected_model = None
        print("ERROR: Default model with polygons not found")

    return selected_model


def extract_faultlines(ssdl_address, filter):
    endpoint = ssdl_address.api + filter
    columns = ["SEG I.D.", "geometry", "coordinates"]

    return extract_ssdl_data(ssdl_address.session, endpoint, columns)


def extract_outlines(ssdl_address, filter):
    endpoint = ssdl_address.api + filter
    columns = ["meta.name", "meta.geometry", "coordinates"]

    return extract_ssdl_data(ssdl_address.session, endpoint, columns)


def extract_ssdl_completion(ssdl_address, filter):
    endpoint = ssdl_address.api + filter
    columns = [
        "wellbore_id",
        "well_id",
        "wellbore_uuid",
        "symbol_name",
        "description",
        "md_top",
        "md_bottom",
        "field_id",
    ]

    return extract_ssdl_data(ssdl_address.session, endpoint, columns)


def extract_ssdl_perforation(ssdl_address, filter):
    endpoint = ssdl_address.api + filter
    columns = [
        "wellbore_id",
        "well_id",
        "wellbore_uuid",
        "status",
        "md_top",
        "md_bottom",
        "field_id",
    ]

    return extract_ssdl_data(ssdl_address.session, endpoint, columns)
