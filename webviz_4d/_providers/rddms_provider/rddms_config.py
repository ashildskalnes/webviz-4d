import json


def get_rddms_config():
    RESERVOIR_DDMS_HOST = (
        "https://interop-rddms.azure-api.net/connected/rest/Reservoir/v2/"
    )
    SEISMIC_ATTRIBUTE_SCHEMA_FILE = "./webviz_4d/_providers/rddms_provider//seismic_attribute_interpretation_042_schema.json"

    # Load the attribute schema and extract the relevant 4D metadata definitions
    with open(SEISMIC_ATTRIBUTE_SCHEMA_FILE) as schema_file:
        schema = json.load(schema_file)

    schema_data = schema.get("properties").get("data").get("allOf")[0]

    rddms_config = {"rddms_host": RESERVOIR_DDMS_HOST, "attribute_schema": schema_data}

    return rddms_config
