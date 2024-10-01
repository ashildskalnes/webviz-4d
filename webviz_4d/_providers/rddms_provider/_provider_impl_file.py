import os
import pandas as pd
import json
import requests
import urllib.parse
from pprint import pprint
from dotenv import load_dotenv
from typing import Optional
from typing import List
import warnings

import webviz_4d._providers.osdu_provider.osdu_provider as osdu

warnings.filterwarnings("ignore")

SEISMIC_ATTRIBUTE_SCHEMA_FILE = "/private/ashska/dev_311/my_forks/webviz-4d/webviz_4d/_providers/rddms_provider/seismic_attribute_interpretation_042_schema.json"


class DefaultRddmsService:
    def __init__(self):
        home = os.path.expanduser("~")
        env_path = os.path.expanduser(os.path.join(home, ".osducli/.rddms"))
        load_dotenv(dotenv_path=env_path)

        try:
            bearer_key = os.environ.get("ACCESS_TOKEN")
        except:
            bearer_key = None
            print("ERROR: RDDMS bearer key not found")
            exit

        self.bearer_key = bearer_key

        self.rddms_host = (
            "https://interop-rddms.azure-api.net/connected/rest/Reservoir/v2/"
        )

        # Load the attribute schema and extract the relevant 4D metadata definitions
        with open(SEISMIC_ATTRIBUTE_SCHEMA_FILE) as schema_file:
            schema = json.load(schema_file)

        schema_data = schema.get("properties").get("data").get("allOf")[0]
        self.schema_properties = schema_data.get("properties")

    def get_dataspaces(self):
        # List the available data space(s)
        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {self.bearer_key}",
        }

        params = {
            "$skip": "0",
            "$top": "30",
        }

        dataspaces = []

        response = requests.get(
            self.rddms_host + "/dataspaces",
            params=params,
            headers=headers,
            verify=False,
        )

        if response.status_code == 200:
            results = response.json()

            for result in results:
                dataspace_name = urllib.parse.quote(result["path"], safe="")
                dataspaces.append(dataspace_name)

        return dataspaces

    def get_attribute_horizons(
        self,
        dataspace_name: str,
        field_name: Optional[str] = "",
    ) -> list[osdu.SeismicAttributeHorizon]:

        # Search for all attribute horizons for a given field name in a selecte dataspace
        seismic_attribute_horizons = []

        bearer_key = self.bearer_key
        rddms_host = self.rddms_host
        attribute_schema = self.schema_properties

        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {bearer_key}",
        }

        params = {
            "$skip": "0",
            "$top": "50",
        }

        # Search for attribute maps in the given dataspace
        response = requests.get(
            f"{rddms_host}/dataspaces/{dataspace_name}/resources",
            headers=headers,
            verify=False,
        )

        response_flat = pd.json_normalize(response.json()).to_dict(orient="records")
        # response_df = pd.DataFrame.from_dict(response_flat)
        # print(response_df)

        surface_response = requests.get(
            f"{rddms_host}/dataspaces/{dataspace_name}/resources/resqml20.obj_Grid2dRepresentation",
            params=params,
            headers=headers,
            verify=False,
        )

        params = {
            "$format": "json",
            "arrayMetadata": "false",
            "arrayValues": "false",
            "referencedContent": "true",
        }

        nsurfaces = len(surface_response.json())

        for index in range(nsurfaces):
            uuid = urllib.parse.quote(
                surface_response.json()[index]["uri"].split("(")[-1].replace(")", "")
            )

            response = requests.get(
                f"{rddms_host}/dataspaces/{dataspace_name}/resources/resqml20.obj_Grid2dRepresentation/{uuid}",
                params=params,
                headers=headers,
                verify=False,
            )

            response_flat = pd.json_normalize(response.json()).to_dict(
                orient="records"
            )[0]
            # pprint(response_flat)

            extra_metadata = response_flat.get("ExtraMetadata")
            # pprint(extra_metadata)

            metadata_dict = {}

            for item in extra_metadata:
                name = item.get("Name")
                value = item.get("Value")

                metadata_dict.update({name: value})

            # print("Extra metadata:")
            # pprint(metadata_dict)

            rddms_object = {}

            for key in attribute_schema.keys():
                metadata_value = metadata_dict.get(key)

                if metadata_value is None:
                    metadata_value = []

                elif metadata_value is not None and type(metadata_value) != list:
                    metadata_value = metadata_value.split("\n")

                    if type(metadata_value) == list:
                        metadata_list = []

                        for item in metadata_value:
                            meta = item.replace("- ", "")

                            if "None" in item:
                                metadata_list.append("")
                            elif item != "...":
                                metadata_list.append(meta)
                        metadata_value = metadata_list
                    else:
                        if "None" in metadata_value:
                            metadata_value = []

                elif metadata_value is not None and type(metadata_value) == list:
                    metadata_list = []

                    for item in metadata_value:
                        if "None" not in item:
                            metadata_value = item.replace("- ", "")
                        else:
                            metadata_value = ""

                        metadata_list.append(metadata_value)

                    metadata_value = metadata_list

                rddms_object.update({key: metadata_value})
                # print(key, ":", metadata_value)

            # pprint(rddms_object)

            response = requests.get(
                f"{rddms_host}/dataspaces/{dataspace_name}/resources/resqml20.obj_Grid2dRepresentation/{uuid}/arrays",
                params=params,
                headers=headers,
                verify=False,
            )
            # print(json.dumps(response.json(), indent=4))

            uuid_url = urllib.parse.quote(
                response.json()[0]["uid"]["pathInResource"], safe=""
            )

            if field_name != "" and rddms_object.get("FieldName")[0] == field_name:
                seismic_attribute_horizon = self.parse_seismic_attribute_horizon(
                    rddms_object, uuid, uuid_url
                )
                seismic_attribute_horizons.append(seismic_attribute_horizon)
            else:
                seismic_attribute_horizon = self.parse_seismic_attribute_horizon(
                    rddms_object, uuid_url
                )
                seismic_attribute_horizons.append(seismic_attribute_horizon)

        return seismic_attribute_horizons

    def parse_seismic_attribute_horizon(
        self,
        rddms_object: dict,
        uuid: str,
        uuid_url: str,
    ) -> osdu.SeismicAttributeHorizon:

        try:
            attribute_horizon = osdu.SeismicAttributeHorizon_042(
                id=uuid,
                kind="eqnr:cns-api:seismic-attribute-interpretation:0.4.2",
                Name=rddms_object.get("Name")[0],
                MetadataVersion=rddms_object.get("MetadataVersion")[0],
                FieldName=rddms_object.get("FieldName")[0],
                SeismicBinGridName=rddms_object.get("SeismicBinGridName")[0],
                StratigraphicColumn=rddms_object.get("StratigraphicColumn")[0],
                ApplicationName=rddms_object.get("ApplicationName")[0],
                MapTypeDimension=rddms_object.get("MapTypeDimension")[0],
                SeismicTraceDataSource=rddms_object.get("SeismicTraceDataSource")[0],
                SeismicTraceDataSourceNames=rddms_object.get(
                    "SeismicTraceDataSourceNames"
                ),
                SeismicTraceDataSourceIDs=rddms_object.get("SeismicTraceDataSourceIDs"),
                SeismicTraceDomain=rddms_object.get("SeismicTraceDomain")[0],
                SeismicTraceAttribute=rddms_object.get("SeismicTraceAttribute")[0],
                OsduSeismicTraceNames=rddms_object.get("OsduSeismicTraceNames"),
                SeismicDifferenceType=rddms_object.get("SeismicDifferenceType")[0],
                SeismicCoverage=rddms_object.get("SeismicCoverage")[0],
                AttributeWindowMode=rddms_object.get("AttributeWindowMode")[0],
                HorizonDataSource=rddms_object.get("HorizonDataSource")[0],
                HorizonSourceNames=rddms_object.get("HorizonSourceNames"),
                HorizonSourceIDs=rddms_object.get("HorizonSourceIDs"),
                HorizonOffsets=rddms_object.get("HorizonOffsets"),
                FixedWindowValues=rddms_object.get("FixedWindowValues"),
                StratigraphicZone=rddms_object.get("StratigraphicZone")[0],
                AttributeExtractionType=rddms_object.get("AttributeExtractionType")[0],
                AttributeDifferenceType=rddms_object.get("AttributeDifferenceType")[0],
                IrapBinaryID=uuid_url,
            )
        except:
            attribute_horizon = None

        return attribute_horizon
