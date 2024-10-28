import os
import numpy as np
import pandas as pd
import json
import requests
import urllib.parse
from pprint import pprint
from dotenv import load_dotenv
from typing import Optional
from typing import List
import xtgeo
import warnings

import webviz_4d._providers.osdu_provider.osdu_provider as osdu

warnings.filterwarnings("ignore")

SEISMIC_ATTRIBUTE_SCHEMA_FILE = "/private/ashska/dev_311/my_forks/webviz-4d/webviz_4d/_providers/rddms_provider/seismic_attribute_interpretation_042_schema.json"


class DefaultRddmsService:
    def __init__(self):
        env_path = os.path.join(os.path.expanduser("~"), ".env")
        load_dotenv(dotenv_path=env_path, override=True)

        try:
            bearer_key = os.environ.get("ACCESS_TOKEN")
        except:
            bearer_key = None
            print("ERROR: RDDMS bearer key not found")
            exit

        self.headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {bearer_key}",
        }

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

        params = {
            "$skip": "0",
            "$top": "50",
        }

        dataspaces = []

        response = requests.get(
            self.rddms_host + "/dataspaces",
            params=params,
            headers=self.headers,
            verify=False,
        )

        if response.status_code == 200:
            results = response.json()

            for result in results:
                dataspace_name = urllib.parse.quote(result["path"], safe="")
                dataspaces.append(dataspace_name)

        return dataspaces

    def get_rddms_map(
        self, dataspace_name, horizon_name, uuid, uuid_url
    ) -> xtgeo.surface:
        dataspace = dataspace_name.replace("/", "%2F")

        grid_geometry = self.get_grid2d_metadata(dataspace, uuid)
        z_values = self.get_grid2_values(dataspace, uuid, uuid_url)

        ncol = grid_geometry.get("nrow")
        nrow = grid_geometry.get("ncol")
        xinc = grid_geometry.get("xinc")
        yinc = grid_geometry.get("yinc")
        xori = grid_geometry.get("origin")[0]
        yori = grid_geometry.get("origin")[1]
        rotation = grid_geometry.get("rotation")

        if "swat" in horizon_name:
            rotation = rotation + 220

        rddms_surface = xtgeo.RegularSurface(
            ncol=ncol,
            nrow=nrow,
            xinc=xinc,
            yinc=yinc,
            yflip=-1,
            xori=xori,
            yori=yori,
            values=z_values,
            name=horizon_name,
            rotation=rotation,
        )

        return rddms_surface

    def get_grid2ds(self, dataspace, object_type):
        params = {
            "$skip": "0",
            "$top": "100",
        }

        grid2s = []

        response = requests.get(
            f"{self.rddms_host}/dataspaces/{dataspace}/resources/{object_type}",
            params=params,
            headers=self.headers,
            verify=False,
        )

        ngrid2s = len(response.json())

        if ngrid2s == 0:
            print("ERROR: No objects found:", object_type)
            return grid2s

        for index in range(ngrid2s):
            uuid = urllib.parse.quote(
                response.json()[index]["uri"].split("(")[-1].replace(")", "")
            )
            name = urllib.parse.quote(response.json()[index]["name"])

            grid2s.append({"name": name, "uuid": uuid})

        return grid2s

    def get_extra_metadata(self, dataspace, uuid, field_name):
        metadata = {}

        params = {
            "$skip": "0",
            "$top": "100",
        }

        response = requests.get(
            f"{self.rddms_host}/dataspaces/{dataspace}/resources/resqml20.obj_Grid2dRepresentation/{uuid}",
            params=params,
            headers=self.headers,
            verify=False,
        )

        response_flat = pd.json_normalize(response.json()).to_dict(orient="records")[0]

        extra_metadata = response_flat.get("ExtraMetadata")

        if extra_metadata is not None:
            metadata_dict = {}

            for item in extra_metadata:
                name = item.get("Name")
                value = item.get("Value")

                metadata_dict.update({name: value})
                self.name = name

            attribute_schema = self.schema_properties

            for key in attribute_schema.keys():
                tag = key

                if tag not in metadata_dict:
                    tag = "osdu/tags/" + key

                if tag not in metadata_dict:
                    tag = "osdu/" + key

                metadata_value = metadata_dict.get(tag)

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

                metadata_dict.update({key: metadata_value})

            if field_name != "":
                metadata_field_name = metadata_dict.get("FieldName", "")[0]

                if metadata_field_name != "" and metadata_field_name == field_name:
                    metadata = metadata_dict
                else:
                    metadata = {}
            else:
                metadata = metadata_dict

        return metadata

    def get_grid2d_metadata(self, dataspace_name, uuid):
        geometry_dict = {}
        params = {
            "$format": "json",
            "arrayMetadata": "false",
            "arrayValues": "false",
            "referencedContent": "true",
        }

        dataspace = dataspace_name.replace("/", "%2F")

        response = requests.get(
            f"{self.rddms_host}/dataspaces/{dataspace}/resources/resqml20.obj_Grid2dRepresentation/{uuid}",
            params=params,
            headers=self.headers,
            verify=False,
        )

        grid2d = response.json()

        if len(grid2d) > 0:

            XOffset = (
                grid2d[0]
                .get("Grid2dPatch")
                .get("Geometry")
                .get("LocalCrs")
                .get("_data")
                .get("XOffset")
            )
            YOffset = (
                grid2d[0]
                .get("Grid2dPatch")
                .get("Geometry")
                .get("LocalCrs")
                .get("_data")
                .get("YOffset")
            )

            origin_1 = grid2d[0]["Grid2dPatch"]["Geometry"]["Points"][
                "SupportingGeometry"
            ]["Origin"]["Coordinate1"]
            origin_2 = grid2d[0]["Grid2dPatch"]["Geometry"]["Points"][
                "SupportingGeometry"
            ]["Origin"]["Coordinate2"]

            offset = grid2d[0]["Grid2dPatch"]["Geometry"]["Points"][
                "SupportingGeometry"
            ]["Offset"]

            # print(XOffset + origin_1, YOffset + origin_2)

            surf_ori = [XOffset + origin_1, YOffset + origin_2]

            increments = []
            counts = []

            for item in offset:
                inc = item["Spacing"]["Value"]
                increments.append(inc)

                count = item["Spacing"]["Count"]
                counts.append(count)

            nrow = counts[0]
            ncol = counts[1]
            dx = increments[1]
            dy = increments[0]

            rotation = 90
            yflip = -1

            geometry_dict = {
                "nrow": nrow,
                "ncol": ncol,
                "origin": surf_ori,
                "xinc": dx,
                "yinc": dy,
                "rotation": rotation,
                "yflip": yflip,
            }

            response = requests.get(
                f"{self.rddms_host}/dataspaces/{dataspace}/resources/resqml20.obj_Grid2dRepresentation/{uuid}/arrays",
                params=params,
                headers=self.headers,
                verify=False,
            )

            uuid_url = urllib.parse.quote(
                response.json()[0]["uid"]["pathInResource"], safe=""
            )

            geometry_dict.update({"uuid_url": uuid_url})

        return geometry_dict

    def get_attribute_horizons(
        self,
        dataspace_name: str,
        field_name: Optional[str] = "",
        object_type: Optional[str] = "resqml20.obj_Grid2dRepresentation",
    ) -> list[osdu.SeismicAttributeHorizon_042]:

        seismic_attribute_horizons = []
        dataspace = dataspace_name.replace("/", "%2F")

        # Search for all attribute horizons for a given field name in a selected dataspace

        grid2d_objects = self.get_grid2ds(dataspace, object_type)
        print(" - ", object_type, ":", len(grid2d_objects))

        for grid2_object in grid2d_objects:
            uuid = grid2_object.get("uuid")
            rddms_horizon = self.get_extra_metadata(dataspace, uuid, field_name)
            uuid_url = self.get_grid2_url(dataspace, uuid)

            if rddms_horizon and uuid_url:
                attribute_horizon = self.parse_seismic_attribute_horizon(
                    rddms_horizon, uuid, uuid_url
                )

            if attribute_horizon:
                seismic_attribute_horizons.append(attribute_horizon)

        return seismic_attribute_horizons

    def get_grid2_values(self, dataspace_name, uuid, uuid_url):
        dataspace = dataspace_name.replace("/", "%2F")
        z_values = []

        params = {
            "$format": "json",
            "arrayMetadata": "false",
            "arrayValues": "false",
            "referencedContent": "true",
        }

        response = requests.get(
            f"{self.rddms_host}/dataspaces/{dataspace}/resources/resqml20.obj_Grid2dRepresentation/{uuid}/arrays/{uuid_url}",
            params=params,
            headers=self.headers,
            verify=False,
        )

        z_values = np.array(response.json()["data"]["data"], dtype=np.float32)

        return z_values

    def get_grid2_url(self, dataspace, uuid):
        uuid_url = None

        params = {
            "$format": "json",
            "arrayMetadata": "false",
            "arrayValues": "false",
            "referencedContent": "true",
        }

        response = requests.get(
            f"{self.rddms_host}/dataspaces/{dataspace}/resources/resqml20.obj_Grid2dRepresentation/{uuid}/arrays",
            params=params,
            headers=self.headers,
            verify=False,
        )

        uuid_url = urllib.parse.quote(
            response.json()[0]["uid"]["pathInResource"], safe=""
        )

        return uuid_url

    def parse_seismic_attribute_horizon(
        self, rddms_object: dict, uuid: str, uuid_url: str
    ) -> osdu.SeismicAttributeHorizon_042:

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
                DatasetIDs=[uuid_url],
            )
        except:
            attribute_horizon = None

        return attribute_horizon
