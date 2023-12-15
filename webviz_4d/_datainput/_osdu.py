import os
import pandas as pd
from typing import Optional

import requests
import numpy as np
from dotenv import load_dotenv

from osdu_api.clients.search.search_client import SearchClient
from osdu_api.clients.dataset.dataset_dms_client import DatasetDmsClient
from osdu_api.clients.base_client import BaseClient
from osdu_api.configuration.base_config_manager import BaseConfigManager
from osdu_api.clients.storage.schema_client import SchemaClient as StorageSchemaClient
from osdu_api.model.http_method import HttpMethod
from osdu_api.model.search.query_request import QueryRequest
from dataclasses import dataclass
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import BaseModel


import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def extract_osdu_metadata(osdu_service):
        names = []
        attributes = []
        times1 = []
        times2 = []
        datasets = []

        headers = [
            "data.name",
            "data.attribute",
            "data.time.t1",
            "data.time.t2",
            "dataset_id",
        ]
        attribute_horizons = osdu_service.get_attribute_horizons()
                
        for horizon in attribute_horizons:
            ow_name=horizon.ow_top_horizon[3:11]

            if ow_name == "IUTU+JS+":
                ow_name = "IUTU_JS"          
                
            monitor_date=horizon.monitor_date
            base_date=horizon.base_date
            seismic_content=horizon.seismic_content
            horizon_content=horizon.horizon_content

            names.append(ow_name)
            date_reformat = (
                    monitor_date[6:10] + "-" + monitor_date[3:5] + "-" + monitor_date[0:2]
                )
            times2.append(date_reformat)
            date_reformat = (
                    base_date[6:10] + "-" + base_date[3:5] + "-" + base_date[0:2]
                )
            times1.append(date_reformat)
            

            dataset_ids = osdu_service.get_dataset_ids(horizon)
            # print("DEBUG datasets")
            # print(dataset_ids)

            for dataset_id in dataset_ids:
                dataset_info = osdu_service.get_dataset_info(dataset_id)

                if dataset_info and dataset_info.source == "OpenWorks":
                    if "dTS" in dataset_info.name:
                        seismic_content = "dTS"

                    datasets.append(dataset_id)
                    attribute = seismic_content + "_" + horizon_content
                    attributes.append(attribute)
                    
        zipped_list = list(
            zip(
                names,
                attributes,
                times1,
                times2,
                datasets,
            )
        )

        metadata = pd.DataFrame(zipped_list, columns=headers)
        metadata.fillna(value=np.nan, inplace=True)

        metadata["fmu_id.realization"] = "---"
        metadata["fmu_id.iteration"] = "---"
        metadata["map_type"] = "observed"
        metadata["statistics"] = ""

        return metadata


class Config(BaseSettings, BaseConfigManager):
    INSTANCE: str = "https://npequinor.energy.azure.com"
    DATA_PARTITION_ID: str = "npequinor-dev"
    LEGAL_TAG: str = "npequinor-dev-equinor-private-default"
    TENANT_ID: str = "3aa4a235-b6e2-48d5-9195-7fcf05b459b0"
    ACCESS_TOKEN: str = f"{INSTANCE}/dummy"  # TODO: Only temp solution
    # Below is required naming for osdu-api library
    STORAGE_URL: str = f"{INSTANCE}/api/storage/v2"
    SEARCH_URL: str = f"{INSTANCE}/api/search/v2"
    LEGAL_URL: str = f"{INSTANCE}/api/legal/v1"
    FILE_DMS_URL: str = f"{INSTANCE}/api/file/v2"
    DATASET_URL: str = f"{INSTANCE}/api/dataset/v1"
    ENTITLEMENTS_URL: str = f"{INSTANCE}/api/entitlements/v1"
    SCHEMA_URL: str = f"{INSTANCE}/api/schema-service/v1"
    INGESTION_WORKFLOW_URL: str = f"{INSTANCE}/api/workflow/v1"

    if ACCESS_TOKEN == f"{INSTANCE}/dummy":
        load_dotenv(dotenv_path=os.path.join(os.path.expanduser("~"),".env"), override=True)
        ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")

    def get(self, section: str, option: str, default: Optional[str] = None) -> str:
        return self.model_dump().get(option.upper(), default)

    def getint(self, section: str, option: str, default: Optional[int] = None) -> int:
        return self.model_dump().get(option.upper(), default)

    def getfloat(
            self, section: str, option: str, default: Optional[float] = None
    ) -> int:
        return self.model_dump().get(option.upper(), default)

    def getbool(
            self, section: str, option: str, default: Optional[bool] = None
    ) -> bool:
        return self.model_dump().get(option.upper(), default)

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


class Schema(BaseModel):
    id: str
    kind: str
    source: str
    acl_viewers: list[str]
    acl_owners: list[str]
    type: str
    version: int


class SeismicHorizon(Schema):
    name: str
    datasets: list[str]
    interpretation_name: str
    bin_grid_id: str
    role: str
    remark: str


class SeismicBinGrid(Schema):
    name: str


class GenericRepresentation(Schema):
    name: str
    ow_horizon_name: str
    ow_top_horizon: str
    ow_base_horizon: str
    monitor_date: str
    base_date: str
    seismic_content: str
    horizon_content: str
    datasets: list[str]


class Dataset(Schema):
    name: str


def parse_seismic_horizon(horizon: dict) -> SeismicHorizon:
    return SeismicHorizon(
        id=horizon.get("id"),
        kind=horizon.get("kind"),
        source=horizon.get("source"),
        acl_viewers=horizon.get("acl").get("viewers"),
        acl_owners=horizon.get("acl").get("owners"),
        type=horizon.get("type"),
        version=horizon.get("version"),
        name=horizon.get("data").get("Name"),
        datasets=horizon.get("data").get("Datasets"),
        interpretation_name=horizon.get("data").get("InterpretationName"),
        bin_grid_id=horizon.get("data").get("BinGridID"),
        role=horizon.get("data").get("Role"),
        remark=horizon.get("data").get("Remark"),
    )


@dataclass
class DefaultOsduService():
    def __init__(self, config=Config(), refresh_token: str = None):
        client_config = {
            "config_manager": config,
            "data_partition_id": config.DATA_PARTITION_ID,
        }
        self.access_token = config.ACCESS_TOKEN
        self.base_client = BaseClient(**client_config)
        self.search_client = SearchClient(**client_config)
        self.dataset_dms_client = DatasetDmsClient(**client_config)
        self.storage_schema_client = StorageSchemaClient(**client_config)

    def get_seismic_horizons(self, bin_grid_version: str = "") -> list[SeismicHorizon]:
        query = "" if bin_grid_version else ""
        query_request = QueryRequest(
            "osdu:wks:work-product-component--SeismicHorizon:1.2.0", query
        )
        result = self.search_client.query_records(query_request, bearer_token=self.access_token)

        return [
            parse_seismic_horizon(horizon=horizon)
            for horizon in result.json().get("results")
        ]

    def get_seismic_horizon(self, id: str) -> SeismicHorizon:
        query_request = QueryRequest(
            kind="osdu:wks:work-product-component--SeismicHorizon:1.2.0",
            query=f'id:"{id}"',
        )
        result = self.search_client.query_records(query_request, bearer_token=self.access_token)
        result.raise_for_status()

        return parse_seismic_horizon(result.json().get("results")[0])

    def get_bin_grids(self, version: str = "") -> list[SeismicBinGrid]:
        v = version if version else "*"
        query_request = QueryRequest(
            kind=f"osdu:wks:work-product-component--SeismicBinGrid:{v}", query=""
        )
        result = self.search_client.query_records(query_request=query_request, bearer_token=self.access_token)

        return [
            SeismicBinGrid(
                id=bin_grid.get("id"),
                kind=bin_grid.get("kind"),
                source=bin_grid.get("source"),
                acl_viewers=bin_grid.get("acl").get("viewers"),
                acl_owners=bin_grid.get("acl").get("owners"),
                type=bin_grid.get("type"),
                version=bin_grid.get("version"),
                name=bin_grid.get("data").get("Name"),
            )
            for bin_grid in result.json().get("results")
        ]

    def get_horizon_map(self, file_id: str) -> np.ndarray:
        # Some file id taken from metadata can sometimes end with ':', so remove it just in case
        if file_id.endswith(":"):
            file_id = file_id.rstrip(":")
        # Get the download url for the data
        expiry_time: str = (
            "2M"  # TODO: Find out best expiry time, for the time being it is 1 minute
        )
        #print(self.access_token)
        result = self.base_client.make_request(
            method=HttpMethod.GET,
            url=f'{self.base_client.config_manager.get("", "FILE_DMS_URL")}/files/{file_id}/downloadURL?expiryTime={expiry_time}',
            bearer_token=self.access_token
        )
        result.raise_for_status()
        # Extract download url
        signed_url = result.json().get("SignedUrl")

        try:
            file_result = requests.get(signed_url)
            file_result.raise_for_status()
            # data = np.loadtxt(io.StringIO(file_result.text))
            # # check the file is correct format
            # data[:, 4]
            #data = io.StringIO(file_result.text)
            return file_result
        
        except IndexError:
            raise Exception("The data format is not correct format (ijzyz)")
        except Exception as e:
            raise Exception(f"Something went wrong with horizon download, {e}")

    def get_bin_grid_versions(self) -> list[str]:
        # This does not exist in client class
        response = self.storage_schema_client.make_request(
            method=HttpMethod.GET,
            url=f"{self.storage_schema_client.storage_url}/query/kinds",
            bearer_token=self.access_token
        )
        return [
            schema.split(":")[-1]
            for schema in response.json().get("results", [])
            if "osdu:wks:work-product-component--SeismicBinGrid" in schema
        ]
    
    def get_attribute_horizons(self) -> list[GenericRepresentation]:
        query_request = QueryRequest(
            "osdu:wks:work-product-component--GenericRepresentation:1.0.0", ""
        )
        result = self.search_client.query_records(query_request, self.access_token)
        result.raise_for_status()

        return [
            GenericRepresentation(
                id=horizon.get("id"),
                kind=horizon.get("kind"),
                source=horizon.get("source"),
                acl_viewers=horizon.get("acl").get("viewers"),
                acl_owners=horizon.get("acl").get("owners"),
                type=horizon.get("type"),
                version=horizon.get("version"),
                name=horizon.get("data").get("Name"),
                datasets=horizon.get("data").get("Datasets"),
                ow_horizon_name=horizon.get("tags").get("OW Horizon name"),
                ow_top_horizon=horizon.get("tags").get("OW Top Horizon"),
                ow_base_horizon=horizon.get("tags").get("OW Base Horizon"),
                monitor_date=horizon.get("tags").get("Monitor reference date"),
                base_date=horizon.get("tags").get("Base reference date"),
                seismic_content=horizon.get("tags").get("Seismic content"),
                horizon_content=horizon.get("tags").get("Horizon content"),
            )
            for horizon in result.json().get("results")
        ]
    

    def get_dataset_info(self, id: str = "") -> Dataset:
        query = 'id:"' + id[:-1] + '"'
        query_request = QueryRequest(
            f"osdu:wks:dataset--File.Generic:1.0.0", query
        )
        result = self.search_client.query_records(query_request, self.access_token)
        result.raise_for_status()
        results = result.json().get("results")

        dataset = None

        if results:
            info = results[0]

            dataset = Dataset(
                id=info.get("id"),
                name=info.get("data").get("Name"),
                kind=info.get("kind"),
                source = info.get("data").get("Source"),
                acl_viewers=info.get("acl").get("viewers"),
                acl_owners=info.get("acl").get("owners"),
                type=info.get("type"),
                version=info.get("version"),
            )

        return dataset
    

    def get_dataset_ids(self, horizon) -> list[str ]:
        dataset_ids = horizon.datasets
        
        return dataset_ids
    

def main():
    osdu_service = DefaultOsduService()
    surface_metadata = extract_osdu_metadata(osdu_service)
    print(surface_metadata)

if __name__ == '__main__':
    main()