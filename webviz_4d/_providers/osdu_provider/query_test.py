import numpy as np
import pandas as pd
from datetime import datetime
import requests

from osdu_api.clients.search.search_client import SearchClient
from osdu_api.clients.dataset.dataset_dms_client import DatasetDmsClient
from osdu_api.clients.base_client import BaseClient
from osdu_api.clients.storage.schema_client import SchemaClient as StorageSchemaClient
from osdu_api.model.search.query_request import QueryRequest
from osdu_api.model.http_method import HttpMethod  # type: ignore
from osdu_api.model.search.query_request import QueryRequest  # type: ignore

from webviz_4d._providers.osdu_provider.osdu_config import Config
import webviz_4d._providers.osdu_provider.osdu_provider as osdu

from typing import Optional
from typing import List

from pprint import pprint


class DefaultOsduService:
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

    def get_osdu_metadata(self, kind, string):
        # Search for an osdu object 
        osdu_object = None

        translation = {
            "Any": "*:*:*:*",
            "id": "*:*:*:*",
            "Generic Representations": "osdu:wks:work-product-component--GenericRepresentation:*"
        }

        if kind in translation.keys():
            translated_kind = translation.get(kind)
        else:
            translated_kind = None
            print("ERROR: kind not supported:",kind)
        
        if translated_kind:
            query =  kind + '*:\"' + string + '*\"'
            query_request = QueryRequest(kind=translated_kind, query=query)
            result = self.search_client.query_records(query_request, self.access_token)
            result.raise_for_status()

            osdu_object = None

            if result.status_code == 200:
                osdu_objects = result.json().get("results")

                if len(osdu_objects) > 0:
                    osdu_object = osdu_objects[0]

        return osdu_object
    
    
def main():
    osdu_service = DefaultOsduService()

    id = "npequinor-dev:work-product-component--GenericRepresentation:8b2223f115374fac9f1a5bb545d564ab"

    osdu_metadata = osdu_service.get_osdu_metadata("id",id)
    pprint(osdu_metadata)

    string = "Datafun"
    osdu_metadata = osdu_service.get_osdu_metadata("Generic Representations",string)
    pprint(osdu_metadata)




if __name__ == "__main__":
    main()