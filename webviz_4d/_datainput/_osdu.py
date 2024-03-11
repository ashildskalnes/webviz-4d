import os
import pandas as pd
from typing import Optional
from datetime import datetime
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

def find_all_substrings(txt,sub):
    positions = []
    start_index=0

    for i in range(len(txt)):
        j = txt.find(sub,start_index)
        if(j!=-1):
            start_index = j+1
            positions.append(j)
    
    return positions

def extract_osdu_metadata(osdu_service, meta_version):
        surface_names = []
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
        print("Searching for 4D attribute maps in OSDU (version)", meta_version)
        attribute_horizons = osdu_service.get_all_attribute_horizons(meta_version)
                
        for attribute_object in attribute_horizons:
            horizon_names = []
            kind = attribute_object.get("kind")
            id = attribute_object.get("id")
            name = attribute_object.get("data").get("Name")
            tags =  attribute_object.get("tags")
            metadata_version = tags.get("MetadataVersion")
            seismic_content = tags.get("AttributeMap.SeismicTraceContent")
            attribute_type = tags.get("AttributeMap.AttributeType")
            window_mode = tags.get("CalculationWindow.WindowMode")

            irap_binary_dataset = tags.get("file.irap")
            datasets.append(irap_binary_dataset)

            if window_mode == "AroundHorizon":
                    seismic_horizon = tags.get("CalculationWindow.HorizonName")
                    seismic_horizon = seismic_horizon.replace("+","_")
                    horizon_names.append(seismic_horizon)
            elif window_mode == "BetweenHorizons":
                seismic_horizon = tags.get("CalculationWindow.TopHorizonName")
                seismic_horizon = seismic_horizon.replace("+","_")
                horizon_names.append(seismic_horizon)

                seismic_horizon = tags.get("CalculationWindow.BaseHorizonName")
                seismic_horizon = seismic_horizon.replace("+","_")
                horizon_names.append(seismic_horizon)

            surface_names.append(horizon_names[0])
            attribute = seismic_content + "_" + attribute_type
            attributes.append(attribute)   

            base_seismic_name = tags.get("SeismicProcessingTraces.BaseSeismicTraces")
            monitor_seismic_name = tags.get("SeismicProcessingTraces.MonitorSeismicTraces")

            seismic_names = [base_seismic_name, monitor_seismic_name]
            seismic_objects = osdu_service.get_seismic_cubes(seismic_names)

            dates = []

            for seismic_object in seismic_objects:
                data = seismic_object.get("data")
                seismic_name = data.get("Name")
                #print("Seismic cube:", seismic_name)
                
                processing_project_id = data.get("ProcessingProjectID")
                #print("- Processing project id:", processing_project_id)

                acquisition_id = data.get("PrincipalAcquisitionProjectID")
                
                positions = find_all_substrings(acquisition_id, ":")

                if len(positions) == 3:
                    pos = positions [-1]
                    acquisition_id = acquisition_id[:pos]

                #print("- Acquisition id:", acquisition_id)

                acquisition_meta = osdu_service.get_osdu_metadata(acquisition_id)
                data = acquisition_meta.get("data")
                                    
                begin_date = data.get("ProjectBeginDate")
                end_date = data.get("ProjectEndDate")
                begin_date = datetime.strptime(begin_date[:10],"%Y-%m-%d")
                end_date = datetime.strptime(end_date[:10],"%Y-%m-%d")
                reference_date = begin_date + (end_date - begin_date)/2
                reference_date = datetime.strftime(reference_date,"%Y-%m-%d")

                dates.append(reference_date)
                
            base_date = dates [0]
            times1.append(base_date)

            monitor_date = dates [1]
            times2.append(monitor_date)

            print("Map name:", name)
            print("  Seismic horizon:",seismic_names[0])
            print("  Seismic content:", seismic_content)
            print("  Attribute type", attribute_type)
            print("  Base survey date:", base_date)
            print("  Monitor survey date:", monitor_date)
            
                    
        zipped_list = list(
            zip(
                surface_names,
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
    #source: str
    #acl_viewers: list[str]
    #acl_owners: list[str]
    #type: str
    #version: int


class SeismicHorizon(Schema):
    name: str
    datasets: list[str]
    interpretation_name: str
    bin_grid_id: str
    seismic_trace_id: str
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
    base_cube_name: str
    monitor_cube_name: str


class Dataset(Schema):
    id: str
    name: str
    kind: str
    source: str


class SeismicAcquisition(Schema):
    id: str
    name: str
    kind: str
    begin_date: str
    end_date: str    


class SeismicProcessing(Schema):
    id: str
    kind: str
    project_name: str
    acquisition_survey_id: str
    

class SeismicCube(Schema):
    id: str
    kind: str
    name: str
    source: str
    domain: str
    inline_min: float
    inline_max: float
    inline_min: float
    xline_min: float
    xline_max: float
    sample_interval: int
    sample_count: int
    processing_project_id: str


def parse_seismic_horizon(horizon: dict) -> SeismicHorizon:   
    return SeismicHorizon(
        id=horizon.get("id",""),
        kind=horizon.get("kind",""),
        source=horizon.get("source",""),
        acl_viewers=horizon.get("acl").get("viewers"),
        acl_owners=horizon.get("acl").get("owners"),
        type=horizon.get("type",""),
        version=horizon.get("version",""),
        name=horizon.get("data").get("Name",""),
        datasets=horizon.get("data").get("Datasets"),
        interpretation_name=horizon.get("data").get("InterpretationName",""),
        bin_grid_id=horizon.get("data").get("BinGridID",""),
        seismic_trace_id=horizon.get("data").get("SeismicTraceDataID",""),
        remark=horizon.get("data").get("Remark",""),
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


    def get_seismic_acquisitions(self, selected_acquisitions) -> list[SeismicAcquisition]:
        results = []

        for acquisition in selected_acquisitions:
            query = f"data.ProjectName:\"{acquisition}\""

            query_request = QueryRequest(
                "osdu:wks:master-data--SeismicAcquisitionSurvey:1.2.0", query=query, limit=1000
            )
            result = self.search_client.query_records(query_request, self.access_token)
            result.raise_for_status()

            if result.status_code == 200:
                osdu_object = result.json().get("results") [0]
                seismic_acquisition = SeismicAcquisition(
                    id=osdu_object.get("id"),
                    kind=osdu_object.get("kind"),
                    version=osdu_object.get("version"),
                    name=osdu_object.get("data").get("ProjectName"),
                    begin_date=osdu_object.get("data").get("ProjectBeginDate"),
                    end_date=osdu_object.get("data").get("ProjectEndDate")
                )
                results.append(seismic_acquisition)

        return results

    
    def get_seismic_processings(self, selected_processings) -> list[SeismicProcessing]:
        results = []

        for processing in selected_processings:
            status = False
            query = f"data.ProjectName:\"{processing}\""
            query_request = QueryRequest(
                "osdu:wks:master-data--SeismicProcessingProject:*", query=query, limit=1000
            )
            result = self.search_client.query_records(query_request, self.access_token)
            result.raise_for_status()

            if result.status_code == 200:
                osdu_objects = result.json().get("results")

                for osdu_object in osdu_objects:
                    project_name = osdu_object.get("data").get("ProjectName")
                    acquisition_surveys = osdu_object.get("data").get("SeismicAcquisitionSurveys")

                    if acquisition_surveys:
                        acquisition_survey_id = acquisition_surveys[0]
                    else:
                        print("WARNING: SeismicProcessingProject:", project_name, "acquisition_survey_id not found")
                        acquisition_survey_id = ""

                    if project_name == processing:
                        seismic_processing = SeismicProcessing(
                            id=osdu_object.get("id"),
                            kind=osdu_object.get("kind"),
                            project_name=osdu_object.get("data").get("ProjectName"),
                            acquisition_survey_id=acquisition_survey_id
                        )

                        results.append(seismic_processing)
                        status = True

            if not status:
                print("WARNING: Processing project not found: ", processing)

        return results
    

    def get_seismic_processing_metadata(self, id) -> SeismicProcessing:
        query = f"id:\"{id}\""
        query_request = QueryRequest("osdu:wks:master-data--SeismicProcessingProject:1.2.0", query=query)
        result = self.search_client.query_records(query_request, self.access_token)
        result.raise_for_status()

        if result.status_code == 200:
            osdu_object = result.json().get("results") [0]
            project_id = osdu_object.get("id")

            if project_id == id:
                id=osdu_object.get("id")
                survey = osdu_object.get("data").get("SeismicAcquisitionSurveys")[0]
                
                seismic_processing = SeismicProcessing(
                    id=id,
                    kind=osdu_object.get("kind"),
                    project_name=osdu_object.get("data").get("ProjectName"),
                    acquisition_survey_id=survey
                )
            else:
                print("ERROR: Processing project not found: ", id)
                seismic_processing = None

        return seismic_processing
    

    def get_seismic_acquisition_metadata(self, id) -> SeismicAcquisition:
        query = f"id:\"{id}\""
        query_request = QueryRequest("osdu:wks:master-data--SeismicAcquisitionSurvey:1.2.0", query=query)
        result = self.search_client.query_records(query_request, self.access_token)
        result.raise_for_status()

        if result.status_code == 200:
            osdu_object = result.json().get("results") [0]
            project_id = osdu_object.get("id")

            if project_id == id:
                seismic_acquisition = SeismicAcquisition(
                    id=osdu_object.get("id"),
                    kind=osdu_object.get("kind"),
                    name=osdu_object.get("data").get("ProjectName"),
                    begin_date=osdu_object.get("data").get("ProjectBeginDate"),
                    end_date=osdu_object.get("data").get("ProjectEndDate")

                )
            else:
                print("ERROR: Seismic acquisition not found: ", id)
                seismic_acquisition = None

        return seismic_acquisition
    

    def get_seismic_cubes(self, selected_cubes):
        seismic_objects = []

        for cube in selected_cubes:
            status = False
            query = f"data.Name:\"{cube}\""
            query_request = QueryRequest(
                "osdu:wks:work-product-component--SeismicTraceData:1.3.0", query=query)
            result = self.search_client.query_records(query_request, self.access_token)
            result.raise_for_status()

            if result.status_code == 200:
                osdu_objects = result.json().get("results")
                seismic_objects.append(osdu_objects [0])


            #     for osdu_object in osdu_objects:
            #         name = osdu_object.get("data").get("Name")
            #         domain_type = osdu_object.get("data").get("SeismicDomainTypeID")
            #         processing_project=osdu_object.get("data").get("ProcessingProjectID")

            #         if name == cube:
            #             if "Depth" in domain_type:
            #                 domain = "Depth"
            #             else:
            #                 domain = "Time"  

            #             seismic_cube= SeismicCube(
            #                 id=osdu_object.get("id"),
            #                 kind=osdu_object.get("kind"),
            #                 name=name,
            #                 source="Not available", 
            #                 domain = domain,
            #                 inline_min=osdu_object.get("data").get("InlineMin"),
            #                 inline_max=osdu_object.get("data").get("InlineMax"),
            #                 xline_min=osdu_object.get("data").get("CrosslineMin"),    
            #                 xline_max=osdu_object.get("data").get("CrosslineMax"), 
            #                 sample_interval=osdu_object.get("data").get("SampleInterval"), 
            #                 sample_count=osdu_object.get("data").get("SampleCount"),
            #                 processing_project_id=processing_project,
            #             )

            #             results.append(seismic_cube)
            #             status = True

            # if not status:
            #     print("WARNING: Seismic cube not found: ", cube)

        return seismic_objects
        

    def get_seismic_horizons(self, selected_names):
        query = "" 
        seismic_horizon_objects = []

        if len(selected_names) > 0:
            if "" in selected_names:
                selected_names.remove("")

            for selected_name in selected_names:
                query = f"data.Name:\"{selected_name}\""
                query_request = QueryRequest(
                    "osdu:wks:work-product-component--SeismicHorizon:*", query=query,
                )
                result = self.search_client.query_records(query_request,  bearer_token=self.access_token)
                result_objects = result.json().get("results")
                seismic_horizon_objects.append(result_objects [0])

                # for result_object in result_objects:
                #     data = result_object.get("data")

                #     if data.get("Name") == selected_name:                       
                #         seismic_horizon = parse_seismic_horizon(horizon=result_object)
                #         seismic_horizons.append(seismic_horizon)
        else:
            query_request = QueryRequest(
                    "osdu:wks:work-product-component--SeismicHorizon:*", query
            )
            result = self.search_client.query_records(query_request, bearer_token=self.access_token)
            result_objects = result.json().get("results")
            seismic_horizon_objects.append(result_objects)

            # for result_object in result_objects:
            #     data = result_object.get("data")
            #     if data.get("interpretationName") is None:
            #         data ["interpretationName"] = "-"
            #         data ["Role" ] = "-"
            #         data ["Remark"] ="-"
            #         result_object ["data"] = data
                    
            #     seismic_horizon = parse_seismic_horizon(horizon=result_object)
            #     seismic_horizons.append(seismic_horizon)

        return seismic_horizon_objects


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
    
    def get_all_attribute_horizons(self, meta_version) -> list[GenericRepresentation]:
        # Search for all objects of type GenericRepresentation 
        # If meta_version is not None, select only maps with the given meta_version
        query=""

        # if meta_version:
        #     query = f"data.tags.MetadataVersion:\"{meta_version}\""

        query_request = QueryRequest(
            "osdu:wks:work-product-component--GenericRepresentation:*", query, limit=100
        )
        result = self.search_client.query_records(query_request, self.access_token)
        result.raise_for_status()

        horizon_objects = []

        if result.status_code == 200:
            osdu_objects = result.json().get("results")

            if meta_version:
                for osdu_object in osdu_objects: 
                    tags =  osdu_object.get("tags")
                    metadata_version = tags.get("MetadataVersion")

                    if metadata_version and metadata_version == meta_version:
                        name = osdu_object.get("data").get("Name")
                        source = tags.get("Source.Source")

                        if source == "OpenWorks":
                            ow_name = tags.get("Source.*_Horizon")

                            if name == ow_name:
                                horizon_objects.append(osdu_object)
            else:
                horizon_objects = osdu_objects

        return horizon_objects
    
    
    def get_osdu_metadata(self, id):
        # Search for an osdu object with a selected id
        query = f"id:\"{id}\""
        query_request = QueryRequest(
            "*:*:*:*", query=query
        )
        result = self.search_client.query_records(query_request, self.access_token)
        result.raise_for_status()

        if result.status_code == 200:
            osdu_objects = result.json().get("results")
            osdu_object = osdu_objects[0]

        return osdu_object
    
    
    def get_attribute_horizons(self) -> list[GenericRepresentation]:
        js_cubes = {
            "19au": "EQ19231DZC23A-KPSDM-RAW-FULL-0535-TIME",
            "20au": "EQ20231DZC23A-KPSDM-RAW-FULL-0535-TIME",
            "21sp": "EQ21200DZC23A-KPSDM-RAW-FULL-0535-TIME",
            "22sp": "EQ22200DZC23A-KPSDM-RAW-FULL-0535-TIME",
            "22au": "EQ22205DZC23A-KPSDM-RAW-FULL-0535-TIME",
            "22au": "EQ22205DZC23A-KPSDM-RAW-FULL-0535-TIME",
            "23sp": "EQ23200DZC23A-KPSDM-RAW-FULL-0535-TIME",
            "23au": "EQ23205DZC23B-KPSDM-RAW-FULL-0535-TIME",
        }

        query_request = QueryRequest(
            "osdu:wks:work-product-component--GenericRepresentation:1.0.0", ""
        )
        result = self.search_client.query_records(query_request, self.access_token)
        result.raise_for_status()

        horizons = []

        if result.status_code == 200:
            osdu_objects = result.json().get("results")

            for osdu_object in osdu_objects:        
                name = osdu_object.get("data").get("Name")

                if not "4D_JS_" in name:
                    name = "4D_JS_" + name

                name_objects = name.split("_")

                date_string = name_objects [3]
                date_string_objects =  date_string.split("-")
                monitor_string = date_string_objects [0]
                base_string = date_string_objects [1]

                monitor_cube = js_cubes.get(monitor_string[:4])
                base_cube = js_cubes.get(base_string[:4])

                horizon = GenericRepresentation(
                    id=osdu_object.get("id"),
                    kind=osdu_object.get("kind"),
                    source=osdu_object.get("source"),
                    acl_viewers=osdu_object.get("acl").get("viewers"),
                    acl_owners=osdu_object.get("acl").get("owners"),
                    type=osdu_object.get("type"),
                    version=osdu_object.get("version"),
                    name=osdu_object.get("data").get("Name"),
                    datasets=osdu_object.get("data").get("Datasets"),
                    ow_horizon_name=osdu_object.get("tags").get("OW Horizon name"),
                    ow_top_horizon=osdu_object.get("tags").get("OW Top Horizon"),
                    ow_base_horizon=osdu_object.get("tags").get("OW Base Horizon"),
                    monitor_date=osdu_object.get("tags").get("Monitor reference date"),
                    base_date=osdu_object.get("tags").get("Base reference date"),
                    seismic_content=osdu_object.get("tags").get("Seismic content"),
                    horizon_content=osdu_object.get("tags").get("Horizon content"),
                    base_cube_name= base_cube,
                    monitor_cube_name=monitor_cube,
                )
                horizons.append(horizon)
        
        return horizons
    

    def get_dataset_info(self, id: str = "") -> Dataset:
        query = f"id:\"{id}\""
        query_request = QueryRequest(
            f"osdu:wks:dataset--File.Generic:*", query
        )
        result = self.search_client.query_records(query_request, self.access_token)
        result.raise_for_status()
        results = result.json().get("results")

        dataset = None

        if results:
            info = results[0]
            data = info.get("data")
            acl = info.get("acl")

            dataset = Dataset(
                id=info.get("id"),
                name=data.get("Name"),
                kind=info.get("kind"),
                source=data.get("Source"),
                acl_viewers=acl.get("viewers"),
                acl_owners=acl.get("owners"),
                type=info.get("type"),
                version=info.get("version"),
            )

        return dataset
    

    def get_dataset_ids(self, horizon) -> list[str ]:
        dataset_ids = horizon.datasets
        
        return dataset_ids
    

def main():
    osdu_service = DefaultOsduService()
    meta_version = "0.3.1"
    horizon_metadata = extract_osdu_metadata(osdu_service, meta_version)
    print(horizon_metadata)

if __name__ == '__main__':
    main()