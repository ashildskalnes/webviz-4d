import numpy as np
import pandas as pd
from datetime import datetime
import time
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

    def get_osdu_metadata(self, id):
        # Search for an osdu object with a selected id
        query = f'id:"{id}"'
        query_request = QueryRequest("*:*:*:*", query=query)
        result = self.search_client.query_records(query_request, self.access_token)
        result.raise_for_status()

        osdu_object = None

        if result.status_code == 200:
            osdu_objects = result.json().get("results")

            if len(osdu_objects) == 1:
                osdu_object = osdu_objects[0]

        return osdu_object

    def parse_seismic_attribute_horizon(
        self, osdu_object: dict
    ) -> osdu.SeismicAttributeHorizon:
        attribute_horizon = None

        id = osdu_object.get("id")
        kind = osdu_object.get("kind")
        Name = osdu_object.get("data").get("Name", "")
        Source = osdu_object.get("data").get("Source", "")

        if Source is None:
            Source = ""

        WindowMode = osdu_object.get("tags").get("CalculationWindow.WindowMode", "")

        if WindowMode == "BetweenHorizons":
            TopHorizonName = osdu_object.get("tags").get(
                "CalculationWindow.TopHorizonName", ""
            )
            BaseHorizonName = osdu_object.get("tags").get(
                "CalculationWindow.BaseHorizonName", ""
            )
            TopHorizonOffset = osdu_object.get("tags").get(
                "CalculationWindow.TopHorizonOffset", ""
            )
            BaseHorizonOffset = osdu_object.get("tags").get(
                "CalculationWindow.BaseHorizonOffset", ""
            )

            HorizonName = ""
            HorizonOffsetShallow = ""
            HorizonOffsetDeep = ""
        elif WindowMode == "AroundHorizon":
            TopHorizonName = ""
            BaseHorizonName = ""
            TopHorizonOffset = ""
            BaseHorizonOffset = ""

            HorizonName = osdu_object.get("tags").get(
                "CalculationWindow.HorizonName", ""
            )
            HorizonOffsetShallow = osdu_object.get("tags").get(
                "CalculationWindow.HorizonOffsetShallow", ""
            )
            HorizonOffsetDeep = osdu_object.get("tags").get(
                "CalculationWindow.HorizonOffsetSDeep", ""
            )
        else:
            print(Name, "  - WARNING WindowMode is not valid:", WindowMode)
            map = osdu.SeismicAttributeHorizon(
                id=id, kind=kind, Name=Name, Source=Source
            )

        if WindowMode == "BetweenHorizons" or WindowMode == "AroundHorizon":
            all_datasets = osdu_object.get("data").get("Datasets")
            irap_dataset_id = ""

            for dataset_id in all_datasets:
                metadata = self.get_osdu_metadata(dataset_id)
                format_type = metadata.get("data").get("EncodingFormatTypeID", "")

                if format_type and "irap-binary" in format_type:
                    irap_dataset_id = dataset_id

            attribute_horizon = osdu.SeismicAttributeHorizon(
                id=id,
                kind=kind,
                Name=Name,
                Source=Source,
                AttributeMap_FieldName=osdu_object.get("tags").get(
                    "AttributeMap.FieldName", ""
                ),
                MetadataVersion=osdu_object.get("tags").get("MetadataVersion", ""),
                AttributeMap_AttributeType=osdu_object.get("tags").get(
                    "AttributeMap.AttributeType", ""
                ),
                AttributeMap_Coverage=osdu_object.get("tags").get(
                    "AttributeMap.Coverage", ""
                ),
                AttributeMap_DifferenceType=osdu_object.get("tags").get(
                    "AttributeMap.DifferenceType", ""
                ),
                AttributeMap_MapType=osdu_object.get("tags").get(
                    "AttributeMap.MapType", ""
                ),
                AttributeMap_Name=osdu_object.get("tags").get("AttributeMap.Name", ""),
                AttributeMap_SeismicDifference=osdu_object.get("tags").get(
                    "AttributeMap.SeismicDifference", ""
                ),
                AttributeMap_SeismicTraceContent=osdu_object.get("tags").get(
                    "AttributeMap.SeismicTraceContent", ""
                ),
                CalculationWindow_WindowMode=WindowMode,
                CalculationWindow_TopHorizonName=TopHorizonName,
                CalculationWindow_BaseHorizonName=BaseHorizonName,
                CalculationWindow_TopHorizonOffset=TopHorizonOffset,
                CalculationWindow_BaseHorizonOffset=BaseHorizonOffset,
                CalculationWindow_HorizonName=HorizonName,
                CalculationWindow_HorizonOffsetShallow=HorizonOffsetShallow,
                CalculationWindow_HorizonOffsetDeep=HorizonOffsetDeep,
                SeismicProcessingTraces_SeismicVolumeA=osdu_object.get("tags").get(
                    "SeismicProcessingTraces.SeismicVolumeA", ""
                ),
                SeismicProcessingTraces_SeismicVolumeB=osdu_object.get("tags").get(
                    "SeismicProcessingTraces.SeismicVolumeB", ""
                ),
                IrapBinaryID=irap_dataset_id,
            )

        return attribute_horizon

    def parse_seismic_trace_data(self, osdu_object: dict) -> osdu.SeismicTraceData:
        seismic_trace_data = osdu.SeismicTraceData(
            id=osdu_object.get("id"),
            kind=osdu_object.get("kind"),
            Name=osdu_object.get("data").get("Name", ""),
            InlineMin=osdu_object.get("data").get("InlineMin", ""),
            InlineMax=osdu_object.get("data").get("InlineMax", ""),
            CrosslineMin=osdu_object.get("data").get("CrosslineMin", ""),
            CrosslineMax=osdu_object.get("data").get("CrosslineMax", ""),
            SampleInterval=osdu_object.get("data").get("SampleInterval", ""),
            SampleCount=osdu_object.get("data").get("SampleCount", ""),
            SeismicDomainTypeID=osdu_object.get("data").get("SeismicDomainTypeID", ""),
            PrincipalAcquisitionProjectID=osdu_object.get("data").get(
                "PrincipalAcquisitionProjectID", ""
            ),
        )

        return seismic_trace_data

    def parse_seismic_acquisition(
        self, osdu_object: dict
    ) -> osdu.SeismicAcquisitionSurvey:
        begin_date = osdu_object.get("data").get("ProjectBeginDate", "")

        if len(begin_date) > 1:
            begin_date = begin_date[:10]

        end_date = osdu_object.get("data").get("ProjectEndDate", "")

        if len(end_date) > 1:
            end_date = end_date[:10]

        reference_date = (
            datetime.strptime(begin_date, "%Y-%m-%d")
            + (
                datetime.strptime(end_date, "%Y-%m-%d")
                - datetime.strptime(begin_date, "%Y-%m-%d")
            )
            / 2
        )
        reference_date = datetime.strftime(reference_date, "%Y-%m-%d")

        seismic_seismic_acquisition = osdu.SeismicAcquisitionSurvey(
            id=osdu_object.get("id"),
            kind=osdu_object.get("kind"),
            ProjectName=osdu_object.get("data").get("ProjectName", ""),
            ProjectID=osdu_object.get("data").get("ProjectID", ""),
            ProjectBeginDate=begin_date,
            ProjectEndDate=end_date,
            ProjectReferenceDate=reference_date,
        )

        return seismic_seismic_acquisition

    def empty_seismic_acquisition(self) -> osdu.SeismicAcquisitionSurvey:
        seismic_seismic_acquisition = osdu.SeismicAcquisitionSurvey(
            id="",
            kind="",
            ProjectName="",
            ProjectID="",
            ProjectBeginDate="",
            ProjectEndDate="",
            ProjectReferenceDate="",
        )

        return seismic_seismic_acquisition

    def get_attribute_horizons(
        self,
        search_key: Optional[str] = "",
        search_value: Optional[str] = "",
    ) -> list[osdu.SeismicAttributeHorizon]:

        # Search for all or selected objects of type GenericRepresentation
        query = ""

        if search_key and search_value:
            query = f'tags.AttributeMap.FieldName:"{search_value}"'

        start_time = time.time()
        print("Searching for all attribute horizons ...")

        query_request = QueryRequest(
            "osdu:wks:work-product-component--GenericRepresentation:*",
            query,
            limit=1000,
        )

        # print("Query request:", query_request.kind)

        result = self.search_client.query_records(query_request, self.access_token)
        result.raise_for_status()

        print("--- %s seconds ---" % (time.time() - start_time))

        seismic_attribute_horizons = []

        if result.status_code == 200:
            osdu_objects = result.json().get("results")
            print(
                "Number of selected GenericRepresentation objects:", len(osdu_objects)
            )
            print()

            print("Extracting attribute data ...")
            start_time = time.time()

            for osdu_object in osdu_objects:
                try:
                    seismic_attribute_horizon = self.parse_seismic_attribute_horizon(
                        osdu_object
                    )
                except:
                    id = osdu_object.get("id")
                    kind = osdu_object.get("kind")
                    Name = osdu_object.get("data").get("Name", "")
                    print(
                        "  WARNING: Not possible to convert to SeismicAttributeHorizon - ",
                        Name,
                    )

                    seismic_attribute_horizon = osdu.SeismicAttributeHorizon(
                        id, kind, Name
                    )

                if seismic_attribute_horizon:
                    seismic_attribute_horizons.append(seismic_attribute_horizon)

        print(" --- %s seconds ---" % (time.time() - start_time))
        print()

        return seismic_attribute_horizons

    def get_seismic_trace_data(self, selected_name) -> list[osdu.SeismicTraceData]:
        # Search for objects of type SeismicTraceData
        query = ""

        if len(selected_name) > 0:
            query = f'data.Name:"{selected_name}"'
        query_request = QueryRequest(
            "osdu:wks:work-product-component--SeismicTraceData:*", query, limit=1000
        )

        # print("Query request:", query_request.kind)

        result = self.search_client.query_records(query_request, self.access_token)
        result.raise_for_status()

        seismic_trace_datas = []

        if result.status_code == 200:
            osdu_objects = result.json().get("results")
            # print("Number of seismic trace data:", len(osdu_objects))
            # print()

            for osdu_object in osdu_objects:
                try:
                    seismic_trace_data = self.parse_seismic_trace_data(osdu_object)

                    if seismic_trace_data.Name == selected_name:
                        seismic_trace_datas.append(seismic_trace_data)
                except:
                    id = osdu_object.get("id")
                    kind = osdu_object.get("kind")
                    Name = osdu_object.get("data").get("Name", "")
                    print(
                        Name, "  - WARNING not possible to convert to SeismicTraceData"
                    )
                    seismic_trace_data = osdu.SeismicTraceData(id, kind, Name)
                    seismic_trace_datas.append(seismic_trace_data)

        return seismic_trace_datas

    def get_seismic_surveys(
        self, selected_trace_data: Optional[osdu.SeismicTraceData]
    ) -> list[osdu.SeismicAcquisitionSurvey]:
        # Search for objects of type SeismicAcquisitionSurvey
        query = ""

        seismic_surveys = []

        if selected_trace_data:
            survey_id = selected_trace_data.PrincipalAcquisitionProjectID
            survey_object = self.get_osdu_metadata(survey_id)

            if survey_object:
                try:
                    seismic_survey = self.parse_seismic_acquisition(survey_object)
                except:
                    print("WARNING: something went wrong")
                    return seismic_surveys
            else:
                # print("  WARNING: survey_id not valid:", survey_id)
                survey_name = selected_trace_data.Name[:7]
                query = f'data.ProjectName:"{survey_name}"'

        query_request = QueryRequest(
            "osdu:wks:master-data--SeismicAcquisitionSurvey:*", query, limit=1000
        )

        # print("Query request:", query_request.kind)

        result = self.search_client.query_records(query_request, self.access_token)
        result.raise_for_status()

        if result.status_code == 200:
            osdu_objects = result.json().get("results")
            # print("Number of seismic surveys:", len(osdu_objects))

            for osdu_object in osdu_objects:
                seismic_survey = self.parse_seismic_acquisition(osdu_object)
                try:
                    seismic_survey = self.parse_seismic_acquisition(osdu_object)
                except:
                    id = osdu_object.get("id")
                    kind = osdu_object.get("kind")
                    ProjectName = osdu_object.get("data").get("Name", "")
                    print(
                        ProjectName,
                        "  - WARNING not possible to convert to SeismicAcquisitionSurvey",
                    )
                    seismic_survey = osdu.SeismicAcquisitionSurvey(
                        id, kind, ProjectName
                    )

                seismic_surveys.append(seismic_survey)

        return seismic_surveys

    def get_metadata_attributes(self, horizons):
        metadata_dicts = []

        for horizon in horizons:
            metadata_dicts.append(horizon.__dict__)

        maps_df = pd.DataFrame(metadata_dicts)
        columns = maps_df.columns
        new_columns = [col.replace("_", ".") for col in columns]
        maps_df.columns = new_columns

        return maps_df

    def update_reference_dates(self, metadata_input):
        metadata = metadata_input.copy(deep=True)
        datesA = []
        datesB = []

        metadata_version = metadata_input["MetadataVersion"].values[0]

        print("Extracting reference dates ...")
        start_time = time.time()

        for index, row in metadata.iterrows():
            # print(row["Name"])
            if metadata_version == "0.4.2":
                seismic_names = row["OsduSeismicTraceNames"]
                seismic_nameA = seismic_names[1]
                seismic_nameB = seismic_names[0]
            else:
                seismic_nameA = row["SeismicProcessingTraces.SeismicVolumeA"]
                seismic_nameB = row["SeismicProcessingTraces.SeismicVolumeB"]

            seismic_nameA_data = self.get_seismic_trace_data(seismic_nameA)

            if len(seismic_nameA_data) > 0:
                seismic_nameA_data = seismic_nameA_data[0]
                acquisition_surveys = self.get_seismic_surveys(seismic_nameA_data)

                if len(acquisition_surveys) > 1:
                    print(row["Name"])
                    print(
                        "  - WARNING: Number of selected acquisitions are:",
                        len(acquisition_surveys),
                    )

                acquisition_surveyA = acquisition_surveys[0]
            else:
                acquisition_surveyA = self.empty_seismic_acquisition()
                print(row["Name"])
                print("  - WARNING: No acquisitions surveys found")

            # print(
            #     "  - Seismic name",
            #     seismic_nameA,
            #     acquisition_surveyA.ProjectBeginDate,
            #     acquisition_surveyA.ProjectEndDate,
            #     acquisition_surveyA.ProjectReferenceDate,
            # )

            seismic_nameB_data = self.get_seismic_trace_data(seismic_nameB)

            if len(seismic_nameB_data) > 0:
                seismic_nameB_data = seismic_nameB_data[0]
                acquisition_surveys = self.get_seismic_surveys(seismic_nameB_data)

                if len(acquisition_surveys) > 1:
                    print(
                        "  - WARNING: Number of selected acquisitions are:",
                        len(acquisition_surveys),
                    )

                acquisition_surveyB = acquisition_surveys[0]
            else:
                acquisition_surveyB = self.empty_seismic_acquisition()
                print("  - WARNING: No acquisitions surveys found")

            # print(
            #     "  - Seismic name",
            #     seismic_nameB,
            #     acquisition_surveyB.ProjectBeginDate,
            #     acquisition_surveyB.ProjectEndDate,
            #     acquisition_surveyB.ProjectReferenceDate,
            # )
            # print()

            datesA.append(acquisition_surveyA.ProjectReferenceDate)
            datesB.append(acquisition_surveyB.ProjectReferenceDate)

        metadata["AcquisitionDateA"] = datesA
        metadata["AcquisitionDateB"] = datesB

        print(" --- %s seconds ---" % (time.time() - start_time))
        print()

        selected = ["Name", "AcquisitionDateA", "AcquisitionDateB"]
        print(metadata[selected])

        return metadata

    def get_horizon_map(self, file_id: str) -> np.ndarray:
        # Some file id taken from metadata can sometimes end with ':', so remove it just in case
        if file_id.endswith(":"):
            file_id = file_id.rstrip(":")
        # Get the download url for the data
        expiry_time: str = (
            "2M"  # TODO: Find out best expiry time, for the time being it is 1 minute
        )
        # print(self.access_token)
        result = self.base_client.make_request(
            method=HttpMethod.GET,
            url=f'{self.base_client.config_manager.get("", "FILE_DMS_URL")}/files/{file_id}/downloadURL?expiryTime={expiry_time}',
            bearer_token=self.access_token,
        )
        result.raise_for_status()
        # Extract download url
        signed_url = result.json().get("SignedUrl")

        try:
            file_result = requests.get(signed_url)
            file_result.raise_for_status()

            return file_result

        except IndexError:
            raise Exception("The data format is not correct format (ijzyz)")
        except Exception as e:
            raise Exception(f"Something went wrong with horizon download, {e}")
