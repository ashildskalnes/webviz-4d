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
    ) -> osdu.SeismicAttributeInterpretation:
        attribute_horizon = None

        id = osdu_object.get("id")
        kind = osdu_object.get("kind")
        Name = osdu_object.get("data").get("Name", "")
        metadata_version = osdu_object.get("tags").get("MetadataVersion")

        if metadata_version is None:
            return None

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
            print(Name, "  - WARNING: WindowMode is not valid:", WindowMode)
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

            attribute_horizon = osdu.SeismicAttributeHorizon_033(
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

    def convert_attribute_horizon(
        self, attribute_horizon: osdu.SeismicAttributeHorizon_033
    ) -> osdu.SeismicAttributeInterpretation:
        """Convert metadata version 0.3.3 to version 0.4.2"""
        version = "0.4.2*"

        osdu_seismic_traces = [
            attribute_horizon.SeismicProcessingTraces_SeismicVolumeA,
            attribute_horizon.SeismicProcessingTraces_SeismicVolumeB,
        ]
        seismic_content = attribute_horizon.AttributeMap_SeismicTraceContent
        window_mode = attribute_horizon.CalculationWindow_WindowMode
        seismic_dfference = attribute_horizon.AttributeMap_SeismicDifference

        if seismic_content == "Timeshift" or seismic_content == "Timestrain":
            seismic_dfference = ""
        elif seismic_content == "RawDifference":
            seismic_dfference = "NotTimeshifted"
        elif seismic_content == "TimeshiftedDifference":
            seismic_dfference = "Timeshifted"

        if window_mode == "BetweenHorizons":
            horizon_names = [
                attribute_horizon.CalculationWindow_TopHorizonName,
                attribute_horizon.CalculationWindow_BaseHorizonName,
            ]
            horizon_offsets = [
                attribute_horizon.CalculationWindow_HorizonOffsetShallow,
                attribute_horizon.CalculationWindow_HorizonOffsetDeep,
            ]
        elif window_mode == "AroundHorizon":
            horizon_names = [attribute_horizon.CalculationWindow_TopHorizonName, None]
            horizon_offsets = [attribute_horizon.CalculationWindow_HorizonOffsetShallow]

        if (
            attribute_horizon.CalculationWindow_TopHorizonName
            == "3D+TAasgard+JS+Z22+Merge_EQ20231_PH2DG3"
        ):
            zone = "FullReservoirEnvelope"
        elif (
            attribute_horizon.CalculationWindow_HorizonName
            == "3D+IUTU+JS+Z22+Merge_EQ20231_PH2DG3"
        ):
            zone = "3D+IUTU+JS+Z22+Merge_EQ20231_PH2DG3"
        else:
            zone = ""

        seismic_attribute_horizon = osdu.SeismicAttributeInterpretation(
            id=attribute_horizon.id,
            kind=attribute_horizon.kind,
            Name=attribute_horizon.Name,
            MetadataVersion=version,
            FieldName=attribute_horizon.AttributeMap_FieldName,
            SeismicBinGridName="",
            ApplicationName="",
            MapTypeDimension="4D",
            SeismicTraceDataSource="",
            SeismicTraceDataSourceNames=[],
            SeismicTraceDomain="Unknown",
            SeismicTraceAttribute=seismic_content,
            OsduSeismicTraceNames=osdu_seismic_traces,
            SeismicDifferenceType=seismic_dfference,
            AttributeWindowMode=window_mode,
            HorizonDataSource="",
            HorizonSourceNames=horizon_names,
            StratigraphicZone=zone,
            AttributeExtractionType=attribute_horizon.AttributeMap_AttributeType,
            AttributeDifferenceType=attribute_horizon.AttributeMap_DifferenceType,
            SeismicCoverage=attribute_horizon.AttributeMap_Coverage,
            SeismicTraceDataSourceIDs=["", ""],
            StratigraphicColumn="",
            HorizonSourceIDs=["", ""],
            HorizonOffsets=horizon_offsets,
            FixedWindowValues=["", ""],
            IrapBinaryID=attribute_horizon.IrapBinaryID,
        )

        return seismic_attribute_horizon

    def get_attribute_horizons(
        self,
        field_name: Optional[str] = "",
        metadata_version: Optional[str] = "",
    ) -> list[osdu.SeismicAttributeInterpretation]:

        # Search for all or selected objects of type GenericRepresentation or SeismicAttributeInterpretation
        versions = {
            "0.3.3": "osdu:wks:work-product-component--GenericRepresentation:*",
            "0.4.2": "eqnr:cns-api:seismic-attribute-interpretation:*",
        }

        field_name_options = {
            "0.3.3": "tags.AttributeMap.FieldName",
            "0.4.2": "FieldName",
        }

        attribute_horizons = []

        if metadata_version not in versions.keys():
            print("ERROR: Metadata version not supported", metadata_version)
            return attribute_horizons

        query = ""

        if metadata_version == "":
            kinds = list(versions.values())
        else:
            kinds = [metadata_version]

        print("Searching for attribute horizons ...", field_name, metadata_version)

        if field_name != "":
            # query = field_name_options.get(metadata_version)
            query = f'tags.AttributeMap.FieldName:"{field_name}"'

        for kind in kinds:
            query_request = QueryRequest(
                versions.get(metadata_version), query, limit=50
            )
            # print("Query request:", query_request.kind)

            result = self.search_client.query_records(query_request, self.access_token)
            result.raise_for_status()

            if result.status_code == 200:
                osdu_objects = result.json().get("results")
                print(
                    "Number of ",
                    versions.get(kind),
                    "objects found:",
                    len(osdu_objects),
                )
                print()
                print("Extracting attribute data ...")

                for osdu_object in osdu_objects:
                    data = osdu_object.get("data")
                    tags = osdu_object.get("tags")

                    if data and tags:
                        metadata_version = osdu_object.get("tags").get(
                            "MetadataVersion"
                        )
                        name = data.get("Name")
                    else:
                        metadata_version = None

                    if metadata_version in versions:
                        attribute_horizon = self.parse_seismic_attribute_horizon(
                            osdu_object
                        )

                        if attribute_horizon is not None:
                            seismic_attribute_horizon = self.convert_attribute_horizon(
                                attribute_horizon
                            )
                    else:
                        print(
                            "WARNING:",
                            name,
                            " Unsupported metadata version:",
                            metadata_version,
                        )
                        seismic_attribute_horizon = None

                    if seismic_attribute_horizon:
                        attribute_horizons.append(seismic_attribute_horizon)

        print()

        return attribute_horizons

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
                        Name, "  - WARNING: Not possible to convert to SeismicTraceData"
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
                    print("WARNING: Something went wrong:-()")
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
                        "  - WARNING: Not possible to convert to SeismicAcquisitionSurvey",
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
        dates = []

        metadata_version = metadata_input["MetadataVersion"].values[0]

        start_time = time.time()

        for index, row in metadata.iterrows():
            # print(" - ", row["Name"])
            dateA = ""
            dateB = ""

            seismic_names = row["OsduSeismicTraceNames"]
            seismic_nameA = seismic_names[1]
            seismic_nameB = seismic_names[0]
            # else:
            #     seismic_nameA = row["SeismicProcessingTraces.SeismicVolumeA"]
            #     seismic_nameB = row["SeismicProcessingTraces.SeismicVolumeB"]

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

            dateA = acquisition_surveyA.ProjectReferenceDate
            dateB = acquisition_surveyB.ProjectReferenceDate

            dates.append([dateA, dateB])
            # print("   - acquisition dates found:", [dateA, dateB])

        metadata["AcquisitionDates"] = dates

        # print(" --- %s seconds ---" % (time.time() - start_time))
        # print()

        # selected = ["Name", "AcquisitionDates"]
        # print(metadata[selected])

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
