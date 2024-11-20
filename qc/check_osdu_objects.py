import os
import numpy as np
import pandas as pd
import time
from datetime import datetime

from webviz_4d._providers.osdu_provider._provider_impl_file import DefaultOsduService
from webviz_4d._datainput._osdu import (
    get_osdu_metadata_attributes,
    convert_metadata,
)
import warnings


warnings.filterwarnings("ignore")


def main():
    osdu_service = DefaultOsduService()
    js_acquisitions = [
        "EQ19231",
        "EQ20231",
        "EQ21200",
        "EQ22200",
        "EQ22205",
        "EQ23200",
        "EQ23205",
    ]
    js_processings = [
        "EQ19231DZC23A-JOHAN-SVERDRUP-PRM",
        "EQ20231DZC23A-JOHAN-SVERDRUP-PRM",
        "EQ21200DZC23A-JOHAN-SVERDRUP-PRM",
        "EQ22200DZC23A-JOHAN-SVERDRUP-PRM",
        "EQ22205DZC23A-JOHAN-SVERDRUP-PRM",
        "EQ23200DZC23A-JOHAN-SVERDRUP-PRM",
        "EQ23205DZC23B-JOHAN-SVERDRUP-PRM",
    ]
    js_cubes = [
        "EQ19231DZC23A-KPSDM-RAW-FULL-0535-TIME",
        "EQ20231DZC23A-KPSDM-RAW-FULL-0535-TIME",
        "EQ21200DZC23A-KPSDM-RAW-FULL-0535-TIME",
        "EQ22200DZC23A-KPSDM-RAW-FULL-0535-TIME",
        "EQ22205DZC23A-KPSDM-RAW-FULL-0535-TIME",
        "EQ23200DZC23A-KPSDM-RAW-FULL-0535-TIME",
        "EQ23205DZC23B-KPSDM-RAW-FULL-0535-TIME",
    ]

    # 4D maps
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
        "dataset_ids",
    ]

    metadata_version = "0.3.3"
    updated_version = "0.4.2*"
    field_name = "JOHAN SVERDRUP"

    config_folder = "/private/ashska/dev_311/my_forks/fields/johan_sverdrup/osdu_config"
    cache_file = "metadata_cache.csv"
    metadata_file_cache = os.path.join(config_folder, cache_file)

    if os.path.isfile(metadata_file_cache):
        print("  Reading cached metadata from", metadata_file_cache)
        metadata = pd.read_csv(metadata_file_cache)
        metadata = metadata.replace("---", "")
        updated_metadata = metadata.loc[
            (metadata["FieldName"] == field_name)
            & (metadata["MetadataVersion"] == updated_version)
        ]

    else:
        print("Extracting metadata from OSDU Core ...")
        attribute_horizons = osdu_service.get_attribute_horizons(
            field_name=field_name, metadata_version=metadata_version
        )
        print("Number of valid attribute maps:", len(attribute_horizons))

        metadata = get_osdu_metadata_attributes(attribute_horizons)

        updated_metadata = osdu_service.update_reference_dates(metadata)
        updated_metadata.to_csv(metadata_file_cache)
        print("Updated metadata stored to:", metadata_file_cache)

    data_viewer_columns = {
        "FieldName": "FieldName",
        "Name": "Name",
        "Zone": "StratigraphicZone",
        "MapTypeDim": "MapTypeDimension",
        "SeismicAttribute": "SeismicTraceAttribute",
        "AttributeType": "AttributeExtractionType",
        "Coverage": "SeismicCoverage",
        "DifferenceType": "SeismicDifferenceType",
        "AttributeDiff": "AttributeDifferenceType",
        "Dates": "AcquisitionDates",
        "Version": "MetadataVersion",
    }

    standard_metadata = pd.DataFrame()
    for key, value in data_viewer_columns.items():
        standard_metadata[key] = updated_metadata[value]

    pd.set_option("display.max_rows", None)
    # pd.set_option("display.max_columns", None)
    print(standard_metadata)
    standard_metadata.to_csv("standard_metadata.csv")

    # Seismic horizons (structural interpretations)
    osdu_objects = osdu_service.get_seismic_horizons()
    print("Seismic horizons from OSDU:", len(osdu_objects))

    seismic_horizons = []

    for index, osdu_object in enumerate(osdu_objects):
        seismic_horizon = osdu_service.parse_seismic_horizon(osdu_object)

        if seismic_horizon:
            if seismic_horizon.FieldID != "":
                print(index, seismic_horizon.Name, seismic_horizon.FieldID)
                seismic_horizons.append(seismic_horizon)

    print("")

    # Seismic cubes
    for seismic_cube in js_cubes:
        seismic_cubes = osdu_service.get_seismic_trace_data(seismic_cube)

        if len(seismic_cubes) == 1:
            cube = seismic_cubes[0]
            print("Name:", cube.Name)
            print("  - id:", cube.id)
            print("  - domain:", cube.SeismicDomainTypeID)
            print("  - inline_min:", cube.InlineMin)
            print("  - inline_max:", cube.InlineMax)
            print("  - xline_min:", cube.CrosslineMin)
            print("  - xline_max:", cube.CrosslineMax)
            print("  - sample_interval:", cube.SampleInterval)
            print("  - sample_count:", cube.SampleCount)
            print("  - dataset_id:", cube.DatasetID)

    print("")

    # Seismic processing projects
    processings = []

    for js_processing in js_processings:
        processing = osdu_service.get_processing_projects(js_processing)

        if len(processing) == 1:
            processing = processing[0]

        project_name = processing.ProjectName
        id = processing.id
        acquisition_survey_id = processing.acquisition_survey_id

        print("Project name:", project_name)
        print("  - id:", id)
        print("  - acquisition_id:", acquisition_survey_id)

        # Seismic acquisition surveys
        surveys = osdu_service.get_seismic_surveys(
            selected_processing_project=processing, selected_trace_data=None
        )

        if len(surveys) == 1:
            survey = surveys[0]
            acquisition = osdu_service.parse_seismic_acquisition(survey)

            name = acquisition.ProjectName
            id = acquisition.id

            begin_date = acquisition.ProjectBeginDate
            end_date = acquisition.ProjectEndDate
            reference_date = acquisition.ProjectReferenceDate

            print("Project name:", name)
            print("  - id:", id)
            print("  - begin_date:", datetime.strftime(begin_date, "%Y-%m-%d"))
            print("  - end_date:", datetime.strftime(end_date, "%Y-%m-%d"))
            print("  - ref_date:", datetime.strftime(reference_date, "%Y-%m-%d"))

        print("")


if __name__ == "__main__":
    main()
