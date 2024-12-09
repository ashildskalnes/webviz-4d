import io
import pandas as pd
import time
from datetime import datetime
import xtgeo
import prettytable as pt

from webviz_4d._providers.osdu_provider._provider_impl_file import DefaultOsduService
from webviz_4d._datainput._osdu import (
    get_osdu_metadata_attributes,
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

    metadata_version = "0.4.2"
    field_name = "JOHAN SVERDRUP"

    print()
    print("--------------------------------------------------------------------------")
    print("OSDU Objects status:", datetime.now(), "npequinor | npequinor-dev")
    print("--------------------------------------------------------------------------")
    print()

    attribute_horizons = osdu_service.get_attribute_horizons(
        metadata_version=metadata_version, field_name=field_name
    )

    print("Number of attribute maps:", len(attribute_horizons))
    print()

    if len(attribute_horizons) == 0:
        exit()

    metadata = get_osdu_metadata_attributes(attribute_horizons)

    # # Load and print one attribute map
    # selected_metadata = metadata.iloc[0]
    # selected_name = selected_metadata["Name"]
    # dataset_ids = selected_metadata["DatasetIDs"]
    # dataset_id = dataset_ids[1]

    # if dataset_id is not None:
    #     print("Loading surface from OSDU Core:", selected_name)
    #     start_time = time.time()
    #     dataset = osdu_service.get_horizon_map(file_id=dataset_id)
    #     blob = io.BytesIO(dataset.content)
    #     surface = xtgeo.surface_from_file(blob)
    #     print(" --- %s seconds ---" % (time.time() - start_time))

    #     print(surface)
    #     print()

    print("Searching for corresponding seismic trace data ...")
    updated_metadata = osdu_service.update_reference_dates(metadata)
    # pprint("updated_metadata")

    data_viewer_columns = {
        "FieldName": "FieldName",
        "Name": "Name",
        "Zone": "StratigraphicZone",
        "MapDim": "MapTypeDimension",
        "SeismicAttribute": "SeismicTraceAttribute",
        "AttributeType": "AttributeExtractionType",
        "Coverage": "SeismicCoverage",
        "DifferenceType": "SeismicDifferenceType",
        "Dates": "AcquisitionDates",
        "Version": "MetadataVersion",
    }

    # Load and print one seismic cube
    # selected_cube_name = js_cubes[0]
    # selected_osu_objects = osdu_service.get_seismic_trace_data(selected_cube_name)
    # selected_osdu_object = selected_osu_objects[0]
    # selected_name = selected_osdu_object.Name
    # dataset_id = selected_osdu_object.DatasetID

    # if dataset_id is not None:
    #     print("Loading seismic trace data from OSDU Core:", selected_name)
    #     start_time = time.time()
    #     dataset = osdu_service.get_seismic_cube(file_id=dataset_id)
    #     blob = io.BytesIO(dataset.content)
    #     cube = xtgeo.cube_from_file(blob)
    #     print(" --- %s seconds ---" % (time.time() - start_time))

    #     print(cube)

    standard_metadata = pd.DataFrame()
    for key, value in data_viewer_columns.items():
        standard_metadata[key] = updated_metadata[value]

    print("SeismicAttributeInterpretation selected metadata:")
    pd.set_option("display.max_rows", None)
    print(standard_metadata)
    print()

    # Seismic horizons (structural interpretations)
    osdu_objects = osdu_service.get_seismic_horizons()

    seismic_horizons = []

    search_string = "3D+"
    print("Searching for seismic horizon names starting with:", search_string)

    horizon_table = pt.PrettyTable()
    horizon_table.field_names = ["Name", "FieldID", "FieldName"]
    FieldName = ""

    for _index, osdu_object in enumerate(osdu_objects):
        seismic_horizon = osdu_service.parse_seismic_horizon(osdu_object)

        if seismic_horizon:
            if seismic_horizon.Name[0 : len(search_string)] == search_string:
                horizon_table.add_row(
                    [seismic_horizon.Name, seismic_horizon.FieldID, FieldName]
                )
                seismic_horizons.append(seismic_horizon)

    print()
    print(horizon_table)
    print()

    # Seismic cubes
    seismic_table = pt.PrettyTable()
    seismic_table.field_names = [
        "Name",
        "FieldID",
        "InlineMin",
        "InlineMax",
        "XlineMin",
        "XlineMax",
    ]
    for seismic_cube in js_cubes:
        seismic_cubes = osdu_service.get_seismic_trace_data(seismic_cube)

        if len(seismic_cubes) == 1:
            cube = seismic_cubes[0]
            seismic_table.add_row(
                [
                    cube.Name,
                    cube.FieldID,
                    cube.InlineMin,
                    cube.InlineMax,
                    cube.CrosslineMin,
                    cube.CrosslineMax,
                ]
            )
            # print("Name:", cube.Name)
            # print("  - id:", cube.id)
            # print("  - domain:", cube.SeismicDomainTypeID)
            # print("  - inline_min:", cube.InlineMin)
            # print("  - inline_max:", cube.InlineMax)
            # print("  - xline_min:", cube.CrosslineMin)
            # print("  - xline_max:", cube.CrosslineMax)
            # print("  - sample_interval:", cube.SampleInterval)
            # print("  - sample_count:", cube.SampleCount)
            # print("  - dataset_id:", cube.DatasetID)

    print("")
    print(seismic_table)
    print("")

    # Seismic processing projects
    # processings = []

    # for js_processing in js_processings:
    #     processing = osdu_service.get_processing_projects(js_processing)

    #     if len(processing) == 1:
    #         processing = processing[0]

    #     project_name = processing.ProjectName
    #     id = processing.id
    #     acquisition_survey_id = processing.acquisition_survey_id

    #     print("Project name:", project_name)
    #     print("  - id:", id)
    #     print("  - acquisition_id:", acquisition_survey_id)

    #     # Seismic acquisition surveys
    #     surveys = osdu_service.get_seismic_surveys(
    #         selected_processing_project=processing, selected_trace_data=None
    #     )

    #     if len(surveys) == 1:
    #         survey = surveys[0]
    #         acquisition = osdu_service.parse_seismic_acquisition(survey)

    #         name = acquisition.ProjectName
    #         id = acquisition.id

    #         begin_date = acquisition.ProjectBeginDate
    #         end_date = acquisition.ProjectEndDate
    #         reference_date = acquisition.ProjectReferenceDate

    #         print("Project name:", name)
    #         print("  - id:", id)
    #         print("  - begin_date:", datetime.strftime(begin_date, "%Y-%m-%d"))
    #         print("  - end_date:", datetime.strftime(end_date, "%Y-%m-%d"))
    #         print("  - ref_date:", datetime.strftime(reference_date, "%Y-%m-%d"))

    #     print("")


if __name__ == "__main__":
    main()
