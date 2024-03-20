import numpy as np
import pandas as pd
from webviz_4d._datainput._osdu import DefaultOsduService

import warnings
from datetime import datetime
from pprint import pprint

warnings.filterwarnings("ignore")

def find_all_substrings(txt,sub):
    positions = []
    start_index=0

    for i in range(len(txt)):
        j = txt.find(sub,start_index)
        if(j!=-1):
            start_index = j+1
            positions.append(j)
    
    return positions


def main():
    osdu_service = DefaultOsduService()

    # Search for 4D maps
    print("Searching for all seismic 4D attribute maps in OSDU ...")
    attribute_objects = osdu_service.get_all_attribute_horizons(None)
    print("  ", len(attribute_objects))

    selected_attribute_maps = {}
    selected_metadata_version = "0.3.2"

    for attribute_object in attribute_objects:
        kind = attribute_object.get("kind")
        id = attribute_object.get("id")
        name = attribute_object.get("data").get("Name")
        tags =  attribute_object.get("tags")
        metadata_version = tags.get("MetadataVersion")

        print(name)

        if metadata_version is None:
            print("  WARNING: Metadata version not found") 
            print()
        elif metadata_version != selected_metadata_version:
            print("  WARNING: Wrong metadata version:", metadata_version)
            print()
        else:
            if tags.get("Source.* Horizon") != name:
                print("  WARNING: OSDU name is different from OW name")
                print()
            else:
                attribute_map = {"Name": name, "id": id}
                seismic_content = tags.get("AttributeMap.SeismicTraceContent")
                attribute_map.update({"Seismic content": seismic_content})

                difference = tags.get("AttributeMap.SeismicDifference")
                attribute_map.update({"Difference type": difference})

                attribute_type = tags.get("AttributeMap.AttributeType")
                attribute_map.update({"Attribute type": attribute_type})

                all_datasets = attribute_object.get("data").get("Datasets")

                for dataset in all_datasets:
                    meta = osdu_service.get_osdu_metadata(dataset)
                    format_type = meta.get("data").get("EncodingFormatTypeID")
                    print("  dataset:", dataset, format_type)

                attribute_map.update({"Attribute map datasets":all_datasets})
                window_mode = tags.get("CalculationWindow.WindowMode")

                seismic_names = []

                if window_mode == "AroundHorizon":
                    seismic_horizon = tags.get("CalculationWindow.HorizonName")
                    seismic_horizon = seismic_horizon.replace("+","_")
                    seismic_names.append(seismic_horizon)
                elif window_mode == "BetweenHorizons":
                    seismic_horizon = tags.get("CalculationWindow.TopHorizonName")
                    seismic_horizon = seismic_horizon.replace("+","_")
                    seismic_names.append(seismic_horizon)

                    seismic_horizon = tags.get("CalculationWindow.BaseHorizonName")
                    seismic_horizon = seismic_horizon.replace("+","_")
                    seismic_names.append(seismic_horizon)

                pprint(attribute_map)

                seismic_horizon_objects = osdu_service.get_seismic_horizons(seismic_names)

                for osdu_object in seismic_horizon_objects:
                    data = osdu_object.get("data")
                    name = data.get("Name")
                    kind = osdu_object.get("kind")
                    datasets = data.get("Datasets")
                    bin_grid_id = data.get("BinGridID")[0]
                    seismic_trace_id = data.get("SeismicTraceDataID")[0]
                    print("Seismic horizon:", name)
                    print("- Kind:", kind)
                    print("- Datasets:", datasets)

                if selected_metadata_version == "0.3.1":
                    base_seismic_name = tags.get("SeismicProcessingTraces.BaseSeismicTraces")
                    monitor_seismic_name = tags.get("SeismicProcessingTraces.MonitorSeismicTraces")
                elif selected_metadata_version == "0.3.2":
                    base_seismic_name = tags.get("SeismicProcessingTraces.SeismicVolumeB")
                    monitor_seismic_name = tags.get("SeismicProcessingTraces.SeismicVolumeA")

                seismic_names = [base_seismic_name, monitor_seismic_name]
                seismic_objects = osdu_service.get_seismic_cubes(seismic_names)

                for seismic_object in seismic_objects:
                    data = seismic_object.get("data")
                    seismic_name = data.get("Name")
                    print("Seismic cube:", seismic_name)
                    
                    processing_project_id = data.get("ProcessingProjectID")
                    print("- Processing project id:", processing_project_id)

                    acquisition_id = data.get("PrincipalAcquisitionProjectID")
                    
                    positions = find_all_substrings(acquisition_id, ":")

                    if len(positions) == 3:
                        pos = positions [-1]
                        acquisition_id = acquisition_id[:pos]

                    print("- Acquisition id:", acquisition_id)

                    acquisition_meta = osdu_service.get_osdu_metadata(acquisition_id)
                    data = acquisition_meta.get("data")
                                        
                    begin_date = data.get("ProjectBeginDate")
                    end_date = data.get("ProjectEndDate")
                    begin_date = datetime.strptime(begin_date[:10],"%Y-%m-%d")
                    end_date = datetime.strptime(end_date[:10],"%Y-%m-%d")
                    reference_date = begin_date + (end_date - begin_date)/2

                    print("        - Begin date:", datetime.strftime(begin_date,"%Y-%m-%d" ))
                    print("        - End date:", datetime.strftime(end_date,"%Y-%m-%d" )) 
                    print("        - Reference date:", datetime.strftime(reference_date,"%Y-%m-%d" ))

                selected_attribute_maps.update(attribute_map)

            print()
                



if __name__ == '__main__':
    main()