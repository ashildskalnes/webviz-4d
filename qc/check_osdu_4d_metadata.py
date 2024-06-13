import os
import numpy as np
import pandas as pd
import argparse
from webviz_4d._providers.osdu_provider._provider_impl_file import DefaultOsduService

from webviz_4d._datainput.common import (
    read_config,
)

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
    description = "Check 4D maps in OSDU"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("config_file", help="Enter path to the configuration file")
    
    args = parser.parse_args()
    config_file = args.config_file

    config = read_config(config_file)
    config_file = os.path.abspath(config_file)
    config_folder = os.path.dirname(config_file)
    config_folder = os.path.abspath(config_folder)

    shared_settings = config.get("shared_settings")

    osdu = shared_settings.get("osdu")
    osdu_service = DefaultOsduService()

    # Search for 4D maps
    print("Searching for all seismic 4D attribute maps (GenericRepresentations) in OSDU ...")
    attribute_objects = osdu_service.get_attribute_horizons(None)
    
    print("  - found:", len(attribute_objects))

    no_metadata_versions = []
    wrong_metadata_versions = []
    wrong_names = []
    selected_attribute_maps = []
    auto4d_maps = []
    configuration_maps = []
    selected_metadata_version = "0.3.3"
    
    for attribute_object in attribute_objects:
        id = attribute_object.get("id")
        name = attribute_object.get("data").get("Name")
        tags =  attribute_object.get("tags")
        metadata_version = tags.get("MetadataVersion")
        data_source = attribute_object.get("data").get("Source")

        if data_source == "Auto4D":
            auto4d_maps.append(name)

        print()
        print("Name:",name)

        if metadata_version is None:
            no_metadata_versions.append(name)
            print("  WARNING: Metadata version not found") 
        elif metadata_version != selected_metadata_version:
            wrong_metadata_versions.append(name)
            print("  WARNING: Wrong metadata version:", metadata_version)
        else:
            if tags.get("Source.* Horizon") != name:
                print("  WARNING: OSDU name",name, "is different from OW name", tags.get("Source.* Horizon"))
                wrong_names.append(name)
            else:
                attribute_map = {"Name": name, "metadata_version": metadata_version}

                field_name = tags.get("AttributeMap.FieldName")
                attribute_map.update({"field_name": field_name})

                map_type = tags.get("AttributeMap.MapType")
                attribute_map.update({"map_type": map_type})

                diff_type = tags.get("AttributeMap.DifferenceType")
                attribute_map.update({"difference_type": diff_type})

                seismic_content = tags.get("AttributeMap.SeismicTraceContent")
                attribute_map.update({"Seismic content": seismic_content})

                difference = tags.get("AttributeMap.SeismicDifference")
                attribute_map.update({"SeismicDifference": difference})

                coverage = tags.get("AttributeMap.Coverage")
                attribute_map.update({"coverage": coverage})

                attribute_type = tags.get("AttributeMap.AttributeType")
                attribute_map.update({"Attribute type": attribute_type})

                seismic_volume_A = tags.get("SeismicProcessingTraces.SeismicVolumeA")
                attribute_map.update({"Seismic volume A": seismic_volume_A})

                seismic_volume_B = tags.get("SeismicProcessingTraces.SeismicVolumeA")
                attribute_map.update({"Seismic volume B": seismic_volume_B})

                window_mode = tags.get("CalculationWindow.WindowMode")

                if window_mode == "AroundHorizon":
                    attribute_map.update({"Window mode": window_mode})

                    seismic_horizon = tags.get("CalculationWindow.HorizonName")
                    attribute_map.update({"Horizon name": seismic_horizon})

                    shallow_offset = tags.get("CalculationWindow.HorizonOffsetShallow")
                    attribute_map.update({"Shallow offset": shallow_offset})

                    deep_offset = tags.get("CalculationWindow.HorizonOffsetDeep")
                    attribute_map.update({"Deep offset": deep_offset})

                elif window_mode == "BetweenHorizons":
                    attribute_map.update({"Window mode": window_mode})

                    seismic_horizon = tags.get("CalculationWindow.TopHorizonName")
                    attribute_map.update({"Top horizon name": seismic_horizon})

                    top_horizon_offset = tags.get("CalculationWindow.TopHorizonOffset")
                    attribute_map.update({"Top horizon offset": top_horizon_offset})

                    seismic_horizon = tags.get("CalculationWindow.BaseHorizonName")
                    attribute_map.update({"Base horizon name": seismic_horizon})

                    base_horizon_offset = tags.get("CalculationWindow.BaseHorizonOffset")
                    attribute_map.update({"Base horizon offset": base_horizon_offset})

                attribute_map.update({"id": id})

                selected_attribute_maps.append(attribute_map)
                #pprint(attribute_map)

    for attribute_map in selected_attribute_maps:
        status = True

        for key, value in osdu.items():
            if attribute_map.get(key) != value and status:
                status = False
                break

        print(attribute_map.get("Name"), status)

        if status:
            print()
            pprint(attribute_map, sort_dicts=False) 
            configuration_maps.append(attribute_map)

    print()
    print("Number of GenericRepresentations objects:", len(attribute_objects))
    print("  - Number of maps with no metadata version:", len(no_metadata_versions))
    print("  - Number of maps with wrong metadata version:", len(wrong_metadata_versions))
    print("  - Number of maps with wrong name:", len(wrong_names))
    print("  - Number of valid 4D maps with metadata version:", len(selected_attribute_maps))
    print("  - Number of selected 4D maps:", len(configuration_maps))

    print()
    print("Auto4D maps:", len(auto4d_maps))


                
                



if __name__ == '__main__':
    main()