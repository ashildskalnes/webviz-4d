import numpy as np
import pandas as pd
from webviz_4d._datainput._osdu import DefaultOsduService

import warnings
from datetime import datetime
warnings.filterwarnings("ignore")
        

def main():
    osdu_service = DefaultOsduService()

    # Search for 4D maps
    attribute_horizons = osdu_service.get_attribute_horizons()
    print("Seismic 4D attribute maps from OSDU:", len(attribute_horizons))
    
    for horizon in attribute_horizons:
        ow_name=horizon.ow_horizon_name
        ow_top_horizon = horizon.ow_top_horizon
        ow_base_horizon = horizon.ow_base_horizon

        if "IUTU" in ow_top_horizon:
            ow_top_horizon = horizon.ow_top_horizon.replace("+","_")
        if "IUTU" in ow_base_horizon:
            ow_base_horizon = horizon.ow_base_horizon.replace("+","_")

        horizon_content=horizon.horizon_content
        seismic_content=horizon.seismic_content
        base_cube = horizon.base_cube_name
        monitor_cube = horizon.monitor_cube_name

        print("4D attribute map:", ow_name)
        print("  - horizon_content:", horizon_content)
        print("  - seismic_content:", seismic_content)

        dataset_ids = osdu_service.get_dataset_ids(horizon)
        print("  - Datasets:")

        for dataset_id in dataset_ids:
            print("    - dataset_id:",dataset_id[:-1])
            dataset_info = osdu_service.get_dataset_info(dataset_id)

            if dataset_info:
                print("        - Source:", dataset_info.source)
                print("        - Name:", dataset_info.name)

        js_horizon_names = [ow_top_horizon, ow_base_horizon]
        seismic_horizons = osdu_service.get_seismic_horizons(js_horizon_names)

        for seismic_horizon in seismic_horizons:
            print("    - Seismic horizon:", seismic_horizon.name)

        js_cubes = [base_cube, monitor_cube]
        seismic_cubes = osdu_service.get_seismic_cubes(js_cubes)
        
        for seismic_cube in seismic_cubes:
            print("    - Seismic cube:", seismic_cube.name)
            print("         - domain:", seismic_cube.domain)

            processing_id = seismic_cube.processing_project_id
            foo = ( [pos for pos, char in enumerate(processing_id) if char == ":"])
            position = foo[-1]
            processing_id = processing_id[:position]
            print("         - processing_id:", processing_id)

            processing = osdu_service.get_seismic_processing_metadata(processing_id)
            survey_id = processing.acquisition_survey_id
            foo = ( [pos for pos, char in enumerate(survey_id) if char == ":"])
            position = foo[-1]
            survey_id = survey_id[:position]

            if processing is not None:
                print("    - Processing project:", processing.project_name)
                print("         - acquisition_id:", survey_id)

            acquisition = osdu_service.get_seismic_acquisition_metadata(survey_id) 
            print("    - Acquisition survey:", acquisition.name)

            begin_date = datetime.strptime(acquisition.begin_date[:10],"%Y-%m-%d")
            end_date = datetime.strptime(acquisition.end_date[:10],"%Y-%m-%d")
            reference_date = begin_date + (end_date - begin_date)/2

            print("        - Begin date:", datetime.strftime(begin_date,"%Y-%m-%d" ))
            print("        - End date:", datetime.strftime(end_date,"%Y-%m-%d" )) 
            print("        - Reference date:", datetime.strftime(reference_date,"%Y-%m-%d" ))

        print()
    

        

        


    

    


        
    



   

if __name__ == '__main__':
    main()