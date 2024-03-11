import numpy as np
import pandas as pd
from webviz_4d._datainput._osdu import DefaultOsduService

import warnings
from datetime import datetime
warnings.filterwarnings("ignore")
        

def main():
    osdu_service = DefaultOsduService()
 
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
    attribute_horizons = osdu_service.get_attribute_horizons()
    print("Seismic 4D attribute maps from OSDU:", len(attribute_horizons))
    
    for horizon in attribute_horizons:
        ow_name=horizon.ow_horizon_name
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
        attribute = seismic_content + "_" + horizon_content
        attributes.append(attribute)

        dataset_ids = osdu_service.get_dataset_ids(horizon)
        print("  - Datasets:")

        for dataset_id in dataset_ids:
            print("    - dataset_id:",dataset_id[:-1])
            dataset_info = osdu_service.get_dataset_info(dataset_id)

            if dataset_info:
                print("      - Source:", dataset_info.source)
                print("      - Name:", dataset_info.name)

                if dataset_info.source == "OpenWorks":
                    datasets.append(dataset_id)
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

    print("Metadata overview")
    #pd.set_option('display.max_columns', None)
    print(metadata)
    print()

    # Seismic horizons (structural interpretations)
    horizons = osdu_service.get_seismic_horizons("")
    print("Seismic horizons from OSDU:", len(horizons))
    
    for horizon in horizons:
        name = horizon.name
        id = horizon.id
        dataset_ids = horizon.datasets

        print("Horizon name:",name)
        print("  - id:",id)
        print("  - Datasets:")

        for dataset_id in dataset_ids:
            print("    - dataset_id:",dataset_id[:-1])
            dataset_info = osdu_service.get_dataset_info(dataset_id)

            try:
                print("      - Source:", dataset_info.source)
            except:
                print("      WARNING: Missing information: Source")

            try:
                print("      - Name:", dataset_info.name)
            except:
                print("      WARNING: Missing information: Name")

    print("")

    # Seismic cubes
    seismic_cubes = osdu_service.get_seismic_cubes(["EQ*"])
    print("Seismic cubes from OSDU:",len(seismic_cubes))

    for cube in seismic_cubes:
        print("Name:",cube.name)
        print("  - id:",cube.id)
        #print("  - source:", cube.source)
        print("  - domain:", cube.domain)
        print("  - inline_min:", cube.inline_min)
        print("  - inline_max:", cube.inline_max)
        print("  - xline_min:", cube.xline_min)
        print("  - xline_max:", cube.xline_max)
        print("  - sample_interval:", cube.sample_interval)
        print("  - sample_count:", cube.sample_count)

    print("")

    # Seismic processing projects
    processings = osdu_service.get_seismic_processings("*")
    print("Seismic processing projects from OSDU:",len(processings))

    for processing in processings:
        project_name = processing.project_name
        id = processing.id

        print("Project name:",project_name)
        print("  - id:",id)

    print("")

    # Seismic acquisition surveys
    acquisitions = osdu_service.get_seismic_acquisitions("*")
    print("Seismic acquisitions from OSDU:",len(acquisitions))

    for acquisition in acquisitions:
        name = acquisition.name
        id = acquisition.id

        begin_date = datetime.strptime(acquisition.begin_date[:10],"%Y-%m-%d")
        end_date = datetime.strptime(acquisition.end_date[:10],"%Y-%m-%d")
        reference_date = begin_date + (end_date - begin_date)/2

        print("Project name:",name)
        print("  - id:",id)
        print("  - begin_date:", datetime.strftime(begin_date,"%Y-%m-%d" ))
        print("  - end_date:", datetime.strftime(end_date,"%Y-%m-%d" )) 
        print("  - ref_date:", datetime.strftime(reference_date,"%Y-%m-%d" )) 

    print("")

    

    


        
    



   

if __name__ == '__main__':
    main()