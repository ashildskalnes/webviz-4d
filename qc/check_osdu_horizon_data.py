import os
import numpy as np
import pandas as pd
from webviz_4d._datainput._osdu import get_osdu_service
import requests

def prRed(skk): print("\033[91m {}\033[00m" .format(skk))

def main():
    home = os.path.expanduser("~")
    osdu_service = get_osdu_service()

    print("Seismic horizons from OSDU:")
    horizons = osdu_service.get_seismic_horizons()
    #horizons = []
    
    for horizon in horizons:
        name = horizon.name
        id = horizon.id
        dataset_ids = horizon.datasets

        print("Horizon name:",name)
        print("  - id:",id)
        print("  - Datasets:")

        if len(dataset_ids) == 0:
            print("WARNING: No datasets available")

        for dataset_id in dataset_ids:
            print("    - dataset_id:",dataset_id[:-1])
            dataset_info = osdu_service.get_dataset_info(dataset_id)
            
            try:
                source = dataset_info.source
                print("      - Source:", source)
            except:
                source = None
                print("      WARNING: Dataset source not found ")

            try:
                name = dataset_info.name
                path= os.path.join(home, "Downloads", name)

                signed_url = osdu_service.get_signed_url(dataset_id)

                with open(path, "wb") as f:
                    with requests.get(signed_url, stream=True) as result:
                        result.raise_for_status()
                        for chunk in result.iter_content(chunk_size=1024):
                            f.write(chunk)

                print("      Dataset file has been downloaded to", path)
                    
            except:
                name = None
                print("      WARNING: Dataset name not found ")

    print("")
    print("Seismic 4D attribute maps from OSDU:")

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
        print(horizon.name)
        print("  - Datasets:")

        for dataset_id in dataset_ids:
            print("    - dataset_id:",dataset_id[:-1])
            dataset_info = osdu_service.get_dataset_info(dataset_id)

            if dataset_info:
                print("      - Source:", dataset_info.source)
                print("      - Name:", dataset_info.name)

                path= os.path.join(home, "Downloads", dataset_info.name)
                signed_url = osdu_service.get_signed_url(dataset_id)

                with open(path, "wb") as f:
                    with requests.get(signed_url, stream=True) as result:
                        result.raise_for_status()
                        for chunk in result.iter_content(chunk_size=1024):
                            f.write(chunk)

                print("      Dataset file has been downloaded to", path)

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
    print(metadata)

if __name__ == '__main__':
    main()