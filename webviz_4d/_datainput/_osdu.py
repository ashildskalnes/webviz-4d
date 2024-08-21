import pandas as pd
import time
import numpy as np

def get_osdu_metadata_attributes(horizons):
    metadata_dicts = []

    print("Compiling all attribute data ...")
    start_time = time.time()
    
    for horizon in horizons:
        metadata_dicts.append(horizon.__dict__)
        
    maps_df = pd.DataFrame(metadata_dicts)
    columns = maps_df.columns
    new_columns = [col.replace('_', '.') for col in columns]
    maps_df.columns = new_columns

    print(" --- %s seconds ---" % (time.time() - start_time))
    print() 
    return maps_df


def convert_metadata(osdu_metadata):
    surface_names = []
    attributes = []
    times1 = []
    times2 = []
    seismic_contents = []
    coverages = []
    differences = []
    datasets = []
    field_names = []

    headers = [
        "name",
        "attribute",
        "time.t1",
        "time.t2",
        "seismic",
        "coverage",
        "difference",
        "dataset_id",
        "field_name",
    ]
            
    for _index, row in osdu_metadata.iterrows():
        horizon_names = []

        field_name = row ["AttributeMap.FieldName"]
        irap_binary_dataset_id = row["IrapBinaryID"]
        attribute_type = row["AttributeMap.AttributeType"]
        seismic_content = row["AttributeMap.SeismicTraceContent"]
        coverage = row["AttributeMap.Coverage"]
        difference = row["AttributeMap.SeismicDifference"]

        field_names.append(field_name)
        datasets.append(irap_binary_dataset_id)
        attributes.append(attribute_type) 
        seismic_contents.append(seismic_content)
        coverages.append(coverage)
        differences.append(difference)

        window_mode = row["CalculationWindow.WindowMode"]

        if window_mode == "AroundHorizon":
                seismic_horizon = row["CalculationWindow.HorizonName"]
                seismic_horizon = seismic_horizon.replace("+","_")
                horizon_names.append(seismic_horizon)
        elif window_mode == "BetweenHorizons":
            seismic_horizon = row["CalculationWindow.TopHorizonName"]
            seismic_horizon = seismic_horizon.replace("+","_")
            horizon_names.append(seismic_horizon)

            seismic_horizon = row["CalculationWindow.BaseHorizonName"]
            seismic_horizon = seismic_horizon.replace("+","_")
            horizon_names.append(seismic_horizon)   

        surface_names.append(horizon_names[0])

        times1.append(row["AcquisitionDateB"])
        times2.append(row["AcquisitionDateA"])
                
    zipped_list = list(
        zip(
            surface_names,
            attributes,
            times1,
            times2,
            seismic_contents,
            coverages,
            differences,
            datasets,
            field_names,
        )
    )

    metadata = pd.DataFrame(zipped_list, columns=headers)
    metadata.fillna(value=np.nan, inplace=True)
    metadata["original_name"] = osdu_metadata["Name"]
    metadata["map_type"] = "observed"

    return metadata


def create_osdu_lists(metadata, interval_mode):
    selectors = {
        "name": "name",
        "interval": "interval",
        "attribute": "attribute",
        "seismic":"seismic",
        "difference": "difference"
    }

    map_types = ["observed"]
    map_dict = {}

    for map_type in map_types:
        map_dict[map_type] = {}
        map_type_dict = {}

        map_type_metadata = metadata[metadata["map_type"] == map_type]

        intervals_df = map_type_metadata[["time.t1", "time.t2"]]
        intervals = []

        for key, value in selectors.items():
            selector = key
            map_type_metadata = metadata[metadata["map_type"] == map_type]
            map_type_dict[value] = {}

            if selector == "interval":
                for _index, row in intervals_df.iterrows():
                    t1 = row["time.t1"]
                    t2 = row["time.t2"]

                    print("DEBUG", t1,t2)
                    print("DEBUG", type(t1),type(t2))

                    if type(t1) == str and type(t2) is str:
                        if interval_mode == "normal":
                            interval = t2 + "-" + t1
                        else:
                            interval = t1 + "-" + t2
                    else:                                   # Drogon data hack
                        t1 = "2018-01-01"
                        t2 = "2019-01-01"
                        interval = t2 + "-" + t1

                    if interval not in intervals:
                        intervals.append(interval)

                sorted_intervals = sorted(intervals)

                map_type_dict[value] = sorted_intervals
            else:
                items = list(map_type_metadata[selector].unique())
                items.sort()

                map_type_dict[value] = items

        map_dict[map_type] = map_type_dict

    return map_dict


def main():
    metadata = pd.read_csv("metadata.csv")
    selection_list = create_osdu_lists(metadata, "normal")
    print(selection_list)

if __name__ == '__main__':
    main()