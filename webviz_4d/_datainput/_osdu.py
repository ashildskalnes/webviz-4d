import pandas as pd
import time
import numpy as np
from ast import literal_eval
from pprint import pprint


def get_osdu_metadata_attributes(horizons):
    metadata_dicts = []

    # print("Compiling all attribute data ...")
    start_time = time.time()

    for horizon in horizons:
        if horizon:
            metadata_dicts.append(horizon.__dict__)

    maps_df = pd.DataFrame(metadata_dicts)
    columns = maps_df.columns
    new_columns = [col.replace("_", ".") for col in columns]
    maps_df.columns = new_columns

    # print(" --- %s seconds ---" % (time.time() - start_time))
    # print()

    maps_updated_df = maps_df.replace("", "---")

    return maps_updated_df


def convert_metadata(osdu_metadata):
    ids = []
    surface_names = []
    attributes = []
    times1 = []
    times2 = []
    seismic_contents = []
    coverages = []
    differences = []
    datasets = []
    field_names = []
    map_types = []
    map_names = []

    headers = [
        "id",
        "name",
        "attribute",
        "time.t1",
        "time.t2",
        "seismic",
        "coverage",
        "difference",
        "dataset_id",
        "field_name",
        "map_type",
        "map_names",
    ]

    for _index, row in osdu_metadata.iterrows():
        if "0.4.2" in row["MetadataVersion"]:
            id = row["id"]
            field_name = row["FieldName"]
            attribute_type = row["AttributeExtractionType"]
            seismic_content = row["SeismicTraceAttribute"]
            coverage = row["SeismicCoverage"]
            difference = row["SeismicDifferenceType"]
            horizon_names = row["StratigraphicZone"]
            dataset_ids = row["DatasetIDs"]
            map_name = row["Name"]

            if type(dataset_ids) == str:
                dataset_ids = literal_eval(dataset_ids)

            if dataset_ids and len(dataset_ids) > 0:
                dataset_id = dataset_ids[1]
            else:
                dataset_id = ""
        else:
            id = row["id"]
            field_name = row["AttributeMap.FieldName"]
            dataset_id = row["IrapBinaryID"]
            attribute_type = row["AttributeMap.AttributeType"]
            seismic_content = row["AttributeMap.SeismicTraceContent"]
            coverage = row["AttributeMap.Coverage"]
            difference = row["AttributeMap.SeismicDifference"]

            window_mode = row["CalculationWindow.WindowMode"]
            horizon_names = []

            if window_mode == "AroundHorizon":
                seismic_horizon = row["CalculationWindow.HorizonName"]
                seismic_horizon = seismic_horizon.replace("+", "_")
                horizon_names.append(seismic_horizon)
            elif window_mode == "BetweenHorizons":
                seismic_horizon = row["CalculationWindow.TopHorizonName"]
                seismic_horizon = seismic_horizon.replace("+", "_")
                horizon_names.append(seismic_horizon)

                # seismic_horizon = row["CalculationWindow.BaseHorizonName"]
                # seismic_horizon = seismic_horizon.replace("+", "_")
                # horizon_names.append(seismic_horizon)

        name = row["Name"]

        if difference == "RawDifference":
            difference = "NotTimeshifted"

        if difference == "TimeshiftedDifference":
            difference = "Timeshifted"

        if "simulated" in name:
            map_type = "simulated"
        else:
            map_type = "observed"

        map_types.append(map_type)
        field_names.append(field_name)
        ids.append(id)
        datasets.append(dataset_id)
        attributes.append(attribute_type)
        seismic_contents.append(seismic_content)
        coverages.append(coverage)
        differences.append(difference)
        surface_names.append(horizon_names)
        times = row["AcquisitionDates"]

        if type(times) == str:
            times = literal_eval(times)

        times1.append(times[0])
        times2.append(times[1])
        map_names.append(map_name)

    zipped_list = list(
        zip(
            ids,
            surface_names,
            attributes,
            times1,
            times2,
            seismic_contents,
            coverages,
            differences,
            datasets,
            field_names,
            map_types,
            map_names,
        )
    )

    metadata = pd.DataFrame(zipped_list, columns=headers)
    metadata.fillna(value=np.nan, inplace=True)
    metadata["original_name"] = osdu_metadata["Name"]

    return metadata


def create_osdu_lists(metadata, interval_mode):
    selectors = {
        "name": "name",
        "interval": "interval",
        "attribute": "attribute",
        "seismic": "seismic",
        "difference": "difference",
    }

    map_types = ["observed"]
    map_dict = {}

    for map_type in map_types:
        map_dict[map_type] = {}
        map_type_dict = {}

        map_type_metadata = metadata[metadata["map_type"] == map_type]
        map_type_metadata = map_type_metadata.where(~map_type_metadata.isna(), "")

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

                    if type(t1) == str and type(t2) is str:
                        if interval_mode == "normal":
                            interval = t2 + "-" + t1
                        else:
                            interval = t1 + "-" + t2
                    else:  # Drogon data hack
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

        print("DEBUG create osdu_list")
        print(map_dict)

    return map_dict


def main():
    metadata = pd.read_csv("metadata.csv")
    selection_list = create_osdu_lists(metadata, "normal")
    print(selection_list)


if __name__ == "__main__":
    main()
