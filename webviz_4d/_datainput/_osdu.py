import pandas as pd
import time
import numpy as np
from ast import literal_eval
from pprint import pprint


def get_osdu_dataset_id(surface_metadata, data, ensemble, real, map_type, coverage):
    selected_interval = data["date"]
    name = data["name"]
    attribute = data["attr"]

    if selected_interval[0:10] > selected_interval[11:]:
        time2 = selected_interval[0:10]
        time1 = selected_interval[11:]
    else:
        time1 = selected_interval[0:10]
        time2 = selected_interval[11:]

    surface_metadata.replace(np.nan, "", inplace=True)
    metadata_coverage = surface_metadata[surface_metadata["coverage"] == coverage]

    headers = [
        "attribute",
        "seismic",
        "difference",
        "time2",
        "time1",
        "map_name",
    ]

    print("Coverage", coverage)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.max_rows", None)
    print(metadata_coverage[headers].sort_values(by="attribute"))

    map_name = None

    try:
        selected_metadata = metadata_coverage[
            (metadata_coverage["difference"] == real)
            & (metadata_coverage["seismic"] == ensemble)
            & (metadata_coverage["map_type"] == map_type)
            & (metadata_coverage["time1"] == time1)
            & (metadata_coverage["time2"] == time2)
            & (metadata_coverage["name"] == name)
            & (metadata_coverage["attribute"] == attribute)
        ]

        print("Selected dataset info:")
        print(map_type, real, ensemble, name, attribute, time1, time2)

        if len(selected_metadata) != 1:
            print("WARNING number of datasets =", len(selected_metadata))
            print(selected_metadata)

        dataset_id = selected_metadata["dataset_id"].values[0]
        map_name = selected_metadata["map_name"].values[0]

        print(map_name, dataset_id)

        return dataset_id, map_name
    except:
        dataset_id = None
        print("WARNING: Selected map not found in OSDU. Selection criteria are:")
        print(map_type, real, ensemble, name, attribute, time1, time2)

    if dataset_id:
        dataset_id = literal_eval(dataset_id)

    return dataset_id, map_name


def get_correct_list(name, raw_metatadata_items):
    status = False

    metatadata_items = raw_metatadata_items

    if type(metatadata_items) is list and len(metatadata_items) == 1:
        items = metatadata_items[0]

        if "[" in metatadata_items[0]:
            new_names = items.replace("[", "").replace('"', "").replace("]", "")

        new_names = new_names.split(",")

        if type(new_names) is list and len(new_names) == 2:
            metatadata_items = new_names
        else:
            metatadata_items = [items]

    if type(metatadata_items) is str:
        names_split = metatadata_items.split(",")
        metatadata_items = names_split

    if len(metatadata_items) == 1:
        # print(name)
        # print(
        #     " - WARNING: Number of input seismic traces is",
        #     len(metatadata_items),
        # )

        nameA = metatadata_items[0]
        nameB = ""

    else:
        nameA = metatadata_items[0]
        nameB = metatadata_items[1]

    if nameA == "" or nameB == "":
        metatadata_items = raw_metatadata_items

        if type(metatadata_items) is str:
            metatadata_items = literal_eval(metatadata_items)

        if len(metatadata_items) != 2:
            status = False
        else:
            status = True
            nameA = metatadata_items[0]
            nameB = metatadata_items[1]
    else:
        status = True
        nameA = metatadata_items[0]
        nameB = metatadata_items[1]

    if status:
        metatadata_items = [
            nameA.replace(" ", ""),
            nameB.replace(" ", ""),
        ]
    else:
        metatadata_items = []

    return metatadata_items


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
    zone_names = []

    headers = [
        "id",
        "name",
        "attribute",
        "time1",
        "time2",
        "seismic",
        "coverage",
        "difference",
        "dataset_id",
        "field_name",
        "map_type",
        "map_name",
    ]

    for _index, row in osdu_metadata.iterrows():
        if "0.4.2" in row["MetadataVersion"]:
            id = row["id"]
            field_name = row["FieldName"]
            attribute_type = row["AttributeExtractionType"]
            seismic_content = row["SeismicTraceAttribute"]
            coverage = row["SeismicCoverage"]
            difference = row["SeismicDifferenceType"]
            zone = row["StratigraphicZone"]
            dataset_ids = row["DatasetIDs"]

            if type(dataset_ids) == str:
                dataset_ids = literal_eval(dataset_ids)

            if dataset_ids and len(dataset_ids) == 1:
                dataset_id = dataset_ids[0]
            elif dataset_ids and len(dataset_ids) == 2:
                dataset_id = dataset_ids[1]
            else:
                dataset_id = "FullReservoirEnvelope"
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
                horizon_names.append(horizon_names)

        name = row["Name"]

        if difference == "RawDifference" or difference == "NotTimeshifted":
            difference = "NotTimeshifted"
        elif difference == "TimeshiftedDifference" or difference == "Timeshifted":
            difference = "Timeshifted"
        elif difference == "":
            difference = "seismic_content"
        else:
            difference = "---"

        if "simulated" in name:
            map_type = "simulated"
        else:
            map_type = "observed"

        times = row["AcquisitionDates"]

        if type(times) == str:
            times = literal_eval(times)

        if times[0] != "" and times[0] != "":
            times1.append(times[0])
            times2.append(times[1])

            map_types.append(map_type)
            field_names.append(field_name)
            ids.append(id)
            datasets.append(dataset_id)
            attributes.append(attribute_type)
            seismic_contents.append(seismic_content)
            coverages.append(coverage)
            differences.append(difference)
            surface_names.append(zone)
            map_names.append(name)
        else:
            print("WARNING: No time interval information found:", name)

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

        intervals_df = map_type_metadata[["time1", "time2"]]
        intervals = []

        for key, value in selectors.items():
            selector = key
            map_type_metadata = metadata[metadata["map_type"] == map_type]

            map_type_dict[value] = {}

            if selector == "interval":
                for _index, row in intervals_df.iterrows():
                    t1 = row["time1"]
                    t2 = row["time2"]

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
                cleaned_list = [x for x in items if str(x) != "nan"]

                # if len(cleaned_list) == 1:
                #     items = cleaned_list
                # else:
                #     items = cleaned_list.sort()
                map_type_dict[value] = sorted(cleaned_list)

        map_dict[map_type] = map_type_dict

    return map_dict


def get_osdu_metadata(config, osdu_service, field_name):
    shared_settings = config.get("shared_settings")
    metadata_version = shared_settings.get("metadata_version")
    interval_mode = shared_settings.get("interval_mode")

    metadata = attribute_horizons = osdu_service.get_attribute_horizons(
        metadata_version=metadata_version, field_name=field_name
    )

    # print("Number of attribute maps:", len(attribute_horizons))
    print("Checking the extracted metadata ...")

    metadata = get_osdu_metadata_attributes(attribute_horizons)
    updated_metadata = osdu_service.update_reference_dates(metadata)
    selected_field_metadata = updated_metadata[
        updated_metadata["FieldName"] == field_name
    ]
    selected_field_metadata["map_type"] = "observed"
    converted_metadata = convert_metadata(selected_field_metadata)
    selection_list = create_osdu_lists(converted_metadata, interval_mode)

    return converted_metadata, selection_list


def main():
    metadata = pd.read_csv("metadata.csv")
    selection_list = create_osdu_lists(metadata, "normal")
    print(selection_list)


if __name__ == "__main__":
    main()
