#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from time import strptime
import calendar
import argparse
import numpy as np
import pandas as pd
from webviz_4d._datainput import common


def get_start_stop_dates(well_prod_data, prod_file_update, volume_code):
    well_prod_data.replace(" 00:00:00", "", regex=True, inplace=True)
    # print(well_prod_data)

    start_date = well_prod_data.loc[well_prod_data[volume_code] > 0, "DATEPRD"].min()
    stop_date = well_prod_data.loc[well_prod_data[volume_code] > 0, "DATEPRD"].max()

    if not common.is_nan(start_date):
        stop_utc_time = strptime(stop_date, "%Y-%m-%d")
        epoch_time = calendar.timegm(stop_utc_time)
        # print(stop_date,prod_file_update - epoch_time)

        if prod_file_update - epoch_time < 130000:
            # print(stop_date)
            stop_date = np.nan

    return start_date, stop_date


def check_production_wells(sorted_production_wells, well_info, pdm_names_file):
    well_names = []

    # print(sorted_production_wells)

    for pdm_well in sorted_production_wells:
        well_name = common.get_wellname(well_info, pdm_well)

        if not well_name:
            print("ERROR: " + pdm_well + " not found in REP database")
            pdm_names = pd.read_csv(pdm_names_file)

            try:
                row = pdm_names[pdm_names["PDM well name"] == pdm_well]
                correct_name = row["Well name"][0]
                well_name = common.get_wellname(well_info, correct_name)
                print("Alias name found in " + pdm_names_file, pdm_well, well_name)
            except:
                print("Alias should be defined in " + pdm_names_file)

        well_names.append(well_name)

    return well_names


## Main program
def main():
    parser = argparse.ArgumentParser(description="Extract production data")
    parser.add_argument("well_directory", help="Enter path to the main well folder")
    parser.add_argument(
        "production_file", help="Enter path to a file with the daily production volumes"
    )
    parser.add_argument("fmu_directory", help="Enter path to the FMU case folder")

    args = parser.parse_args()

    well_directory = args.well_directory
    production_file = args.production_file
    fmu_directory = args.fmu_directory

    print(well_directory, production_file, fmu_directory)

    wellbore_info_file = "wellbore_info.csv"
    delimiter = "--"
    map_suffix = ".gri"

    well_info = pd.read_csv(os.path.join(well_directory, wellbore_info_file))
    # print(well_info)

    prod_data = pd.read_csv(production_file)
    prod_file_update = os.path.getmtime(production_file)

    print(prod_data)

    prod_data_wells = prod_data["WELL_BORE_CODE"].unique()

    sorted_production_wells = common.sort_wellbores(prod_data_wells)
    pdm_names_file = os.path.join(well_directory, "wrong_pdm_well_names.csv")
    all_well_names = check_production_wells(
        sorted_production_wells, well_info, pdm_names_file
    )

    dates_4d = common.all_interval_dates(fmu_directory, delimiter, map_suffix)
    print(dates_4d)
    # print(len(dates_4d))

    volume_codes = [
        "BORE_OIL_VOL",
        "BORE_GAS_VOL",
        "BORE_WAT_VOL",
        "BORE_GI_VOL",
        "BORE_WI_VOL",
    ]

    for volume_code in volume_codes:
        pdm_names = []
        well_names = []
        start_dates = []
        stop_dates = []
        intervals = []
        volumes = np.zeros((len(sorted_production_wells), len(dates_4d) + 2))

        print(volume_code)
        volume_df = pd.DataFrame()

        index = 0
        for pdm_well in sorted_production_wells:
            print(pdm_well)
            well_prod_data = prod_data[prod_data["WELL_BORE_CODE"] == pdm_well]
            well_prod_data = well_prod_data[["WELL_BORE_CODE", "DATEPRD", volume_code]]
            # print(well_prod_data)
            start_date, stop_date = get_start_stop_dates(
                well_prod_data, prod_file_update, volume_code
            )

            pdm_names.append(pdm_well)
            well_names.append(all_well_names[index])
            start_dates.append(start_date)
            stop_dates.append(stop_date)

            for i in range(0, len(dates_4d) - 1):
                intervals.append(dates_4d[i] + "-" + dates_4d[i + 1])

                volumes[index, i] = well_prod_data.loc[
                    (well_prod_data["DATEPRD"] >= dates_4d[i])
                    & (well_prod_data["DATEPRD"] < dates_4d[i + 1]),
                    volume_code,
                ].sum()
                # print(pdm_well, intervals[i], volumes[index, i])

            last_interval = dates_4d[i + 1] + "-now"
            intervals.append(last_interval)
            volumes[index, i + 1] = well_prod_data.loc[
                (well_prod_data["DATEPRD"] >= dates_4d[i + 1]), volume_code,
            ].sum()

            all_intervals = dates_4d[0] + "-now"
            intervals.append(all_intervals)
            volumes[index, i + 2] = well_prod_data.loc[
                (well_prod_data["DATEPRD"] >= dates_4d[0]), volume_code,
            ].sum()
            # print("")

            index = index + 1

        volume_df["PDM well name"] = pdm_names
        volume_df["Well name"] = well_names
        volume_df["Start date"] = start_dates
        volume_df["Stop date"] = stop_dates

        pd.set_option("display.max_columns", None)
        pd.set_option("display.max_rows", None)

        for i in range(0, len(dates_4d) + 1):
            volume_df[intervals[i]] = volumes[:, i]
            print(i, intervals[i])
            print(volume_df["1993-01-01-2005-07-01"])

        volume_df_actual = volume_df[volume_df[all_intervals] > 0]

        print(volume_df)

        csv_file = os.path.join(well_directory, volume_code + ".csv")
        volume_df_actual.to_csv(csv_file, index=False)

        print("Data exported to file " + csv_file)


if __name__ == "__main__":
    main()
