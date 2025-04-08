import os
import pandas as pd
import time
from datetime import datetime
import argparse

import webviz_4d._providers.wellbore_provider._provider_impl_file as wb


def main():
    description = "Testing some production methods"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("field_name", help="Enter field name")
    args = parser.parse_args()
    print(args)

    field = args.field_name.upper()
    omnia_env = ".omniaapi"
    home = os.path.expanduser("~")
    env_path = os.path.expanduser(os.path.join(home, omnia_env))

    pdm_provider = wb.ProviderImplFile(env_path, "PDM")
    pd.set_option("display.max_rows", 10)

    # Get official start and last dates for all pdm wellbores
    print("Get official start and last dates for all pdm wellbores ..")
    start = time.time()
    all_pdm_dates = pdm_provider.get_pdm_wellbores(field_name=field)
    pdm_dates_df = all_pdm_dates.dataframe.sort_values(by="WB_UWBI")
    end = time.time()
    print("Extracted PDM dates:", end - start)
    print(pdm_dates_df)
    print("PDM Wellbores:", len(pdm_dates_df))

    # Get all wellbores within a given 4D interval
    start_date = "1995-01-01"
    now_str = datetime.today().strftime("%Y-%m-%d")
    end_dates = ["2020-10-01", "2023-10-01", now_str]
    wellbores = []

    for end_date in end_dates:
        interval = end_date + "-" + start_date
        print(
            "\nGet all production data within a given 4D interval:" + interval + " ..."
        )
        start = time.time()
        production_volumes = pdm_provider.get_field_prod_data(
            field_name=field,
            start_date=start_date,
            end_date=end_date,
        )

        end = time.time()
        print("Extracted production volumes:", end - start)
        print("4D interval:", interval)
        print(production_volumes.dataframe.sort_values(by="WB_UWBI"))
        print("PDM wellbores:", len(production_volumes.dataframe))

        print("\nGet all injection data within a given 4D interval ..")
        start = time.time()
        injection_volumes = pdm_provider.get_field_inj_data(
            field_name=field,
            start_date=start_date,
            end_date=end_date,
        )

        end = time.time()
        print("Extracted injection volumes:", end - start)
        interval = end_date + "-" + start_date
        print("4D interval:", interval)
        print(injection_volumes.dataframe.sort_values(by="WB_UWBI"))
        print("PDM wellbores:", len(injection_volumes.dataframe))


if __name__ == "__main__":
    main()
