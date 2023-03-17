import os
import time
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

    print("Field:", field)

    # Get start and last dates for all pdm wellbores
    print("\nGet start and last dates for all pdm wellbores ..")
    start = time.time()
    pdm_dates = pdm_provider.get_pdm_wellbores(field_name=field)
    end = time.time()
    print("Get PDM dates:", end - start)
    if pdm_dates:
        print(pdm_dates.dataframe)

    # Get production volumes for a selected field
    print("\nGet production volumes for a selected field ...")
    first_date = "2019-10-01"
    last_date = "2020-10-01"
    start = time.time()
    production_volumes = pdm_provider.get_production_volumes(field_name=field)
    end = time.time()
    print("Extract production data:", end - start)
    if production_volumes:
        print(production_volumes.dataframe)
        print("\nExtract cumulative values for all wells for a time period")

        print("Time interval:", first_date, "-", last_date)
        prod_info = pdm_provider.get_prod_data(
            production_volumes, first_date, last_date
        )

        if not prod_info.dataframe.empty:
            prod_info.dataframe.sort_values(by=["WB_UWBI"], inplace=True)
            print(prod_info.dataframe[["WB_UWBI", "OIL_VOL", "GAS_VOL", "WATER_VOL"]])

    # Get injection volumes for a selected field
    print("\nGet injection volumes for a selected field ...")
    start = time.time()
    injection_volumes = pdm_provider.get_injection_volumes(field_name=field)
    end = time.time()
    print("Extract injection data:", end - start)
    if not injection_volumes.dataframe.empty:
        print(injection_volumes.dataframe)
        print("\nExtract cumulative values for all wells for a time period")
        print("Time interval:", first_date, "-", last_date)
        start = time.time()
        inj_info = pdm_provider.get_inj_data(injection_volumes, first_date, last_date)
        end = time.time()
        print("Extract injection data:", end - start)
        inj_info.dataframe.sort_values(by=["WB_UWBI"], inplace=True)
        print(inj_info.dataframe[["WB_UWBI", "GI_VOL", "WI_VOL"]])


if __name__ == "__main__":
    main()
