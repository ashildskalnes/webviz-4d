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

    # Get start and last dates for all pdm wellbores
    wellbores = []
    print("\nGet start and last dates for all pdm wellbores ..")
    start = time.time()
    pdm_dates = pdm_provider.get_pdm_wellbores(field_name=field)
    end = time.time()
    print("Get PDM dates:", end - start)
    if not pdm_dates.dataframe.empty:
        print(pdm_dates.dataframe)
        all_wellbores = pdm_dates.dataframe["WB_UWBI"].values.tolist()
        # Get start and last dates for selected pdm wellbores
        print("\nGet start and last dates for selected pdm wellbores ..")
        wellbores = [pdm_dates.dataframe.iloc[0, 0]]
        print(wellbores)
        start = time.time()
        pdm_dates = pdm_provider.get_pdm_wellbores(
            field_name=field, pdm_wellbores=wellbores
        )
        end = time.time()
        print("Get PDM dates:", end - start)
        print(pdm_dates.dataframe)
    else:
        print("ERROR: Not able to find field_uuid for field:", field)

    # Get production volumes for selected well(s)
    print("\nGet production volumes for selected well(s) ...")
    start = time.time()
    production_volumes = pdm_provider.get_production_volumes(
        field_name=field,
        wellbore_names=wellbores,
    )
    end = time.time()
    print("Extract production data selected (daily):", end - start)
    if production_volumes:
        print(production_volumes.dataframe)

    # Get production volumes for selected well(s)
    print("\nGet production volumes for selected well(s) ...")
    start = time.time()
    production_volumes = pdm_provider.get_production_volumes(
        field_name=field,
        wellbore_names=wellbores,
    )
    end = time.time()

    print("Extract production data (monthly):", end - start)
    if production_volumes:
        print(production_volumes.dataframe)

    # Get production volumes for a selected field (daily)
    print("\nGet production volumes for a selected field (daily)...")
    start = time.time()
    production_volumes = pdm_provider.get_production_volumes(field_name=field)
    end = time.time()
    print("Extract production data:", end - start)
    if production_volumes:
        print(production_volumes.dataframe)

        print("Extract cumulative values for a time period - daily")
        prod_info = pdm_provider.get_prod_data(
            production_volumes,
            "2019-10-01",
            "2020-10-01",
            all_wellbores,
        )
        print(prod_info.dataframe)

    # Get production volumes for a selected field (monthly)
    print("\nGet production volumes for a selected field (monthly)...")
    start = time.time()
    production_volumes = pdm_provider.get_production_volumes(
        field_name=field, interval="Month"
    )
    end = time.time()
    print("Extract production data:", end - start)
    if production_volumes:
        print(production_volumes.dataframe)

        print("Extract cumulative values for a time period - monthly")
        prod_info = pdm_provider.get_prod_data(
            production_volumes,
            "2019-10-01",
            "2020-10-01",
            all_wellbores,
            interval="Month",
        )
        print(prod_info.dataframe)

    # Get injection volumes for selected well(s)
    print("\nGet injection volumes for selected well(s) ...")
    start = time.time()
    injection_volumes = pdm_provider.get_injection_volumes(
        field_name=field,
        wellbore_names=wellbores,
    )
    end = time.time()
    print("Extract injection data:", end - start)
    if injection_volumes:
        print(injection_volumes.dataframe)

    # Get injection volumes for a selected field
    print("\nGet injection volumes for a selected field ...")
    start = time.time()
    injection_volumes = pdm_provider.get_injection_volumes(field_name=field)
    end = time.time()
    print("Extract injection data:", end - start)
    if injection_volumes:
        print(injection_volumes.dataframe)

    # Get injection volumes for a selected field and a selected start date
    print("\nGet injection volumes for a selected field and start date ...")
    start_date = "2022-09-18"
    start = time.time()
    injection_volumes = pdm_provider.get_injection_volumes(
        field_name=field,
        start_date=start_date,
    )
    end = time.time()
    print("Extract injection data:", end - start)
    if injection_volumes:
        print(injection_volumes.dataframe)

    # Get injection volumes for a selected field and a selected time period
    print("\nGet injection volumes for a selected field and time period...")
    start_date = "2022-09-18"
    end_date = "2022-09-20"
    start = time.time()
    injection_volumes = pdm_provider.get_injection_volumes(
        field_name=field,
        start_date=start_date,
        end_date=end_date,
    )
    end = time.time()
    print("Extract injection data:", end - start)
    print(start_date, end_date)
    if injection_volumes:
        print(injection_volumes.dataframe)


if __name__ == "__main__":
    main()
