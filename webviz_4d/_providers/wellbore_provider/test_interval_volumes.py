import os
import time
import argparse
import pandas as pd

import webviz_4d._providers.wellbore_provider._provider_impl_file as wb


def main():
    description = "Testing some production methods"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("field_name", help="Enter field name")
    parser.add_argument("start_date", help="Enter start date (example: 1995-01-01")
    parser.add_argument("end_date", help="Enter end date (example: 2022-10-15")
    parser.add_argument("prod_interval", help="Enter either DAY or MONTH")
    args = parser.parse_args()
    print(args)

    field = args.field_name.upper()
    start_date = args.start_date
    end_date = args.end_date
    prod_interval = args.prod_interval
    omnia_env = ".omniaapi"
    home = os.path.expanduser("~")
    env_path = os.path.expanduser(os.path.join(home, omnia_env))

    pdm_provider = wb.ProviderImplFile(env_path, "PDM")

    # Get all production/injection wellbores
    if prod_interval == "Month":
        pdm_data = pdm_provider.get_pdm_wellbores(field_name=field)
        field_uuid = pdm_data.dataframe["FIELD_UUID"][0]
        print(field_uuid)
    else:
        field_uuid = None

    # Daily volumes
    print(prod_interval, "extraction:", start_date, end_date)
    start = time.time()
    production_volumes = pdm_provider.get_field_prod_data(
        field_name=field,
        start_date=start_date,
        end_date=end_date,
        interval=prod_interval,
        field_uuid=field_uuid,
    )

    prod_df = production_volumes.dataframe
    print(prod_df.sort_values(by="WB_UWBI"))

    # Daily volumes
    print(prod_interval, "extraction:", start_date, end_date)
    injection_volumes = pdm_provider.get_field_inj_data(
        field_name=field,
        start_date=start_date,
        end_date=end_date,
        interval=prod_interval,
        field_uuid=field_uuid,
    )
    inj_df = injection_volumes.dataframe
    print(inj_df.sort_values(by="WB_UWBI"))

    # Combined production and injection volumes
    print("Combined volumes", start_date, end_date)
    pdm_volumes = pd.merge(
        prod_df,
        inj_df,
        how="outer",
    )
    end = time.time()
    print(end - start)
    print(pdm_volumes.sort_values(by="WB_UWBI"))


if __name__ == "__main__":
    main()
