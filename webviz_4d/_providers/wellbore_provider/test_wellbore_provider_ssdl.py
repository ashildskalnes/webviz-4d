import os
import time
import argparse

from webviz_4d._providers.wellbore_provider._provider_impl_file import (
    ProviderImplFile,
)


def main():
    description = "Testing some SSDL data methods"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("field_name", help="Enter field name")
    args = parser.parse_args()
    print(args)

    field = args.field_name.upper()
    omnia_env = ".omniaapi"
    home = os.path.expanduser("~")
    env_path = os.path.expanduser(os.path.join(home, omnia_env))

    smda_provider = ProviderImplFile(env_path, "SMDA")
    ssdl_provider = ProviderImplFile(env_path, "SSDL")
    pdm_provider = ProviderImplFile(env_path, "PDM")

    # Get field_uuid from SMDA
    field_uuid = smda_provider.get_field_uuid(field)

    # Get fault lines for selected field
    print("\nGet fault lines for selected field ...")
    start = time.time()
    fault_lines = ssdl_provider.get_faultlines(
        field_uuid=field_uuid,
    )
    end = time.time()
    print("Extract fault lines:", end - start)

    if fault_lines:
        print(fault_lines.dataframe)

    # Get outlines for selected field
    print("\nGet field outlines for selected field ...")
    start = time.time()
    outlines = ssdl_provider.get_field_outlines(
        field_uuid=field_uuid,
    )
    end = time.time()
    print("Extract field outlines:", end - start)

    if fault_lines:
        print(outlines.dataframe)

    # Get completion data for selected wellbores
    print("\nGet completion data for all wellbores ...")
    pdm_dates = pdm_provider.get_pdm_wellbores(field_name=field)

    if not pdm_dates.dataframe.empty:
        wellbore_names = [pdm_dates.dataframe.iloc[0, 0]]
        start = time.time()
        completions = ssdl_provider.get_wellbore_completions(field_uuid)
        end = time.time()
        print("Extract completions:", end - start)

        if completions:
            print(completions.dataframe)

    # Get perforation data for alle wellbores
    print("\nGet perforation data for all wellbores ...")
    wellbore_names = None
    start = time.time()
    perforations = ssdl_provider.get_wellbore_perforations(
        field_uuid=field_uuid,
    )
    end = time.time()
    print("Extract perforations:", end - start)

    if perforations:
        print(perforations.dataframe)


if __name__ == "__main__":
    main()
