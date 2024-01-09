import os
import time
import argparse

import webviz_4d._providers.wellbore_provider._provider_impl_file as wb


def main():
    description = "Testing planned wells"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("field_name", help="Enter field name")
    args = parser.parse_args()
    print(args)

    field = args.field_name.upper()
    omnia_env = ".omniaapi"
    home = os.path.expanduser("~")
    env_path = os.path.expanduser(os.path.join(home, omnia_env))

    pozo_provider = wb.ProviderImplFile(env_path, "POZO")

    # Get planned wellbores
    start = time.time()
    planned_wellbores = pozo_provider.get_planned_wellbores(field_name=field)
    end = time.time()
    print("Get planned wellbores:", end - start)
    if planned_wellbores.metadata:
        print(planned_wellbores.metadata)
        end = time.time()
    else:
        print("ERROR: Not able to find any planned wells for", field)


if __name__ == "__main__":
    main()
