import os
import argparse

from webviz_4d._datainput.common import (
    read_config,
)
from webviz_4d._datainput.well import load_planned_wells
from webviz_4d._providers.wellbore_provider._provider_impl_file import (
    ProviderImplFile,
)


def main():
    description = "Test getting well data from POZO"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("config_file")

    args = parser.parse_args()

    config_file = args.config_file
    config_file = os.path.abspath(config_file)
    config_folder = os.path.dirname(config_file)

    config = read_config(config_file)

    settings_file = config.get("shared_settings").get("settings_file")
    settings_file = os.path.join(config_folder, settings_file)
    settings = read_config(settings_file)

    shared_settings = config.get("shared_settings")
    field_name = shared_settings.get("field_name").upper()

    omnia_env = ".omniaapi"
    home = os.path.expanduser("~")
    env_path = os.path.expanduser(os.path.join(home, omnia_env))
    pozo_provider = ProviderImplFile(env_path, "POZO")

    planned_wells = load_planned_wells(pozo_provider, field_name)

    print(planned_wells)


if __name__ == "__main__":
    main()
