import os
import glob
import argparse
import json
import numpy as np
import pandas as pd
from pprint import pprint

from webviz_4d._datainput.common import (
    read_config,
)
from webviz_4d._providers.wellbore_provider._provider_impl_file import (
    ProviderImplFile,
)
from webviz_4d._datainput.well import (
    load_all_wells,
    load_smda_metadata,
    load_smda_wellbores,
    load_planned_wells,
    load_pdm_info,
    create_basic_well_layers,
    get_surface_picks,
    create_production_layers,
)


def main():
    """Check production data"""
    description = "Compile metadata for all auto4d maps"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("config_file", help="Enter path to the configuration file")

    args = parser.parse_args()
    config_file = args.config_file

    config = read_config(config_file)
    config_file = os.path.abspath(config_file)
    config_folder = os.path.dirname(config_file)
    config_folder = os.path.abspath(config_folder)

    omnia_env = ".omniaapi"
    home = os.path.expanduser("~")
    env_path = os.path.expanduser(os.path.join(home, omnia_env))
    smda_provider = ProviderImplFile(env_path, "SMDA")
    pdm_provider = ProviderImplFile(env_path, "PDM")

    field_name = "JOHAN SVERDRUP"

    print("Loading drilled well data from SMDA ...")
    shared_settings = config.get("shared_settings")
    default_interval = shared_settings.get("default_interval")
    drilled_wells_info = load_smda_metadata(smda_provider, field_name)
    print(drilled_wells_info)

    drilled_wells_df = load_smda_wellbores(smda_provider, field_name)

    pdm_wells_info = load_pdm_info(pdm_provider, field_name)
    pdm_wellbores = pdm_wells_info["WB_UWBI"].tolist()
    pdm_wells_df = drilled_wells_df[
        drilled_wells_df["unique_wellbore_identifier"].isin(pdm_wellbores)
    ]

    layer_options = shared_settings.get("additional_well_layers")

    interval_well_layers = create_production_layers(
        field_name=field_name,
        pdm_provider=pdm_provider,
        interval_4d=default_interval,
        wellbore_trajectories=drilled_wells_df,
        layer_options=layer_options,
        prod_interval="Day",
    )

    print(interval_well_layers)


if __name__ == "__main__":
    main()
