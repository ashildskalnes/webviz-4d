import os
import polars as pl
import polars.selectors as cs
import argparse

from fmu.sumo.explorer import Explorer

from webviz_4d._datainput.common import read_config, print_layer_tooltip
from webviz_4d._providers.wellbore_provider._provider_impl_file import (
    ProviderImplFile,
)
from webviz_4d._datainput.well import (
    load_smda_metadata,
    load_smda_wellbores,
    get_short_wellname,
)
from webviz_4d._datainput._sumo import get_sumo_metadata
from webviz_4d._datainput._eclipse import (
    create_eclipse_production_layers,
    print_all,
    load_eclipse_prod_data,
)


# Main program
def main():
    DESCRIPTION = "Check SUMO production layers"
    parser = argparse.ArgumentParser(description=DESCRIPTION)
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
    field_name = shared_settings.get("field_name")

    # Load wellbores from SMDA
    omnia_env = ".omniaapi"
    home = os.path.expanduser("~")
    env_path = os.path.expanduser(os.path.join(home, omnia_env))
    smda_provider = ProviderImplFile(env_path, "SMDA")
    drilled_wells_info = load_smda_metadata(smda_provider, field_name)
    drilled_wells = load_smda_wellbores(smda_provider, field_name)

    short_names = []

    for indx, row in drilled_wells_info.iterrows():
        well_name = row["unique_wellbore_identifier"]
        short_name = get_short_wellname(well_name)
        short_names.append(short_name)

    drilled_wells_info["Wellbore short name"] = short_names

    sumo_settings = shared_settings.get("sumo")
    env_name = sumo_settings.get("env_name")
    case_name = sumo_settings.get("case_name")

    print()
    print("Searching for seismic 4D attribute in sumo case:", case_name, " ...")

    sumo = Explorer(env=env_name)
    cases = sumo.cases.filter(name=case_name)

    if len(cases) > 1:
        print("WARNING: number of cases", len(cases))
        for case in cases:
            if len(case.surfaces) > 0:
                sumo_case = case
    else:
        sumo_case = cases[0]

    # Some case info
    print(f"{sumo_case.name}: {sumo_case.uuid}")
    print(sumo_case.field)
    print(sumo_case.status)
    print(sumo_case.user)

    metadata, selection_list = get_sumo_metadata(sumo_case)

    first_date = min(metadata["time1"].to_list())
    dates_4d = [first_date] + sorted(metadata["time2"].unique())

    shared_settings = config.get("shared_settings")
    interval_mode = shared_settings.get("interval_mode")

    sumo_settings = shared_settings.get("sumo")
    env_name = sumo_settings.get("env_name")
    case_name = sumo_settings.get("case_name")

    print("Loading production info from SUMO")
    iteration = [it.name for it in sumo_case.iterations][0]
    print("iteration=", iteration)

    tables = sumo_case.tables.filter(
        iteration=iteration, realization=0, tagname="summary"
    )
    table = tables[0]
    ecl_table = table.to_arrow()
    summary_df = pl.from_arrow(ecl_table)
    print("\nEclipse vector")
    print(summary_df)
    print()

    ecl_keywords = ["WOPTH", "WGPTH", "WWPTH"]

    prod_data_list = load_eclipse_prod_data(
        summary_df, ecl_keywords, drilled_wells_info, dates_4d
    )

    for index, ecl_key in enumerate(ecl_keywords):
        ecl_polars_table = prod_data_list[index]

        selected_headers = [
            "ECL name",
            "unique_wellbore_identifier",
            "Start date",
        ] + ecl_polars_table.columns[5:]

        start_date_volumes = ecl_polars_table.filter(pl.col("Start date") != "")

        print("\n" + ecl_key)
        print("Extracted start dates")
        print_all(start_date_volumes.select(selected_headers))
        print()

    default_interval = "2023-09-15-2019-10-01"

    surface_picks = None
    additional_well_layers = shared_settings.get("additional_well_layers")

    well_colors = settings.get("well_colors")

    interval_well_layers = create_eclipse_production_layers(
        prod_data_list=prod_data_list,
        interval_4d=default_interval,
        wellbore_trajectories=drilled_wells,
        surface_picks=surface_picks,
        layer_options=additional_well_layers,
        well_colors=well_colors,
    )

    print("4D interval:", default_interval)

    for layer in interval_well_layers:
        print_layer_tooltip(layer)


if __name__ == "__main__":
    main()
