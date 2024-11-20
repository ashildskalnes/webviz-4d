import os
import argparse
import pandas as pd
import xtgeo
from pathlib import Path
from pprint import pprint

from fmu.sumo.explorer import Explorer

from webviz_4d._datainput.common import (
    read_config,
    get_well_colors,
    get_default_interval,
)

from webviz_4d._datainput.well import (
    load_smda_metadata,
    load_smda_wellbores,
    load_planned_wells,
    create_basic_well_layers,
    create_pdm_well_layer,
    create_pdm_well_layer,
    get_surface_picks,
    create_production_layers,
)

from webviz_4d._providers.wellbore_provider._provider_impl_file import (
    ProviderImplFile,
)

from webviz_4d._datainput._sumo import (
    create_selector_lists,
    get_realization_surface,
    get_aggregated_surface,
)

import webviz_4d._providers.wellbore_provider.wellbore_provider as wb

import warnings

warnings.filterwarnings("ignore")


def get_path(path) -> Path:
    return Path(path)


def get_sumo_top_res_surface(sumo_case, surface_info):
    surface = None

    if surface_info is not None:
        name = surface_info.get("name")
        tagname = surface_info.get("tag_name")
        iter_name = surface_info.get("iter")
        real = surface_info.get("real")
        time_interval = [False, False]

        print("Load top reservoir surface from SUMO:", name, tagname, iter_name, real)

        if "realization" in real:
            real_id = real.split("-")[1]
            surface = get_realization_surface(
                case=sumo_case,
                surface_name=name,
                attribute=tagname,
                time_interval=time_interval,
                iteration_name=iter_name,
                realization=real_id,
            )
        else:
            surface = get_aggregated_surface(
                case=sumo_case,
                surface_name=name,
                attribute=tagname,
                time_interval=time_interval,
                iteration_name=iter_name,
                operation=real,
            )

    if surface:
        return surface.to_regular_surface()
    else:
        print(
            "ERROR: Top reservoir surface not loaded from SUMO",
        )
        return None


def get_top_res_surface(surface_info, sumo_case):
    if sumo_case:
        surface = get_sumo_top_res_surface(sumo_case, surface_info)
    else:
        surface = get_top_res_surface_file(surface_info)

    return surface


def get_top_res_surface_file(surface_info):
    if surface_info is not None:
        name = surface_info.get("file_name")
        print("Load top reservoir surface:", name)

        if os.path.isfile(name):
            return xtgeo.surface_from_file(name)
        else:
            print("ERROR: File not found")
            return None
    else:
        print("WARNING: Top reservoir surface not defined")
        return None


def main():
    description = "Test well layers based on well data from SMDA"
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
    field_name = shared_settings.get("field_name")
    sumo_settings = shared_settings.get("sumo")
    sumo_name = sumo_settings.get("case_name")

    well_colors = get_well_colors(settings)
    basic_well_layers = shared_settings.get("basic_well_layers", None)

    env = "prod"
    sumo = Explorer(env=env)
    my_case = sumo.cases.filter(name=sumo_name)[0]
    print("SUMO case:", my_case.name)

    top_res_surface_info = shared_settings.get("top_reservoir")
    top_res_name = top_res_surface_info.get("name")
    top_res_tag = top_res_surface_info.get("tag_name")
    print("Top reservoir surface:", top_res_name, top_res_tag)
    top_res_surface = get_top_res_surface(top_res_surface_info, my_case)

    omnia_env = ".omniaapi"
    home = os.path.expanduser("~")
    env_path = os.path.expanduser(os.path.join(home, omnia_env))
    smda_provider = ProviderImplFile(env_path, "SMDA")
    pdm_provider = ProviderImplFile(env_path, "PDM")

    print("Loading drilled well data from SMDA ...")
    drilled_wells_info = load_smda_metadata(smda_provider, field_name)
    # print(drilled_wells_info)

    drilled_wells_df = load_smda_wellbores(smda_provider, field_name)
    # print(drilled_wells_df)

    surface_picks = get_surface_picks(drilled_wells_df, top_res_surface)

    if "planned" in basic_well_layers:
        planned_wells_info = smda_provider.planned_wellbore_metadata(
            field=field_name,
        )
        planned_wells_df = smda_provider.planned_trajectories(
            planned_wells_info.dataframe,
        )
    else:
        planned_wells_info = wb.PlannedWellboreMetadata(pd.DataFrame())
        planned_wells_df = wb.Trajectories(
            coordinate_system="", dataframe=pd.DataFrame()
        )

    well_basic_layers = create_basic_well_layers(
        basic_well_layers,
        planned_wells_info.dataframe,
        planned_wells_df.dataframe,
        drilled_wells_info,
        drilled_wells_df,
        surface_picks,
        well_colors,
    )

    # print("Basic well layers")
    # for layer in well_basic_layers:
    #     data = layer.get("data")
    #     print("  ", layer.get("name"), len(data))

    #     for well in data:
    #         tooltip = well.get("tooltip")
    #         if layer == "planned":
    #             print("    ", tooltip)
    #     print(layer.get("data")[0].get("tooltip"))
    #     pprint(layer.get("data")[0].get("positions"))

    selectors = create_selector_lists(my_case, "timelapse")
    map_types = ["observed", "simulated"]

    # Load production data
    print("Loading production/injection data from PDM ...")
    default_interval = get_default_interval(selection_list=selectors, options=map_types)
    additional_well_layers = shared_settings.get("additional_well_layers")

    well_additional_layers = create_production_layers(
        field_name=field_name,
        pdm_provider=pdm_provider,
        interval_4d=default_interval,
        wellbore_trajectories=drilled_wells_df,
        surface_picks=surface_picks,
        layer_options=additional_well_layers,
        well_colors=well_colors,
    )

    prod_data = pdm_provider.get_field_prod_data(
        field_name=field_name,
        start_date=default_interval[-10:],
        end_date=default_interval[:10],
    )

    inj_data = pdm_provider.get_field_inj_data(
        field_name=field_name,
        start_date=default_interval[-10:],
        end_date=default_interval[:10],
    )
    volumes_df = pd.merge(
        prod_data.dataframe,
        inj_data.dataframe,
        how="outer",
    )
    prod_data = volumes_df

    pdm_wellbores = prod_data["WB_UWBI"].tolist()
    pdm_wells_df = drilled_wells_df[
        drilled_wells_df["unique_wellbore_identifier"].isin(pdm_wellbores)
    ]

    # Create addition well layers
    additional_well_layers = shared_settings.get("additional_well_layers")
    interval = default_interval

    well_additional_layers = []

    print("Additional well layers")
    for key, value in additional_well_layers.items():
        layer_name = key
        color = well_colors.get(layer_name, None)
        print("  ", key)

        if color is None:
            color = well_colors.get("default", None)

        well_layer = create_pdm_well_layer(
            interval_4d=interval,
            metadata_df=prod_data,
            trajectories_df=pdm_wells_df,
            surface_picks=surface_picks,
            prod_data=prod_data,
            color_settings=None,
            layer_name=key,
            label=value,
            uwi="WB_UWBI",
        )

        if well_layer:
            well_additional_layers.append(well_layer)

    for layer in well_additional_layers:
        data = layer.get("data")
        print("  ", layer.get("name"), len(data))


if __name__ == "__main__":
    main()
