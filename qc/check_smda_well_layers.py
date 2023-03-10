import os
import argparse
import pandas as pd

from fmu.sumo.explorer import Explorer

from webviz_4d._datainput._surface import make_surface_layer, load_surface
from webviz_4d._datainput.common import (
    read_config,
    get_well_colors,
    get_update_dates,
    get_plot_label,
    get_dates,
    get_default_interval,
)

from webviz_4d._datainput.well import (
    load_all_wells,
    load_smda_metadata,
    load_smda_wellbores,
    load_planned_wells,
    load_pdm_info,
    create_well_layer,
    create_well_layers,
    get_surface_picks,
)

from webviz_4d_input._providers.wellbore_provider._provider_impl_file import (
    ProviderImplFile,
)

from webviz_4d._datainput._sumo import (
    create_selector_lists,
    get_observed_surface,
    get_aggregated_surface,
    get_realization_surface,
    get_polygon_name,
)


def get_sumo_top_res_surface(my_case, shared_settings):
    top_res_surface = shared_settings.get("top_res_surface")
    iter_name = my_case.iterations[0].get("name")

    if top_res_surface is not None:
        name = top_res_surface.get("name")
        tagname = top_res_surface.get("tag_name")
        time_interval = [False, False]

        surface = get_realization_surface(
            case=my_case,
            surface_name=name,
            attribute=tagname,
            time_interval=time_interval,
            iteration_name=iter_name,
        )

    if surface:
        return surface.to_regular_surface()
    else:
        print(
            "ERROR: Top reservoir surface not loaded from SUMO:",
            name,
            tagname,
            time_interval,
        )
        return None


def open_surface_with_xtgeo(surface):
    if surface:
        surface_object = surface.to_regular_surface()
    else:
        surface_object = None
        print("ERROR: non-existing surface")

    return surface_object


def main():
    description = "Test well layers based on well data from POZO and SMDA"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("config_file")

    args = parser.parse_args()

    config_file = args.config_file
    config_file = os.path.abspath(config_file)
    config_folder = os.path.dirname(config_file)

    config = read_config(config_file)

    settings_file = config.get("shared_settings").get("settings")
    settings_file = os.path.join(config_folder, settings_file)
    settings = read_config(settings_file)

    field_name = settings.get("field_name")[0]
    shared_settings = config.get("shared_settings")
    sumo_name = shared_settings.get("sumo_name")

    well_colors = get_well_colors(settings)
    basic_well_layers = shared_settings.get("basic_well_layers", None)

    env = "prod"
    sumo = Explorer(env=env)
    my_case = sumo.cases.filter(name=sumo_name)[0]
    print("SUMO case:", my_case.name)

    top_res_surface = get_sumo_top_res_surface(my_case, shared_settings)

    omnia_env = ".omniaapi"
    home = os.path.expanduser("~")
    env_path = os.path.expanduser(os.path.join(home, omnia_env))
    smda_provider = ProviderImplFile(env_path, "SMDA")
    pozo_provider = ProviderImplFile(env_path, "POZO")
    pdm_provider = ProviderImplFile(env_path, "PDM")

    print("Loading drilled well data from SMDA ...")
    drilled_wells_info = load_smda_metadata(smda_provider, field_name)
    print(drilled_wells_info)

    drilled_wells_df = load_smda_wellbores(smda_provider, field_name)
    # print(drilled_wells_df)

    surface_picks = get_surface_picks(drilled_wells_df, top_res_surface)
    print("SMDA surface_picks:")
    print(surface_picks)

    if "planned" in basic_well_layers:
        print("Loading planned well data from POZO ...")
        planned_wells = load_planned_wells(pozo_provider, field_name)
        planned_wells_info = planned_wells.metadata.dataframe
        planned_wells_df = planned_wells.trajectories.dataframe
        print(planned_wells_info)

    well_basic_layers = create_well_layers(
        basic_well_layers,
        planned_wells_info,
        planned_wells_df,
        drilled_wells_info,
        drilled_wells_df,
        surface_picks,
        well_colors,
    )

    print("Basic well layers")
    for layer in well_basic_layers:
        data = layer.get("data")

        print("  ", layer.get("name"))

        for well in data:
            tooltip = well.get("tooltip")
            print("  ", tooltip)

    selectors = create_selector_lists(my_case, "timelapse")
    map_types = ["observed", "simulated"]

    # Load production data
    print("Loading production/injection data from PDM ...")
    default_interval = get_default_interval(selection_list=selectors, options=map_types)

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

    # Create addition well layers
    additional_well_layers = shared_settings.get("additional_well_layers")
    interval = default_interval

    print("Additional well layers")
    for key, value in additional_well_layers.items():
        layer_name = key
        label = value
        print("  Layer name", layer_name)
        color = well_colors.get(layer_name, None)

        if color is None:
            color = well_colors.get("default", None)

        well_layer = create_well_layer(
            interval_4d=interval,
            metadata_df=drilled_wells_info,
            trajectories_df=drilled_wells_df,
            surface_picks=surface_picks,
            prod_data=prod_data,
            color_settings=None,
            layer_name=key,
            label=value,
        )

        data = well_layer.get("data")

        print("  ", well_layer.get("name"))

        for well in data:
            tooltip = well.get("tooltip")
            print("  ", tooltip)


if __name__ == "__main__":
    main()
