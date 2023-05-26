import pandas as pd
import argparse
import logging
from fmu.sumo.explorer import Explorer
from webviz_4d._datainput._sumo import (
    decode_time_interval,
    get_observed_surface,
    get_realization_surface,
    get_aggregated_surface,
    print_sumo_objects,
)

import sumo.wrapper


def main():
    logging.getLogger("").setLevel(level=logging.WARNING)
    description = "Compile metadata for SUMO seismic_cubes"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("sumo_name")
    args = parser.parse_args()

    sumo_name = args.sumo_name

    sumo_wrapper = sumo.wrapper.SumoClient("prod")
    cubes = sumo_wrapper.get(f"/search", query="data.format:openvds")
    print(
        "Number of openvds formatted objects I have access to: ",
        len(cubes.get("hits").get("hits")),
    )

    sumo = Explorer(env="prod")

    my_case = sumo.cases.filter(name=sumo_name)[0]
    print(f"{my_case.name}: {my_case.uuid}")

    # Some case info
    print(my_case.field)
    print(my_case.status)
    print(my_case.user)

    # Get all observed seismic cubes in a case
    seismic_type = "observed"
    print(seismic_type)

    # seismic_cubes = my_case.cubes.filter(stage="case")
    print_sumo_objects(seismic_cubes)

    # Get sumo instance for one observed seismic
    if len(seismic_cubes) > 0:
        seismic = seismic_cubes[0]
        selected_time = seismic._metadata.get("data").get("time")

        selected_surfaces = my_case.seismic_cubes.filter(
            stage="case", name=seismic.name, tagname=seismic.tagname
        )

        for seismic in selected_surfaces:
            if seismic._metadata.get("data").get("time") == selected_time:
                time_list = decode_time_interval(selected_time)

                print(
                    seismic_type,
                    "seismic:",
                    seismic.name,
                    seismic.tagname,
                    time_list,
                )
        selected_surface = get_observed_surface(
            case=my_case,
            surface_name=seismic.name,
            attribute=seismic.tagname,
            time_interval=time_list,
        )

        surface_instance = selected_surface.to_regular_surface()
        print(surface_instance)

    # Get all realization seismic_cubes in an iteration
    seismic_type = "realization"
    print(seismic_type, "seismic_cubes:")

    iterations = my_case.iterations

    if len(iterations) == 0:
        print("WARNING: No iterations found in case")
    else:
        iter_name = my_case.iterations[0].get("name")
        seismic_cubes = my_case.seismic_cubes.filter(
            stage="realization", iteration=iter_name
        )

        # try:
        #     print_sumo_objects(seismic_cubes)
        # except Exception as e:
        #     print(e)

        print("Number of seismic_cubes:", len(seismic_cubes))

        # seismic_cubes = ["dummy"]

        # Get sumo id for one realization seismic
        if len(seismic_cubes) > 0:
            seismic = seismic_cubes[0]
            surface_name = seismic.name
            attribute = seismic.tagname
            selected_time = seismic._metadata.get("data").get("time")
            time_interval = decode_time_interval(selected_time)

            selected_surface = get_realization_surface(
                case=my_case,
                surface_name=surface_name,
                attribute=attribute,
                time_interval=time_interval,
                iteration_name=iter_name,
                realization=0,
            )

            print(
                selected_surface.name,
                selected_surface.tagname,
                time_interval,
                selected_surface.iteration,
                selected_surface.realization,
            )

            surface_instance = selected_surface.to_regular_surface()
        print(surface_instance)

        # Get all aggregated seismic_cubes in an iteration
        seismic_type = "aggregation"

        seismic_cubes = my_case.seismic_cubes.filter(stage="iteration")
        print(seismic_type, "aggregated seismic_cubes:")

        try:
            print_sumo_objects(seismic_cubes)
        except Exception as e:
            print(e)

        print("Number of seismic_cubes:", len(seismic_cubes))

        # Get sumo id for one aggregated seismic
        if len(seismic_cubes) > 0:
            seismic = seismic_cubes[0]

            surface_name = seismic.name
            attribute = seismic.tagname
            selected_time = seismic._metadata.get("data").get("time")
            time_interval = decode_time_interval(selected_time)

            selected_surface = get_aggregated_surface(
                case=my_case,
                surface_name=surface_name,
                attribute=attribute,
                time_interval=time_interval,
                iteration_name=iter_name,
                operation="mean",
            )

            print(
                selected_surface.name,
                selected_surface.tagname,
                time_interval,
                selected_surface.iteration,
                selected_surface._metadata.get("fmu").get("aggregation"),
            )

            surface_instance = seismic.to_regular_surface()
            print(surface_instance)


if __name__ == "__main__":
    main()
