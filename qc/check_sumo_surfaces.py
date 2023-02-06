import pandas as pd
import argparse
import logging
from fmu.sumo.explorer import Explorer
import fmu.sumo.explorer._utils as explorer_utils
from webviz_4d._datainput._sumo import (
    decode_time_interval,
    get_surface_id,
    print_sumo_objects,
)


def main():
    logging.getLogger("").setLevel(level=logging.WARNING)
    description = "Compile metadata for SUMO surfaces"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("sumo_name")
    args = parser.parse_args()

    sumo_name = args.sumo_name

    sumo = Explorer(env="prod")

    my_case = sumo.cases.filter(name=sumo_name)[0]
    print(f"{my_case.name}: {my_case.id}")

    # Some case info
    print(my_case.field)
    print(my_case.status)
    print(my_case.user)

    # Get all observed surfaces in a case
    surface_type = "observed"
    print(surface_type)

    surfaces = my_case.observation.surfaces
    print_sumo_objects(surfaces)

    # Get sumo id for one observed surface
    if len(surfaces) > 0:
        surface = surfaces[0]
        selected_time = surface._metadata.get("data").get("time")

        selected_surfaces = my_case.observation.surfaces.filter(
            name=surface.name, tagname=surface.tagname
        )

        for surface in selected_surfaces:
            if surface._metadata.get("data").get("time") == selected_time:
                time_list = decode_time_interval(selected_time)

                print(
                    surface_type,
                    "surface:",
                    surface.name,
                    surface.tagname,
                    time_list,
                )

                surface_instance = surface.to_regular_surface()
                print(surface_instance)

        # Get all realization surfaces in an iteration
        surface_type = "realization"
        print(surface_type, "surfaces:")

        iter_id = 0
        real_id = 0

        surfaces = my_case.realization.surfaces.filter(
            iteration=iter_id, realization=real_id
        )

        try:
            print_sumo_objects(surfaces)
        except Exception as e:
            print("Number of surfaces:", len(surfaces))
            print(e)

        # Get sumo id for one realization surface
        if len(surfaces) > 0:
            surface = surfaces[0]
            selected_time = surface._metadata.get("data").get("time")

            selected_surfaces = my_case.realization.surfaces.filter(
                name=surface.name,
                tagname=surface.tagname,
                iteration=surface.iteration,
                realization=surface.realization,
            )

            for surface in selected_surfaces:
                if surface._metadata.get("data").get("time") == selected_time:
                    time_list = decode_time_interval(selected_time)

                print(
                    surface_type,
                    "surface:",
                    surface.name,
                    surface.tagname,
                    time_list,
                    surface.iteration,
                    surface.realization,
                )

                surface_instance = surface.to_regular_surface()
                print(surface_instance)

    # Get all aggregated surfaces in an iteration
    surface_type = "aggregation"
    # iterations = my_case.get_iterations()
    # iter_id = iterations[0].get("id")

    aggregations = ["mean", "min", "max", "p10", "p50", "p90", "std"]

    surfaces = my_case.aggregation.surfaces.filter(iteration=iter_id)
    print(surface_type, "aggreagated surfaces:")

    try:
        print_sumo_objects(surfaces)
    except Exception as e:
        print("Number of surfaces:", len(surfaces))
        print(e)

    # Get sumo id for one aggregated surface
    if len(surfaces) > 0:
        surface = surfaces[0]
        selected_time = surface._metadata.get("data").get("time")
        aggregation = surface._metadata.get("fmu").get("aggregation")
        selected_operation = aggregation.get("operation")

        selected_surfaces = my_case.aggregation.surfaces.filter(
            name=surface.name, tagname=surface.tagname, iteration=surface.iteration
        )

        for surface in selected_surfaces:
            if surface._metadata.get("data").get("time") == selected_time:
                time_list = decode_time_interval(selected_time)
                aggregation = surface._metadata.get("fmu").get("aggregation")
                operation = aggregation.get("operation")

                if aggregation.get("operation") == selected_operation:
                    print(
                        surface_type,
                        "surface:",
                        surface.name,
                        surface.tagname,
                        time_list,
                        surface.iteration,
                        operation,
                    )

                    surface_instance = surface.to_regular_surface()
                    print(surface_instance)


if __name__ == "__main__":
    main()
