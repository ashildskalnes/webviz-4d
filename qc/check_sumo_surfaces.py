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


def main():
    logging.getLogger("").setLevel(level=logging.WARNING)
    description = "Compile metadata for SUMO surfaces"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("sumo_name")
    args = parser.parse_args()

    sumo_name = args.sumo_name

    sumo = Explorer(env="prod", keep_alive="3m")

    my_case = sumo.cases.filter(name=sumo_name)[0]
    print(f"{my_case.name}: {my_case.uuid}")

    # Some case info
    print(my_case.field)
    print(my_case.status)
    print(my_case.user)

    # Get all observed surfaces in a case
    surface_type = "observed"
    print(surface_type)

    surfaces = my_case.surfaces.filter(stage="case")
    print_sumo_objects(surfaces)

    # Get sumo instance for one observed surface
    if len(surfaces) > 0:
        surface = surfaces[0]
        selected_time = surface._metadata.get("data").get("time")

        selected_surfaces = my_case.surfaces.filter(
            stage="case", name=surface.name, tagname=surface.tagname
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
        selected_surface = get_observed_surface(
            case=my_case,
            surface_name=surface.name,
            attribute=surface.tagname,
            time_interval=time_list,
        )

        surface_instance = selected_surface.to_regular_surface()
        print(surface_instance)

    # Get all realization surfaces in an iteration
    surface_type = "realization"
    print(surface_type, "surfaces:")

    iterations = my_case.iterations

    if len(iterations) == 0:
        print("WARNING: No iterations found in case")
    else:
        iter_name = my_case.iterations[0].get("name")
        surfaces = my_case.surfaces.filter(stage="realization", iteration=iter_name)

        # try:
        #     print_sumo_objects(surfaces)
        # except Exception as e:
        #     print(e)

        print("Number of surfaces:", len(surfaces))

        # surfaces = ["dummy"]

        # Get sumo id for one realization surface
        if len(surfaces) > 0:
            surface = surfaces[0]
            surface_name = surface.name
            attribute = surface.tagname
            selected_time = surface._metadata.get("data").get("time")
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

        # Get all aggregated surfaces in an iteration
        surface_type = "aggregation"

        surfaces = my_case.surfaces.filter(stage="iteration")
        print(surface_type, "aggregated surfaces:")

        try:
            print_sumo_objects(surfaces)
        except Exception as e:
            print(e)

        print("Number of surfaces:", len(surfaces))

        # Get sumo id for one aggregated surface
        if len(surfaces) > 0:
            surface = surfaces[0]

            surface_name = surface.name
            attribute = surface.tagname
            selected_time = surface._metadata.get("data").get("time")
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

            surface_instance = surface.to_regular_surface()
            print(surface_instance)


if __name__ == "__main__":
    main()
