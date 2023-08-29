import pandas as pd
import argparse
import logging
from fmu.sumo.explorer import Explorer
from webviz_4d._datainput._sumo import (
    print_sumo_objects,
    check_metadata,
    get_aggregations,
)


def main():
    logging.getLogger("").setLevel(level=logging.WARNING)
    description = "Compile metadata for SUMO surfaces"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("sumo_name")
    args = parser.parse_args()

    sumo_name = args.sumo_name

    sumo = Explorer(env="prod", keep_alive="15m")

    my_case = sumo.cases.filter(name=sumo_name)[0]
    print(f"{my_case.name}: {my_case.uuid}")

    # Some case info
    print(my_case.field)
    print(my_case.status)
    print(my_case.user)

    # Get overview of all surfaces in case
    print("Number of surfaces:", len(my_case.surfaces))
    print("Pre-processed:", len(my_case.surfaces.filter(stage="case")))

    # for iteration in my_case.iterations:
    #     iteration_surfaces = my_case.surfaces.filter(iteration=iteration.get("name"))
    #     print(
    #         iteration.get("name"),
    #         ":",
    #         len(iteration_surfaces),
    #     )

    #     prediction_surfaces = check_metadata(iteration_surfaces, "is_prediction", True)
    #     print("  predictions:", len(prediction_surfaces))
    # print()

    # Get all surfaces on case level
    seismic_type = "Surfaces on case level (pre-processed)"
    print(seismic_type)

    seismic_surfaces = my_case.surfaces.filter(stage="case")
    observed_surfaces = check_metadata(seismic_surfaces, "is_observation", True)
    print("Observations")
    print_sumo_objects(observed_surfaces)

    print()
    print("Predictions")
    prediction_surfaces = check_metadata(seismic_surfaces, "is_prediction", True)
    print_sumo_objects(prediction_surfaces)

    # Get sumo instance for one case surface
    if len(observed_surfaces) > 0:
        surface = observed_surfaces[0]
        selected_time = surface._metadata.get("data").get("time")
        selected_content = surface._metadata.get("data").get("content")

        surfaces = my_case.surfaces.filter(
            stage="case",
            name=surface.name,
            tagname=surface.tagname,
        )

        time_surfaces = check_metadata(surfaces, "time", selected_time)
        selected_surfaces = check_metadata(time_surfaces, "content", selected_content)

        print()
        print("One selected case surface")
        print_sumo_objects(selected_surfaces)

        if len(selected_surfaces) == 1:
            selected_surface = selected_surfaces[0]
            surface_instance = selected_surface.to_regular_surface()
            print(surface_instance)

    # Get all realization surfaces in an iteration
    iter_name = my_case.iterations[0].get("name")
    real = 0

    surface_type = "Surfaces in iteration/realization"
    print(surface_type, iter_name, real)

    realization_surfaces = my_case.surfaces.filter(
        stage="realization", iteration=iter_name, realization=real
    )
    print_sumo_objects(realization_surfaces)

    # Get sumo instance for one realization surface
    if len(realization_surfaces) > 0:
        surface = realization_surfaces[0]
        selected_time = surface._metadata.get("data").get("time")

        realization_surfaces = my_case.surfaces.filter(
            stage="realization",
            name=surface.name,
            tagname=surface.tagname,
            iteration=iter_name,
            realization=real,
        )

        selected_surfaces = check_metadata(realization_surfaces, "time", selected_time)
        print()
        print("One selected realization surface")
        print_sumo_objects(selected_surfaces)

        if len(selected_surfaces) == 1:
            selected_surface = selected_surfaces[0]
            surface_instance = selected_surface.to_regular_surface()
            print(surface_instance)
            print()

        # Get all aggregated surfaces in an iteration
        surface_type = "Aggregation surfaces"

        aggregated_surfaces = my_case.surfaces.filter(
            stage="iteration", iteration=iter_name
        )

        if len(aggregated_surfaces) == 0:
            surfaces = my_case.surfaces.filter(stage="realization", iteration=iter_name)
            aggregated_surfaces = get_aggregations(surfaces)

        print(surface_type, iter_name)
        print_sumo_objects(aggregated_surfaces)

        # Get sumo instance for one aggregated surface
        if len(aggregated_surfaces) > 0:
            surface = aggregated_surfaces[0]
            selected_time = surface._metadata.get("data").get("time")
            selected_operation = (
                surface._metadata.get("fmu").get("aggregation").get("operation")
            )

            aggregated_surfaces = my_case.surfaces.filter(
                name=surface.name,
                tagname=surface.tagname,
                stage="iteration",
                iteration=iter_name,
            )

            if len(aggregated_surfaces) == 0:
                surfaces = my_case.surfaces.filter(
                    name=surface.name,
                    tagname=surface.tagname,
                    stage="realization",
                    iteration=iter_name,
                )
                aggregated_surfaces = get_aggregations(surfaces)

            time_surfaces = check_metadata(aggregated_surfaces, "time", selected_time)
            selected_surfaces = check_metadata(
                time_surfaces, "operation", selected_operation
            )
            print()
            print("One selected aggregated surface")
            print_sumo_objects(selected_surfaces)

            if len(selected_surfaces) == 1:
                selected_surface = selected_surfaces[0]
                surface_instance = selected_surface.to_regular_surface()
                print(surface_instance)


if __name__ == "__main__":
    main()
