import pandas as pd
import argparse
import logging
from fmu.sumo.explorer import Explorer
import fmu.sumo.explorer._utils as explorer_utils
from webviz_4d._datainput._sumo import decode_time_interval, get_surface_id


def main():
    logging.getLogger("").setLevel(level=logging.WARNING)
    description = "Compile metadata for SUMO surfaces"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("sumo_name")
    args = parser.parse_args()

    sumo_name = args.sumo_name
    sumo = Explorer(env="prod")

    my_case = sumo.get_case_by_name(sumo_name)
    print(f"{my_case.name}: {my_case.sumo_id}")

    # Some case info
    print(my_case.field_name)
    print(my_case.status)
    print(my_case.user)

    # Get all observed surfaces in a case
    surface_type = "observed"
    surfaces = my_case.get_objects("surface", stages=["case"])

    print(surface_type, "surfaces:", len(surfaces))

    for surface in surfaces:
        index = surfaces.index(surface)
        time = surface.meta_data.get("data").get("time")
        time_list = decode_time_interval(time)

        print("  ", index, surface.name, surface.tag_name, time_list)

    # Get sumo id for one observed surface
    if len(surfaces) > 0:
        surface_id = get_surface_id(
            sumo,
            sumo_name,
            surface_type,
            surface.name,
            surface.tag_name,
            time_list,
        )

        if surface_id is not None:
            print(
                "\n",
                surface_type,
                "surface:",
                surface.name,
                surface.tag_name,
                time_list,
            )

            surface_instance = explorer_utils.get_surface_object(surface_id, sumo)
            print(surface_instance)

    # Get all realization surfaces in an iteration
    surface_type = "realization"
    iterations = my_case.get_iterations()
    iter_id = iterations[0].get("id")
    iter_name = iterations[0].get("name")
    real_id = 0

    surfaces = my_case.get_objects(
        object_type="surface",
        object_names=[],
        tag_names=[],
        time_intervals=[],
        iteration_ids=[iter_id],
        realization_ids=[real_id],
    )

    print(surface_type, "surfaces:", len(surfaces))

    for surface in surfaces:
        index = surfaces.index(surface)
        time = surface.meta_data.get("data").get("time")
        time_list = decode_time_interval(time)
        iteration_name = surface.meta_data.get("fmu").get("iteration").get("name")
        realization_name = surface.meta_data.get("fmu").get("realization").get("name")

        print(
            "  ",
            index,
            surface.name,
            surface.tag_name,
            time_list,
            iteration_name,
            realization_name,
        )

    # Get sumo id for one realization surface
    surface_id = get_surface_id(
        sumo_explorer=sumo,
        case_name=sumo_name,
        surface_type=surface_type,
        surface_name=surface.name,
        attribute=surface.tag_name,
        time_interval=time_list,
        iteration_name=iter_name,
    )

    if surface_id is not None:
        surface_obj = explorer_utils.get_surface_object(surface_id, sumo)
        print("\n", surface_type, "surface:", surface.name, surface.tag_name, time_list)
        print(surface_obj)

    # Get all aggregated surfaces in an iteration
    surface_type = "aggregation"
    iterations = my_case.get_iterations()
    iter_id = iterations[0].get("id")
    real_id = 0
    aggregations = ["mean", "min", "max", "p10", "p50", "p90", "std"]

    surfaces = my_case.get_objects(
        object_type="surface",
        object_names=[],
        tag_names=[],
        time_intervals=[],
        iteration_ids=[iter_id],
        aggregations=aggregations,
    )

    print(surface_type, "surfaces:", len(surfaces))

    for surface in surfaces:
        index = surfaces.index(surface)
        time = surface.meta_data.get("data").get("time")
        time_list = decode_time_interval(time)
        iteration_name = surface.meta_data.get("fmu").get("iteration").get("name")
        operation = surface.meta_data.get("fmu").get("aggregation").get("operation")

        print(
            "  ",
            index,
            surface.name,
            surface.tag_name,
            time_list,
            iteration_name,
            operation,
        )

    # Get sumo id for one aggregated surface
    surface_id = get_surface_id(
        sumo_explorer=sumo,
        case_name=sumo_name,
        surface_type=surface_type,
        surface_name=surface.name,
        attribute=surface.tag_name,
        time_interval=time_list,
        iteration_name=iter_name,
        aggregation="mean",
    )

    if surface_id is not None:
        surface_obj = explorer_utils.get_surface_object(surface_id, sumo)
        print("\n", surface_type, "surface:", surface.name, surface.tag_name, time_list)
        print(surface_obj)


if __name__ == "__main__":
    main()
