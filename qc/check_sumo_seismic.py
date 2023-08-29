import pandas as pd
import argparse
import logging
from fmu.sumo.explorer import Explorer
from webviz_4d._datainput._sumo import (
    check_metadata,
    print_sumo_objects,
)

from webviz_4d._datainput._oneseismic import OneseismicClient

endpoint = "https://server-oneseismictest-dev.playground.radix.equinor.com"


def main():
    logging.getLogger("").setLevel(level=logging.WARNING)
    description = "Compile metadata for SUMO seismic_cubes"
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

    # Get overview of all cubes in case
    print("Number of seismic cubes:", len(my_case.cubes))
    print("Pre-processed:", len(my_case.cubes.filter(stage="case")))

    for iteration in my_case.iterations:
        iteration_cubes = my_case.cubes.filter(iteration=iteration.get("name"))
        print(
            iteration.get("name"),
            ":",
            len(iteration_cubes),
        )
        observed_cubes = check_metadata(iteration_cubes, "is_observation", True)
        print("  observations:", len(observed_cubes))

        prediction_cubes = check_metadata(iteration_cubes, "is_prediction", True)
        print("  predictions:", len(prediction_cubes))
    print()

    # Get all observed seismic cubes on case level
    seismic_type = "Observations on case level (pre-processed)"
    print(seismic_type)

    seismic_cubes = my_case.cubes.filter(stage="case")
    print_sumo_objects(seismic_cubes)

    # Get sumo instance for one observed seismic cube
    if len(seismic_cubes) > 0:
        seismic = seismic_cubes[0]
        selected_time = seismic._metadata.get("data").get("time")

        cubes = my_case.cubes.filter(
            stage="case", name=seismic.name, tagname=seismic.tagname
        )

        selected_cube = check_metadata(cubes, "time", selected_time)
        print()
        print("One selected observed cube")
        print_sumo_objects(selected_cube)

        if len(selected_cube) == 1:
            cube = selected_cube[0]

            url = cube.url
            url = url.replace(":443", "")
            sas = cube.sas

            cube_instance = OneseismicClient(host=endpoint, vds=url, sas=sas)
            print("VDS metadata")
            print(cube_instance.get_metadata())
        else:
            print("WARNING: Number of seismic cubes found =", str(len(selected_cube)))

    # Get all observed seismic cubes in iteration/realization level
    iter_name = my_case.iterations[0].get("name")
    real = 0
    seismic_type = "Observations in iteration/realization"

    print()
    print(seismic_type, iter_name, real)

    seismic_cubes = my_case.cubes.filter(iteration=iter_name, realization=real)
    observed_cubes = check_metadata(seismic_cubes, "is_observation", True)
    print_sumo_objects(observed_cubes)

    # Get all realization seismic_cubes in an realization
    seismic_type = "Predictions in iteration/realization"
    print()
    print(seismic_type, iter_name, real)

    seismic_cubes = my_case.cubes.filter(
        stage="realization", iteration=iter_name, realization=real
    )

    prediction_cubes = check_metadata(seismic_cubes, "is_prediction", True)
    print_sumo_objects(prediction_cubes)

    # Get sumo id for one realization seismic
    if len(prediction_cubes) > 0:
        seismic = prediction_cubes[0]
        selected_time = seismic._metadata.get("data").get("time")

        cubes = seismic_cubes.filter(name=seismic.name, tagname=seismic.tagname)
        time_cubes = check_metadata(cubes, "time", selected_time)
        selected_cube = check_metadata(time_cubes, "is_prediction", True)

        print()
        print("One selected prediction cube")
        print_sumo_objects(selected_cube)

        if len(selected_cube) == 1:
            cube = selected_cube[0]

            url = cube.url
            url = url.replace(":443", "")
            sas = cube.sas

            cube_instance = OneseismicClient(host=endpoint, vds=url, sas=sas)
            print("VDS metadata")
            print(cube_instance.get_metadata())
        else:
            print("WARNING: Number of seismic cubes found =", str(len(selected_cube)))


if __name__ == "__main__":
    main()
