import pandas as pd
import argparse
import logging
from fmu.sumo.explorer import Explorer
from webviz_4d._datainput._sumo import (
    decode_time_interval,
    get_observed_cubes,
    get_realization_cube,
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

    # Get all observed seismic cubes on case level
    seismic_type = "observed"
    print(seismic_type)

    seismic_cubes = my_case.cubes.filter(stage="case")
    print_sumo_objects(seismic_cubes)

    # Get sumo instance for one observed seismic cube
    if len(seismic_cubes) > 0:
        seismic = seismic_cubes[0]
        selected_time = seismic._metadata.get("data").get("time")

        selected_cubes = my_case.cubes.filter(
            stage="case", name=seismic.name, tagname=seismic.tagname
        )

        for seismic in selected_cubes:
            if seismic._metadata.get("data").get("time") == selected_time:
                time_list = decode_time_interval(selected_time)

                print("")
                print(
                    seismic_type,
                    "seismic:",
                    seismic.name,
                    seismic.tagname,
                    time_list,
                )
        cubes = get_observed_cubes(
            case=my_case,
            names=[seismic.name],
            tagnames=[seismic.tagname],
            time_interval=time_list,
        )

        if len(cubes) == 1:
            selected_cube = cubes[0]

            url = selected_cube.url
            url = url.replace(":443", "")
            sas = selected_cube.sas

            cube_instance = OneseismicClient(host=endpoint, vds=url, sas=sas)
            print(cube_instance.get_metadata())
        else:
            print("WARNING: Number of seismic cubes found =", str(len(cubes)))

    # Get all observed seismic cubes in iteration/realization level
    iter_name = my_case.iterations[0].get("name")
    real = 0
    seismic_type = "observed in iteration/realization", iter_name, real
    print(seismic_type)

    seismic_cubes = get_observed_cubes(
        case=my_case, iterations=[iter_name], realizations=[real]
    )
    print_sumo_objects(seismic_cubes)

    # Get all realization seismic_cubes in an iteration
    seismic_type = "realization"
    print(seismic_type, "seismic_cubes:")

    print("Iteration:", iter_name)
    seismic_cubes = my_case.cubes.filter(stage="realization", iteration=iter_name)

    print("Number of seismic_cubes:", len(seismic_cubes))

    try:
        print_sumo_objects(seismic_cubes)
    except Exception as e:
        print(e)

    # Get sumo id for one realization seismic
    if len(seismic_cubes) > 0:
        seismic = seismic_cubes[0]
        name = seismic.name
        tagname = seismic.tagname
        selected_time = seismic._metadata.get("data").get("time")
        time_interval = decode_time_interval(selected_time)

        selected_cube = get_realization_cube(
            case=my_case,
            name=name,
            tagname=tagname,
            time_interval=time_interval,
            iteration_name=iter_name,
            realization=0,
        )

        print(
            selected_cube.name,
            selected_cube.tagname,
            time_interval,
            selected_cube.iteration,
            selected_cube.realization,
        )

        url = selected_cube.url
        url = url.replace(":443", "")
        sas = selected_cube.sas

        cube_instance = OneseismicClient(host=endpoint, vds=url, sas=sas)
        print(cube_instance.get_metadata())


if __name__ == "__main__":
    main()
