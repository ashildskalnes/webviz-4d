import argparse
import logging
from fmu.sumo.explorer import Explorer


def main():
    logging.getLogger("").setLevel(level=logging.WARNING)
    description = "Compile metadata for SUMO surfaces"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("sumo_name")
    args = parser.parse_args()

    sumo_name = args.sumo_name

    # sumo = Explorer(env="prod", keep_alive="15m")
    sumo = Explorer(env="prod")

    my_case = sumo.cases.filter(name=sumo_name)[0]
    print(f"{my_case.name}: {my_case.uuid}")

    # Some case info
    print(my_case.field)
    print(my_case.status)
    print(my_case.user)

    # Get all realization surfaces in an iteration
    surface_type = "realization"
    print(surface_type, "surfaces:")

    iter_name = my_case.iterations[0].get("name")
    surfaces = my_case.surfaces.filter(iteration=iter_name)

    for index, surface in enumerate(surfaces):
        print(index + 1, surface.name, surface.tagname)

    print("Number of surfaces:", len(surfaces))


if __name__ == "__main__":
    main()
