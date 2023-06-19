import os
import glob
import argparse
import yaml

from fmu.sumo.explorer import Explorer
from fmu.sumo.explorer.timefilter import TimeType, TimeFilter
from webviz_4d._datainput.common import read_config

valid_statistics = ["mean", "std", "p10", "p50", "p90", "min", "max"]


def find_number(cubepath, txt):
    """Return the first number found in a part of a filename (cube)"""
    filename = str(cubepath)
    number = None
    index = str(filename).find(txt)

    if index > 0:
        i = index + len(txt) + 1
        j = filename[i:].find(os.sep)

        if j == -1:  # Statistics
            j = filename[i:].find("--")

        number = filename[i : i + j]

    return number


def get_metadata(cubes_file):
    delimiter = "--"
    ind = []

    real_id = None
    iter_name = None

    if "observations" in cubes_file and not "realization" in cubes_file:
        map_type = "observed"
    elif "realization" in cubes_file:
        map_type = "realization"
        real_id = find_number(cubes_file, "realization")

    return map_type, iter_name, real_id


def get_settings(config_file):
    config_file = os.path.abspath(config_file)
    config_folder = os.path.dirname(config_file)

    config = read_config(config_file)
    settings_file = config.get("shared_settings").get("settings_file")
    settings_file = os.path.join(config_folder, settings_file)
    settings = read_config(settings_file)

    return settings, config_folder


def get_absolute_paths(sumo_objects, fmu_dir):
    absolute_paths = []

    for sumo_object in sumo_objects:
        full_path = os.path.join(
            fmu_dir, sumo_object.metadata.get("file").get("relative_path")
        )
        abs_path = os.path.abspath(full_path)
        absolute_paths.append(abs_path)

    return absolute_paths


def compare_cube_collections(cubes, sumo_cubes, fmu_dir):
    """mode == "sumo" => collect the files that are on disk but not in sumo
    mode == "meta" => collect the files that are on disk but not in the metadata file
    """
    missing_files = []

    if len(cubes) > len(sumo_cubes):
        sumo_filepaths = get_absolute_paths(sumo_cubes, fmu_dir)

        for cube in cubes:
            if cube not in sumo_filepaths:
                missing_files.append(cube)
                # print("WARNING:", cube, "not found in SUMO")

    return missing_files


def print_list(file_list):
    if len(file_list) > 0:
        for item in file_list:
            print(item)

def read_yaml_file(yaml_file):
    """Return the content of a yaml file as a dict"""

    content = {}

    with open(yaml_file, "r") as stream:
        content = yaml.safe_load(stream)

    return content


def main():
    description = "Compare cubes on disk and sumo"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("sumo_id", help="sumo_id")
    parser.add_argument("fmu_dir", help="fmu_dir")

    args = parser.parse_args()
    print(args)

    sumo_id = args.sumo_id
    sumo = Explorer(env="prod")
    my_case = sumo.cases.filter(uuid=sumo_id)[0]

    fmu_dir = args.fmu_dir

    # Some case info
    print("SUMO:")
    print(f"  {my_case.name}: {my_case.uuid}")
    print(" ", my_case.field)
    print(" ", my_case.status)
    print(" ", my_case.user)

    txt = "share/observations/seismic"
    search_string = fmu_dir + os.sep + txt + os.sep + "**" + os.sep + ".*segy.yml"
    observed_metafiles = glob.glob(
        search_string, recursive=True
    )
    print("All observed cube files in", txt, fmu_dir, len(observed_metafiles))

    for metafile in observed_metafiles:
        metadata = read_yaml_file(metafile)
        print(metadata.get("file").get("relative_path"), metadata.get("file").get("checksum_md5"))

    txt = "realization-1/iter-0/share/observations/seismic"
    search_string = fmu_dir + os.sep + txt + os.sep + "**" + os.sep + ".*segy.yml"
    observed_metafiles_realization = glob.glob(
        search_string, recursive=True
    )
    print("All observed cube files in", txt, fmu_dir, len(observed_metafiles_realization))
    for metafile in observed_metafiles_realization:
        metadata = read_yaml_file(metafile)
        print(metadata.get("file").get("relative_path"), metadata.get("file").get("checksum_md5"))

    txt = "realization-1/iter-0/share/results/seismic"
    search_string = fmu_dir + os.sep + txt + os.sep + "**" + os.sep + ".*segy.yml"
    realization_metafiles = glob.glob(
        search_string, recursive=True
    )
    print("All realization cube files in", txt, fmu_dir, len(realization_metafiles))
    for metafile in realization_metafiles:
        metadata = read_yaml_file(metafile)
        print(metadata.get("file").get("relative_path"), metadata.get("file").get("checksum_md5"))

    print("Sumo cubes")
    # Observed cubes
    observed_sumo_cubes = my_case.cubes.filter(stage="case")
    print(
        "Observed cubes",
        len(observed_sumo_cubes),
    )
    for sumo_cube in observed_sumo_cubes:
        print(sumo_cube.metadata.get("file").get("relative_path"))

    # Realization cubes
    realization_sumo_cubes = my_case.cubes.filter(realization=["0"])
    print(
        "Realization cubes",
        len(realization_sumo_cubes),
    )
    results = 0
    observations = 0

    for sumo_cube in realization_sumo_cubes:
        print(sumo_cube.metadata.get("file").get("relative_path"))

        if "observations" in sumo_cube.metadata.get("file").get("relative_path"):
            observations = observations + 1
        else:
            results = results + 1

    print("observations", observations)
    print("results", results)


if __name__ == "__main__":
    main()
