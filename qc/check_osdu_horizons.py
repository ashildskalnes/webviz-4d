import os
import io
import argparse
import warnings
import time
import glob
import json
import pandas as pd
from pprint import pprint
import xtgeo

from webviz_4d._datainput.common import print_osdu_metadata
from webviz_4d._providers.osdu_provider._provider_impl_file import DefaultOsduService

# from webviz_4d._providers.rddms_provider._provider_impl_file import DefaultRddmsService

from webviz_4d._datainput._auto4d import get_auto4d_metadata
from webviz_4d._datainput._sumo import get_sumo_metadata
from webviz_4d._datainput._fmu import get_fmu_metadata
from webviz_4d._datainput._maps import load_surface_from_file, load_surface_from_sumo

from webviz_4d._datainput._osdu import (
    get_osdu_metadata_attributes,
    parse_seismic_horizons,
    load_surface_from_osdu,
)

# from webviz_4d._datainput._rddms import get_rddms_metadata, get_rddms_dataset_id

warnings.filterwarnings("ignore")


def main():
    description = "Extract and check metadata from different sources"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("data_source")
    parser.add_argument("horizon_name")
    parser.add_argument("--plot", action="store_true", help="Plot selected map")

    args = parser.parse_args()

    data_source = args.data_source
    horizon_name = args.horizon_name

    config = {"data_source": data_source}

    print("Selected data source:", data_source)
    print("Selected horizon:", horizon_name)
    print()

    tic = time.perf_counter()

    if data_source == "auto4d_file":
        auto4d_dir = "/scratch/auto4d/userhorizons"
        meta_ext = ".a4dmeta"
        directory = os.path.join(auto4d_dir, os.getlogin(), "horizons")
        config.update({"auto4d_directory": directory})

        # Search for all metadata files
        metadata_files = glob.glob(directory + "/*" + meta_ext)
        metadata_list = []

        for metadata_file in metadata_files:
            horizon_file = metadata_file.replace(".a4dmeta", ".gri")
            # Opening metadata file
            try:
                f = open(metadata_file)
                map_dict = json.load(f)
                map_dict.update({"filename": horizon_file})
                metadata_list.append(map_dict)
            except:
                metadata = None

        metadata = pd.DataFrame(metadata_list)
        metadata["Name"] = metadata["Horizon"]
    elif data_source == "osdu":
        osdu_service = DefaultOsduService()
        objects = osdu_service.get_seismic_horizons()
        horizons = parse_seismic_horizons(osdu_service, objects)
        metadata = get_osdu_metadata_attributes(horizons)

    else:
        print("ERROR Data source not supported:", data_source)
        exit()

    toc = time.perf_counter()

    print("Number of seismic horizons found:", len(metadata))
    print(f"  - metadata extracted in {toc - tic:0.2f} seconds")

    if data_source == "osdu":
        selected_metadata_fields = [
            "Name",
            "OwId",
            "FieldID",
            "SeismicDomainTypeID",
            "BinGridID",
        ]

        print_osdu_metadata(metadata, selected_metadata_fields)

        selected_metadata = metadata[metadata["Name"] == horizon_name]

        if not selected_metadata.empty:
            datasets = selected_metadata["Datasets"].values[0]
            dataset_id = datasets[1]
            dataset = osdu_service.get_horizon_map(file_id=dataset_id)
            blob = io.BytesIO(dataset.content)
            surface = xtgeo.surface_from_file(blob)
        else:
            print("ERROR: Horizon not found:", horizon_name)
            surface = None

    elif data_source == "auto4d_file":
        print(metadata)
        print()

        selected_metadata = metadata[metadata["Name"] == horizon_name]

        if not selected_metadata.empty:
            filename = selected_metadata["filename"].values[0]
            surface = xtgeo.surface_from_file(filename)
        else:
            print("ERROR: Horizon not found:", horizon_name)
            surface = None

    if surface:
        print(surface)
        surface.quickplot(title=horizon_name + " " + data_source)


if __name__ == "__main__":
    main()
