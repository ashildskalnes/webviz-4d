import os
import requests  # type: ignore
import json
import urllib.parse
from dotenv import load_dotenv
import pandas as pd  # type: ignore
import numpy as np  # type: ignore
import requests
import xtgeo
import xtgeoviz.plot as xtgplot
from pprint import pprint
from random import uniform
import warnings

from webviz_4d._providers.rddms_provider._provider_impl_file import DefaultRddmsService

warnings.filterwarnings("ignore")


def main():
    rddms_service = DefaultRddmsService()

    # List the available data space(s)
    dataspaces = rddms_service.get_dataspaces()
    print("Dataspaces:")

    for dataspace in dataspaces:
        print(" -", dataspace)

    print()

    selected_dataspace = "auto4d%2Ftest-cloud"
    object_type = "resqml20.obj_Grid2dRepresentation"

    grid2d_objects = rddms_service.get_grid2ds(selected_dataspace, object_type)
    print("Selected dataspace:", selected_dataspace)
    print(" - ", object_type, ":", len(grid2d_objects))

    for grid2_object in grid2d_objects:
        uuid = grid2_object.get("uuid")
        uuid_url = rddms_service.get_grid2_url(selected_dataspace, uuid)
        grid2_object.update({"uuid_url": uuid_url})

        pprint(grid2_object)
        print()

        if grid2_object.get("name") != "" and grid2_object.get("name") != "":
            rddms_surface = rddms_service.get_rddms_map(
                selected_dataspace, grid2_object.get("name"), uuid, uuid_url
            )

            if rddms_surface:
                print(rddms_surface)
                print()

    metadata_version = "0.4.2"
    field_name = "JOHAN SVERDRUP"

    print()
    print(
        "-------------------------------------------------------------------------------------------"
    )
    print(
        "Searching for seismic 4D attribute maps in RDDMS",
        selected_dataspace,
        metadata_version,
        field_name,
        " ...",
    )

    attribute_horizons = rddms_service.get_attribute_horizons(
        selected_dataspace, field_name
    )

    for attribute_horizon in attribute_horizons:
        if attribute_horizon:
            print(attribute_horizon.Name)

            uuid = attribute_horizon.id
            uuid_url = attribute_horizon.DatasetIDs[0]

            rddms_surface = rddms_service.get_rddms_map(
                selected_dataspace, attribute_horizon.Name, uuid, uuid_url
            )

            print(rddms_surface)
            print()


if __name__ == "__main__":
    main()
