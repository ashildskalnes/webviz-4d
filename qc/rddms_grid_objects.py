import io
import numpy as np
import pandas as pd
import argparse
import time
from pprint import pprint
import xtgeoviz.plot as xtgplot
import xtgeo


from webviz_4d._datainput.common import read_config
from webviz_4d._providers.rddms_provider._provider_impl_file import DefaultRddmsService
from webviz_4d._providers.osdu_provider._provider_impl_file import DefaultOsduService
from webviz_4d._datainput._osdu import (
    get_osdu_metadata_attributes,
    convert_metadata,
)

from webviz_4d._datainput._rddms import get_rddms_dataset_id, create_rddms_lists

rddms_service = DefaultRddmsService()
osdu_service = DefaultOsduService()


def main():
    description = "Check RDDMS grid definitions"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("dataspace_name")

    args = parser.parse_args()
    selected_dataspace = args.dataspace_name

    data_source = "RDDMS"
    dataspace = selected_dataspace.replace("/", "%2F")
    object_type = "resqml20.obj_Grid2dRepresentation"
    kind = "eqnr:cns-api:seismic-attribute-interpretation:*"

    print("-----------------------------------------------------------------")
    print(
        "Searching for",
        object_type,
        "in",
        data_source,
        dataspace,
    )

    grid2d_objects = rddms_service.get_grid2ds(dataspace, object_type)
    print(" - ", object_type, ":", len(grid2d_objects))

    for grid2_object in grid2d_objects:
        grid_name = grid2_object.get("name")
        uuid = grid2_object.get("uuid")
        rddms_horizon = rddms_service.get_extra_metadata(dataspace, uuid, "")
        uuid_url = rddms_service.get_grid2_url(dataspace, uuid, grid_name)

        if uuid is not None:
            print()
            print("Loading surface from", data_source)
            print(" - name:", grid_name)
            print(" - uuid:", uuid)
            print(" - uuid_url:", uuid_url)

            surface = rddms_service.get_rddms_map(
                dataspace_name=selected_dataspace,
                horizon_name=grid_name,
                uuid=uuid,
                uuid_url=uuid_url,
            )

            if surface:
                print(surface)
                print()


if __name__ == "__main__":
    main()
