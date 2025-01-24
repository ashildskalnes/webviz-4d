import os
import argparse
from pprint import pprint
import warnings
import prettytable as pt

from webviz_4d._providers.rddms_provider._provider_impl_file import DefaultRddmsService

warnings.filterwarnings("ignore")


def main():
    description = (
        "Check resqml20.obj_Grid2dRepresentation objects in an OSDU RDDMS dataspace"
    )
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("dataspace")

    args = parser.parse_args()

    selected_dataspace = args.dataspace.replace("/", "%2F")

    schema_dir = "./examples"
    schema_file = "seismic_attribute_interpretation_042_schema.json"
    schema_file = os.path.join(schema_dir, schema_file)
    rddms_service = DefaultRddmsService(schema_file)
    object_type = "resqml20.obj_Grid2dRepresentation"

    # List the available data space(s)
    dataspaces = rddms_service.get_dataspaces()
    print("Dataspaces:")

    for dataspace in dataspaces:
        print(" -", dataspace)

    print()

    print("Selected dataspace:", selected_dataspace)
    grid2d_objects = rddms_service.get_grid2ds(selected_dataspace, object_type)
    print(" - ", object_type, ":", len(grid2d_objects))

    if len(grid2d_objects) > 0:
        table_names = ["name", "uuid", "uuid_url"]

        table = pt.PrettyTable()
        table.field_names = table_names

        for idx, grid2_object in enumerate(grid2d_objects):
            uuid = grid2_object.get("uuid")
            name = grid2_object.get("name")

            print(idx, name, uuid)

            if name == "4D_JS_FulRes_23sp-22auP_TS_0535_IUTU-10":
                uuid_url = rddms_service.get_grid2_url(selected_dataspace, uuid, name)
                grid2_object.update({"uuid_url": uuid_url})

                if (
                    uuid_url
                    and grid2_object.get("name") != ""
                    and grid2_object.get("name") != ""
                ):
                    table.add_row([name, uuid, uuid_url])

                    # rddms_surface = rddms_service.get_rddms_map(
                    #     selected_dataspace, grid2_object.get("name"), uuid, uuid_url
                    # )

                    if name == "4D_JS_FulRes_23sp-22auP_TS_0535_IUTU-10":
                        rddms_surface = rddms_service.get_auto4d_rddms_map(
                            selected_dataspace, grid2_object.get("name"), uuid, uuid_url
                        )

                    if rddms_surface:
                        print(idx, name, uuid, uuid_url)
                        print(rddms_surface)
                        print()

        print(table)

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
    print()

    attribute_horizons = rddms_service.get_attribute_horizons(
        selected_dataspace, field_name
    )

    for attribute_horizon in attribute_horizons:
        if attribute_horizon:
            uuid = attribute_horizon.id
            uuid_url = attribute_horizon.DatasetIDs[0]

            print(attribute_horizon.Name, uuid, uuid_url)

            rddms_surface = rddms_service.get_rddms_map(
                selected_dataspace, attribute_horizon.Name, uuid, uuid_url
            )

            print(rddms_surface)
            print()


if __name__ == "__main__":
    main()
