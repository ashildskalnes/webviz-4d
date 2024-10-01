from pprint import pprint

from webviz_4d._providers.rddms_provider._provider_impl_file import DefaultRddmsService
from webviz_4d._providers.osdu_provider._provider_impl_file import DefaultOsduService
from webviz_4d._datainput._osdu import (
    get_osdu_metadata_attributes,
    convert_metadata,
    create_osdu_lists,
)


rddms_service = DefaultRddmsService()
osdu_service = DefaultOsduService()


def main():
    dataspaces = rddms_service.get_dataspaces()

    print("Dataspaces:")
    for dataspace in dataspaces:
        print(" -", dataspace)

    dataspace = "JS%2FSoumik_1"
    field = "JOHAN SVERDRUP"

    print(dataspace, field)

    attribute_horizons = rddms_service.get_attribute_horizons(dataspace, field)
    print("Number of attribute maps:", len(attribute_horizons))

    for attribute_horizon in attribute_horizons:
        print(" -", attribute_horizon.Name, attribute_horizon.id)

    metadata = get_osdu_metadata_attributes(attribute_horizons)
    updated_metadata = osdu_service.update_reference_dates(metadata)

    selected_columns = [
        "Name",
        "FieldName",
        "AttributeExtractionType",
        "SeismicTraceAttribute",
        "AttributeDifferenceType",
        "HorizonSourceNames",
    ]
    print(updated_metadata[selected_columns])
    converted_metadata = convert_metadata(updated_metadata)
    print()
    print(converted_metadata)

    interval_mode = "normal"
    selection_list = create_osdu_lists(converted_metadata, interval_mode)

    print()
    pprint(selection_list)


if __name__ == "__main__":
    main()
