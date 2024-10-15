from webviz_4d._providers.osdu_provider._provider_impl_file import DefaultOsduService
import warnings
from datetime import datetime

from webviz_4d._datainput._osdu import get_osdu_metadata_attributes

warnings.filterwarnings("ignore")


def main():
    osdu_service = DefaultOsduService()

    # Search for 4D maps
    osdu_key = "tags.AttributeMap.FieldName"
    field_name = "DROGON"
    metadata_version = "0.3.3"
    print(field_name, metadata_version)
    print()

    attribute_horizons = osdu_service.get_attribute_horizons(osdu_key, field_name)

    # for idx,attribute_horizon in enumerate(attribute_horizons):
    #     print(idx, attribute_horizon.Name)

    metadata = get_osdu_metadata_attributes(attribute_horizons)
    selected_attribute_maps = metadata.loc[
        (
            (metadata["MetadataVersion"] == metadata_version)
            & (metadata["Name"] == metadata["AttributeMap.Name"])
            & (metadata["AttributeMap.FieldName"] == field_name)
        )
    ]

    updated_metadata = osdu_service.update_reference_dates(selected_attribute_maps)
    print(
        updated_metadata[
            [
                "Name",
                "AttributeMap.AttributeType",
                "AttributeMap.SeismicTraceContent",
                "AttributeMap.Coverage",
                "AcquisitionDateA",
                "AcquisitionDateB",
            ]
        ]
    )


if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()
