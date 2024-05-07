from webviz_4d._providers.osdu_provider._provider_impl_file import DefaultOsduService
from webviz_4d._datainput._osdu import get_osdu_metadata_attributes, convert_metadata
import pandas as pd
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def main():
    osdu_service = DefaultOsduService()

    id = "npequinor-dev:work-product-component--GenericRepresentation:8b2223f115374fac9f1a5bb545d564ab"
    osdu_metadata = osdu_service.get_osdu_metadata(id)

    osdu_key = "tags.AttributeMap.FieldName"
    osdu_value = "DROGON"

    attribute_horizons = osdu_service.get_attribute_horizons(osdu_key, osdu_value)
    metadata = get_osdu_metadata_attributes(attribute_horizons)

    print(
        metadata[
            ["Name", "AttributeMap.FieldName", "MetadataVersion", "AttributeMap.Name"]
        ]
    )
    metadata.to_csv("maps.csv")

    version = "0.3.2"
    selected_attribute_maps = metadata.loc[
        (
            (metadata["MetadataVersion"] == version)
            & (metadata["Name"] == metadata["AttributeMap.Name"])
        )
    ]

    updated_metadata = osdu_service.update_reference_dates(selected_attribute_maps)

    validA = updated_metadata.loc[updated_metadata["AcquisitionDateA"] !=""]
    valid_metadata = validA.loc[validA["AcquisitionDateB"] !=""]

    print("Selected and valid metadata version:", version)
    print(
        valid_metadata[["Name", "AttributeMap.FieldName", "AttributeMap.Name"]]
    )

    webviz4d_metadata = convert_metadata(valid_metadata)
    webviz4d_metadata.to_csv("metadata.csv")
    print(webviz4d_metadata)
    
if __name__ == "__main__":
    main()
