import time
import warnings
from datetime import datetime

from webviz_4d._providers.osdu_provider._provider_impl_file import DefaultOsduService
from webviz_4d._datainput._osdu import (
    get_osdu_metadata_attributes,
    convert_metadata,
)

warnings.filterwarnings("ignore")

osdu_service = DefaultOsduService()


def main():
    # Search for 4D maps
    print("Searching for seismic 4D attribute maps in OSDU ...")
    osdu_key = "tags.AttributeMap.FieldName"
    field_name = "JOHAN SVERDRUP"

    print("Selected FieldName =", field_name)
    metadata_version = "0.3.3"

    print("Extracting metadata from OSDU ...")
    start_time = time.time()
    attribute_horizons = osdu_service.get_attribute_horizons(osdu_key, field_name)

    metadata = get_osdu_metadata_attributes(attribute_horizons)
    print(" --- %s seconds ---" % (time.time() - start_time))
    print()

    selected_attribute_maps = metadata.loc[
        (
            (metadata["MetadataVersion"] == metadata_version)
            & (metadata["Name"] == metadata["AttributeMap.Name"])
            & (metadata["AttributeMap.FieldName"] == field_name)
        )
    ]

    updated_metadata = osdu_service.update_reference_dates(selected_attribute_maps)

    print(updated_metadata)

    validA = updated_metadata.loc[updated_metadata["AcquisitionDateA"] != ""]
    attribute_metadata = validA.loc[validA["AcquisitionDateB"] != ""]

    webviz_metadata = convert_metadata(attribute_metadata)
    print(webviz_metadata)

    output_file = (
        "metadata_" + field_name.replace(" ", "_") + "_" + metadata_version + ".csv"
    )
    webviz_metadata.to_csv(output_file)

    print("Selected metadata written to:", output_file)


if __name__ == "__main__":
    main()
