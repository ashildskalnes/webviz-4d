import io
import xtgeo
from webviz_4d._providers.osdu_provider._provider_impl_file import DefaultOsduService
from webviz_4d._datainput._osdu import get_osdu_metadata, convert_metadata
import urllib3
from pprint import pprint

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def main():
    osdu_service = DefaultOsduService()

    id = "data:work-product-component--GenericRepresentation:481cce21-f805-4264-898e-e705d4a6c3f6"
    osdu_metadata = osdu_service.get_osdu_metadata(id)
    print("id:", id)
    pprint(osdu_metadata)
    print()

    print("Attribute horizons:")
    attribute_horizons = osdu_service.get_attribute_horizons(metadata_version="0.4.2")
    metadata = get_osdu_metadata(attribute_horizons)
    print(metadata[["Name", "FieldName", "MetadataVersion"]])
    print()

    print("Seismic horizons:")
    seismic_horizons = osdu_service.get_seismic_horizons(version="1.2.0")
    metadata = get_osdu_metadata(seismic_horizons)
    print(metadata[["Name", "version", "Source", "BinGridID", "Status", "Datasets"]])

    for indx, row in metadata.iterrows():
        print(row[["Name", "Datasets"]])
        dataset_ids = row["Datasets"]

        if type(dataset_ids) == list:
            for dataset_id in dataset_ids:
                dataset_metadata = osdu_service.parse_dataset(dataset_id)
                pprint(dataset_metadata)

                if (
                    dataset_metadata
                    and dataset_metadata.EncodingFormatTypeID
                    and "irap-binary" in dataset_metadata.EncodingFormatTypeID
                ):
                    dataset = osdu_service.get_horizon_map(file_id=dataset_id)
                    blob = io.BytesIO(dataset.content)
                    surface = xtgeo.surface_from_file(blob)
                    print(surface)

    bingrid_ids = metadata["BinGridID"].unique()
    selected_id = bingrid_ids[1]

    print()
    print("Seismic bingrids:")
    seismic_bingrids = osdu_service.get_bingrids(selected_id)
    metadata = get_osdu_metadata(seismic_bingrids)
    print(metadata[["Name", "version", "id"]])


if __name__ == "__main__":
    main()
