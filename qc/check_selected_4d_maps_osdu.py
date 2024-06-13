import sys
import os
import argparse
import warnings
import pandas as pd

from webviz_4d._datainput.common import read_config
from webviz_4d._providers.osdu_provider._provider_impl_file import DefaultOsduService

if sys.platform == "win32":
    from webviz_4d._datainput._osdu import (
        get_osdu_metadata_attributes, 
        convert_metadata,
    )

warnings.filterwarnings("ignore")
        
def main():
    """Load metadata for all timelapse maps from OW"""
    description = "Compile metadata for all auto4d maps"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("config_file", help="Enter path to the configuration file")

    args = parser.parse_args()
    config_file = args.config_file

    config = read_config(config_file)
    config_file = os.path.abspath(config_file)
    config_folder = os.path.dirname(config_file)
    config_folder = os.path.abspath(config_folder)

    shared_settings = config.get("shared_settings")
    osdu = shared_settings.get("osdu")

    if osdu:
        osdu_service = DefaultOsduService()
        osdu_key = "tags.AttributeMap.FieldName"

        # Search for 4D maps
        field_name = shared_settings.get("field_name")
        metadata_version = osdu.get("metadata_version")
        coverage = osdu.get("coverage")

        cache_file = "metadata_cache_" + coverage + ".csv"
        cache_file = os.path.join(config_folder, cache_file)

        if os.path.isfile(cache_file):
            print("Reading metadata from", cache_file)
            updated_metadata = pd.read_csv(cache_file)
        else:
            print("Extract metadata from OSDU ...")
            attribute_horizons = osdu_service.get_attribute_horizons(osdu_key, field_name)
            metadata = get_osdu_metadata_attributes(attribute_horizons)

            cache_file = "metadata_cache_all.csv"
            cache_file = os.path.join(config_folder, cache_file)
            metadata.to_csv(cache_file)
            print("Metadata saved to", cache_file)

            selected_attribute_maps = metadata.loc[
                (
                    (metadata["MetadataVersion"] == metadata_version)
                    & (metadata["Name"] == metadata["AttributeMap.Name"])
                    & (metadata["AttributeMap.FieldName"] == field_name)
                    & (metadata["AttributeMap.Coverage"] == coverage)
                )
            ]

            updated_metadata = osdu_service.update_reference_dates(selected_attribute_maps)
            cache_file = "metadata_cache_" + coverage + ".csv"
            cache_file = os.path.join(config_folder, cache_file)
            updated_metadata.to_csv(cache_file)
            print("Metadata saved to", cache_file)

        print(updated_metadata[["Name","AttributeMap.AttributeType","AttributeMap.SeismicTraceContent","AttributeMap.Coverage","AcquisitionDateA", "AcquisitionDateB"]])
        
        validA = updated_metadata.loc[updated_metadata["AcquisitionDateA"] !=""]
        attribute_metadata = validA.loc[validA["AcquisitionDateB"] !=""]
        
        surface_metadata = convert_metadata(attribute_metadata)
        print(surface_metadata)

if __name__ == "__main__":
    main()
        
