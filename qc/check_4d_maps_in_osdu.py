import sys
from webviz_4d._providers.osdu_provider._provider_impl_file import DefaultOsduService
import warnings
from datetime import datetime

if sys.platform == "win32":
    from webviz_4d._datainput._osdu import (
        get_osdu_metadata_attributes, 
        convert_metadata,
        create_osdu_lists
    )

warnings.filterwarnings("ignore")
        

def main():
    osdu_service = DefaultOsduService()

    # Search for 4D maps

    osdu_key = "tags.AttributeMap.FieldName"
    field_name = "JOHAN SVERDRUP"
    metadata_version = "0.3.3"
    
    attribute_horizons = osdu_service.get_attribute_horizons(osdu_key, field_name)
    metadata = get_osdu_metadata_attributes(attribute_horizons)
    selected_attribute_maps = metadata.loc[
        (
            (metadata["MetadataVersion"] == metadata_version)
            & (metadata["Name"] == metadata["AttributeMap.Name"])
            & (metadata["AttributeMap.FieldName"] == field_name)
        )
    ]

    updated_metadata = osdu_service.update_reference_dates(selected_attribute_maps)
    print(updated_metadata)
    

        

        


    

    


        
    



   

if __name__ == '__main__':
    main()