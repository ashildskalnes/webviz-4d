import argparse
from webviz_4d._providers.osdu_provider._provider_impl_file import DefaultOsduService
from webviz_4d._datainput._osdu import get_osdu_metadata
from webviz_4d._datainput.common import read_config


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("config_file", help="Enter name of config file")
    args = parser.parse_args()

    config_file = args.config_file
    config = read_config(config_file)
    field_name = config.get("shared_settings").get("field_name")

    osdu_service = DefaultOsduService()
    metadata, selection_list = get_osdu_metadata(config, osdu_service, field_name)

    print(metadata)


if __name__ == "__main__":
    main()
