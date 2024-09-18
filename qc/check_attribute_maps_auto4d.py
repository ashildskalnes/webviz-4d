import os
import argparse

import warnings
from pprint import pprint

from webviz_4d._datainput.common import read_config
from webviz_4d._datainput._auto4d import (
    load_auto4d_metadata,
    create_auto4d_lists,
)

warnings.filterwarnings("ignore")


def main():
    description = "Check auto4d metadata"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("config_file")

    args = parser.parse_args()

    config_file = args.config_file
    config_file = os.path.abspath(config_file)
    config = read_config(config_file)

    shared_settings = config.get("shared_settings")
    auto4d_settings = shared_settings.get("auto4d")
    directory = auto4d_settings.get("directory")
    metadata_version = auto4d_settings.get("metadata_version")
    metadata_format = auto4d_settings.get("metadata_format")
    acquisition_dates = auto4d_settings.get("acquisition_dates")
    selections = auto4d_settings.get("selections")
    interval_mode = shared_settings.get("interval_mode")

    print("Searching for seismic 4D attribute maps on disk ...")

    attribute_metadata = load_auto4d_metadata(
        directory, metadata_format, metadata_version, selections, acquisition_dates
    )
    print(attribute_metadata)
    output_file = "metadata_" + os.path.basename(config_file)
    attribute_metadata.to_csv(output_file)
    print("Metadata writen to", output_file)

    print("Create auto4d selection lists ...")
    selection_list = create_auto4d_lists(attribute_metadata, interval_mode)

    pprint(selection_list)


if __name__ == "__main__":
    main()
