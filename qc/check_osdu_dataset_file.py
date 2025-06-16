import io
import xtgeo
import argparse
from webviz_4d._datainput._osdu import DefaultOsduService
import matplotlib

matplotlib.use("TkAgg")
from hashlib import md5

import warnings
from datetime import datetime

warnings.filterwarnings("ignore")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("dataset_id", help="Enter dataset id")
    args = parser.parse_args()
    dataset_id = args.dataset_id

    osdu_service = DefaultOsduService()

    # dataset_id = "data:dataset--File.Generic:f4e41a92-d870-4fdd-b90a-2d2011ed5f56"
    # dataset_id = "data:dataset--File.Generic:eafe521c-abf0-44d0-bf14-e6f93841510f"

    print(dataset_id)
    dataset = osdu_service.get_horizon_map(file_id=dataset_id)

    try:
        blob = io.BytesIO(dataset.content)
        # md5sum = md5(blob.getbuffer())
        # print(md5sum.hexdigest())
        surface = xtgeo.surface_from_file(blob)
        print(surface)
        surface.quickplot(title=dataset_id)
    except Exception as inst:
        print("ERROR")
        print(type(inst))


if __name__ == "__main__":
    main()
