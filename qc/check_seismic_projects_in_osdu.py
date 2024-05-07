import os
import io
import numpy as np
import pandas as pd
from webviz_4d._datainput._osdu import DefaultOsduService
from hashlib import md5
import warnings
from datetime import datetime
from pprint import pprint

warnings.filterwarnings("ignore")

osdu_service = DefaultOsduService()


def main():
    # Search for 4D maps
    print("Searching for all Seismic Processing objects in OSDU ...")
    osdu_objects = osdu_service.get_seismic_processings(None)
    print("  ", len(osdu_objects))

if __name__ == "__main__":
    main()