import io
import xtgeo
from webviz_4d._providers.osdu_provider._provider_impl_file import DefaultOsduService
from webviz_4d._datainput._osdu import get_osdu_metadata
import urllib3
from pprint import pprint

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SEISMIC_HORIZON_DATA = [
    "3D+BLateOligocene+JS+Merge_EQ20231_PH2DG3",
    "3D+BUtsira+JS+Merge_EQ20231_PH2DG3",
    "3D+Basement+JS+Z22+Merge_EQ20231_PH2DG3",
    "3D+IUTU+JS+M22+Merge_EQ20231_PH2DG3",
    "3D+TAasgard+JS+M22+Merge_EQ20231_PH2DG3",
    "3D+TCromerK+JS+Merge_EQ20231_PH2DG3",
    "3D+TUtsira+JS+Merge_EQ20231_PH2DG3",
    "3D+TViking+JS+M22+Merge_EQ20231_PH2DG3",
]


def main():
    osdu_service = DefaultOsduService()
    version = "1.2.0"

    seismic_horizon_datas = []
    print("Seismic horizon data:")

    for horizon_name in SEISMIC_HORIZON_DATA:
        print("Seismic name", horizon_name)
        seismic_horizons = osdu_service.get_seismic_horizons(version, horizon_name)

        if len(seismic_horizons) > 0:
            osdu_seismic = seismic_horizons[0]

            if osdu_seismic:
                seismic_horizon_datas.append(osdu_seismic)

    metadata = get_osdu_metadata(seismic_horizon_datas)
    metadata.to_csv("JS_seismic_horizon_data.csv")
    print(metadata)


if __name__ == "__main__":
    main()
