import io
import xtgeo
from webviz_4d._providers.osdu_provider._provider_impl_file import DefaultOsduService
from webviz_4d._datainput._osdu import get_osdu_metadata
import urllib3
from pprint import pprint

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SEISMIC_TRACE_DATA = [
    "EQ19231DZC23A-KPSDM-RAW-FULL-0535-TIME",
    "EQ20231DZC23A-KPSDM-RAW-FULL-0535-TIME",
    "EQ21200DZC23A-KPSDM-RAW-FULL-0535-TIME",
    "EQ22200DZC23A-KPSDM-RAW-FULL-0535-TIME",
    "EQ22205DZC23A-KPSDM-RAW-FULL-0535-TIME",
    "EQ23200DZC23A-KPSDM-RAW-FULL-0535-TIME",
    "EQ23205DZC23B-KPSDM-RAW-FULL-0535-TIME",
]


def main():
    osdu_service = DefaultOsduService()

    seismic_trace_datas = []
    print("Seismic trace data:")

    for seismic_name in SEISMIC_TRACE_DATA:
        print("Seismic name", seismic_name)
        seismic_traces = osdu_service.get_seismic_trace_data(seismic_name)

        if len(seismic_traces) > 0:
            osdu_seismic = seismic_traces[0]

            if osdu_seismic:
                seismic_trace_datas.append(osdu_seismic)

    metadata = get_osdu_metadata(seismic_trace_datas)
    metadata.to_csv("JS_seismic_trace_data.csv")
    print(metadata)


if __name__ == "__main__":
    main()
