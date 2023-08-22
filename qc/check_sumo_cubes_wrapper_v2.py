import json
import pandas as pd

import sumo.wrapper

from webviz_4d._datainput._oneseismic import OneseismicClient, plotslice
from webviz_4d._datainput._seismic import load_cube_data, get_zslice, plot_zslice


iterations = []
realizations = []
predictions = []
observations = []
names = []
tagnames = []
attributes = []
calculations = []
offsets = []
domains = []
time0s = []
time1s = []
relative_paths = []

seismic_metadata = {
    "iteration": iterations,
    "realization": realizations,
    "is_observation": observations,
    "is_prediction": predictions,
    "name": names,
    "tagname": tagnames,
    "attribute": attributes,
    "calculation": calculations,
    "offset": offsets,
    "domain": domains,
    "time0": time0s,
    "time1": time1s,
    "relative_path": relative_paths,
}


def add_metadata(metadata):
    fmu = metadata.get("_source").get("fmu")
    iteration = fmu.get("iteration")

    if iteration:
        iteration_name = iteration.get("name")
    else:
        iteration_name = None

    realization = fmu.get("realization")

    if iteration:
        realization_name = realization.get("name")
    else:
        realization_name = None

    data = metadata.get("_source").get("data")
    name = data.get("name")
    tagname = data.get("tagname")
    attribute = data.get("seismic").get("attribute")
    calculation = data.get("seismic").get("calculation")
    stacking_offset = data.get("seismic").get("stacking_offset")
    vertical_domain = data.get("vertical_domain")

    t0 = data.get("time").get("t0").get("value")[:10]
    t1 = data.get("time").get("t1")

    if t1:
        t1 = t1.get("value")[:10]

    xmin = data.get("bbox").get("xmin")
    xmax = data.get("bbox").get("xmax")
    ymin = data.get("bbox").get("ymin")
    ymax = data.get("bbox").get("ymax")
    zmin = data.get("bbox").get("zmin")
    zmax = data.get("bbox").get("zmax")

    # bounding_box = [[xmin, xmax], [ymin, ymax], [zmin, zmax]]

    is_observation = data.get("is_observation")
    is_prediction = data.get("is_prediction")

    file_info = metadata.get("_source").get("file")
    relative_path = file_info.get("relative_path")

    iterations.append(iteration_name)
    realizations.append(realization_name)
    observations.append(is_observation)
    predictions.append(is_prediction)
    names.append(name)
    tagnames.append(tagname)
    attributes.append(attribute)
    calculations.append(calculation)
    offsets.append(stacking_offset)
    domains.append(vertical_domain)
    time0s.append(t0)
    time1s.append(t1)
    relative_paths.append(relative_path)


def main():
    # Connect to Sumo and list openvds formatted objects
    sumo_wrapper = sumo.wrapper.SumoClient("prod")
    cubes = sumo_wrapper.get(f"/search", query="data.format:openvds", size=100)
    print(
        "Number of openvds formatted objects I have access to: ",
        len(cubes.get("hits").get("hits")),
    )

    # Scan through all objects
    for cube in cubes.get("hits").get("hits"):
        id = cube.get("_id")

        # Have a look at this objects metadata
        metadata = sumo_wrapper.get(f"/objects('{id}')")
        add_metadata(metadata)

        # Get authorization token to read blob data for this object
        # response = sumo_wrapper.get(f"/objects('{id}')/blob/authuri")
        # json_resp = json.loads(response.decode("UTF-8"))

        # url = "https:" + json_resp.get("baseuri")[6:] + id
        # url = url.replace(":443", "")
        # sas = json_resp.get("auth")

        # # Massage the returned token into arguments accepted by oneseismic
        # endpoint = "https://server-oneseismictest-dev.playground.radix.equinor.com"

        # vds_cube = OneseismicClient(host=endpoint, vds=url, sas=sas)
        # vds_metadata = vds_cube.get_metadata()

        # print(json.dumps(vds_metadata, sort_keys=True, indent=4))

    metadata_overview = pd.DataFrame(seismic_metadata)
    csv_file = "seismic_metadata.csv"
    metadata_overview.to_csv(csv_file)
    print("Metadata:", csv_file)
    print(metadata_overview)


if __name__ == "__main__":
    main()
