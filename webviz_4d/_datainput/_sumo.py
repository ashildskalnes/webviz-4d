import pandas as pd

from fmu.sumo.explorer import Explorer
from webviz_4d._datainput._metadata import sort_realizations


def get_aggregated_surfaces(
    sumo_exp: Explorer = None, sumo_name: str = None, iteration_ids: list = [0]
):
    size = 10000
    my_case = sumo_exp.sumo.cases.filter(name=sumo_name)[0]

    select = "fmu,data,file"
    query = f"_sumo.parent_object:{my_case.sumo_id}"
    query += " AND class:surface"

    if len(iteration_ids) == 0:
        iter_ids = []
        iterations = my_case.get_iterations()

        for iteration in iterations:
            iter_id = iteration.get("id")
            iter_ids.append(iter_id)

        iteration_ids = iter_ids

    surfaces = []

    for iteration_id in iteration_ids:
        query += f" AND fmu.iteration.id:{iteration_id}"

        result = sumo_exp.get(path="/search", query=query, size=size, select=select)
        # print(result)

        hits = result["hits"]["hits"]
        total_hits = result["hits"]["total"]["value"]

        if result["hits"]["total"]["relation"] != "eq":
            print(
                "WARNING! Hits exceeded maximum returned hits, narrow the search query)"
            )

        if result["hits"]["total"]["value"] > len(hits):
            print(f"=====\nShowing {len(hits)} of {total_hits} hits")

        for hit in hits:
            fmu = hit.get("_source").get("fmu")

            if fmu.get("aggregation") is not None:
                surfaces.append(hit)

    # Compile metadata (dataframe)
    object_names = []
    object_ids = []
    tag_names = []
    contents = []
    time1s = []
    time2s = []
    is_predictions = []
    is_observations = []
    fmu_contexts = []
    iterations = []
    realizations = []
    operations = []
    filenames = []

    default = "---"

    for surface in surfaces:
        stage = default
        status = False
        iteration_name = default
        realization_name = default
        operation_name = default

        fmu = surface.get("_source").get("fmu", default)
        if fmu:
            context = fmu.get("context", default)
            if context != default:
                stage = context.get("stage", default)
        iteration = fmu.get("iteration", default)
        realization = fmu.get("realization", default)
        aggregation = fmu.get("aggregation", default)

        data = surface.get("_source").get("data", default)
        tagname = data.get("tagname", default)
        is_prediction = data.get("is_prediction")
        is_observation = data.get("is_observation")

        if iteration != default:
            iteration_name = iteration.get("name", default)

        if realization != default:
            realization_name = realization.get("id", default)

        if aggregation != default:
            operation_name = aggregation.get("operation", default)

        time1 = default
        time2 = default
        times = data.get("time")

        if times and type(times) == dict:
            time1 = times.get("t0").get("value")[:10]
            time2 = times.get("t1", default)

            if time2 != default:
                time2 = time2.get("value")[:10]
        elif times and type(times) == list:
            time1 = times[0].get("value")[:10]

            if len(times) == 2:
                time2 = times[1].get("value")[:10]

        if operation_name != default:
            status = True

        filename = surface.get("_source").get("file").get("relative_path")

        if status:
            object_names.append(data.get("name"))
            object_ids.append(surface.get("_id"))
            tag_names.append(tagname)
            contents.append(data.get("content"))
            fmu_contexts.append(stage)
            time1s.append(time1)
            time2s.append(time2)
            iterations.append(iteration_name)
            realizations.append(realization_name)
            operations.append(operation_name)
            is_predictions.append(is_prediction)
            is_observations.append(is_observation)
            filenames.append(filename)

    dataframe = pd.DataFrame()
    dataframe["Object name"] = object_names
    dataframe["Tag name"] = tag_names
    dataframe["Content"] = contents
    dataframe["Time 1"] = time1s
    dataframe["Time 2"] = time2s
    dataframe["Prediction"] = is_predictions
    dataframe["Observation"] = is_observations
    dataframe["Context"] = fmu_contexts
    dataframe["Iteration"] = iterations
    dataframe["Realization"] = realizations
    dataframe["Operation"] = operations
    dataframe["Object id"] = object_ids
    dataframe["Relative path"] = filenames

    return dataframe


def get_observed_surfaces(sumo_exp: Explorer = None, sumo_name: str = None):
    size = 10000
    my_case = sumo_exp.sumo.cases.filter(name=sumo_name)[0]

    select = "fmu,data,file"
    query = f"_sumo.parent_object:{my_case.sumo_id}"
    query += " AND class:surface"
    query += " AND fmu.context.stage:case"

    result = sumo_exp.get(path="/search", query=query, size=size, select=select)

    hits = result["hits"]["hits"]
    total_hits = result["hits"]["total"]["value"]

    if result["hits"]["total"]["relation"] != "eq":
        print("WARNING! Hits exceeded maximum returned hits, narrow the search query)")

    if result["hits"]["total"]["value"] > len(hits):
        print(f"=====\nShowing {len(hits)} of {total_hits} hits")

        # Compile metadata (dataframe)
    object_names = []
    object_ids = []
    tag_names = []
    contents = []
    time1s = []
    time2s = []
    is_predictions = []
    is_observations = []
    fmu_contexts = []
    iterations = []
    realizations = []
    operations = []
    filenames = []

    default = "---"

    for surface in hits:
        stage = default
        status = False
        iteration_name = default
        realization_name = default
        operation_name = default

        fmu = surface.get("_source").get("fmu", default)
        if fmu:
            context = fmu.get("context", default)
            if context != default:
                stage = context.get("stage", default)
        iteration = fmu.get("iteration", default)
        realization = fmu.get("realization", default)
        aggregation = fmu.get("aggregation", default)

        data = surface.get("_source").get("data", default)
        tagname = data.get("tagname", default)
        is_prediction = data.get("is_prediction")
        is_observation = data.get("is_observation")

        if iteration != default:
            iteration_name = iteration.get("name", default)

        if realization != default:
            realization_name = realization.get("id", default)

        if aggregation != default:
            operation_name = aggregation.get("operation", default)

        time1 = default
        time2 = default
        times = data.get("time")

        if times and type(times) == dict:
            time1 = times.get("t0").get("value")[:10]
            time2 = times.get("t1", default)

            if time2 != default:
                time2 = time2.get("value")[:10]
        elif times and type(times) == list:
            time1 = times[0].get("value")[:10]

            if len(times) == 2:
                time2 = times[1].get("value")[:10]

        filename = surface.get("_source").get("file").get("relative_path")

        object_names.append(data.get("name"))
        object_ids.append(surface.get("_id"))
        tag_names.append(tagname)
        contents.append(data.get("content"))
        fmu_contexts.append(stage)
        time1s.append(time1)
        time2s.append(time2)
        iterations.append(iteration_name)
        realizations.append(realization_name)
        operations.append(operation_name)
        is_predictions.append(is_prediction)
        is_observations.append(is_observation)
        filenames.append(filename)

    dataframe = pd.DataFrame()
    dataframe["Object name"] = object_names
    dataframe["Tag name"] = tag_names
    dataframe["Content"] = contents
    dataframe["Time 1"] = time1s
    dataframe["Time 2"] = time2s
    dataframe["Prediction"] = is_predictions
    dataframe["Observation"] = is_observations
    dataframe["Context"] = fmu_contexts
    dataframe["Iteration"] = iterations
    dataframe["Realization"] = realizations
    dataframe["Operation"] = operations
    dataframe["Object id"] = object_ids
    dataframe["Relative path"] = filenames

    return dataframe


def get_surfaces(
    sumo_exp: Explorer = None,
    sumo_name: str = None,
    mode: str = "observations",
    object_name: str = None,
    tag_name: str = None,
    time_interval: list = None,
    observation: bool = False,
    context: str = "realization",
    iteration_name: str = "iter-0",
    realization_id: str = "0",
    aggregation: str = None,
):

    size = 9999
    default = "---"
    my_case = sumo_exp.sumo.cases.filter(name=sumo_name)[0]
    iterations = my_case.get_iterations()

    iteration_id = 0
    for iteration in iterations:
        iter_name = iteration.get("name")

        if iter_name == iteration_name:
            iteration_id = iteration.get("id")

    select = "fmu,data,file"
    query = f"_sumo.parent_object:{my_case.sumo_id}"
    query += " AND class:surface"

    if mode == "realizations":
        query += f" AND fmu.iteration.id:{iteration_id}"
        query += f" AND fmu.realization.id:{realization_id}"
    elif mode == "observations":
        # query += " AND fmu.context.stage:case"
        query = query
    elif mode == "aggregations":
        query += f" AND fmu.iteration.id:{iteration_id}"

    if object_name is not None and object_name != default:
        query += f" AND data.name:{object_name}"

    if tag_name is not None and tag_name != default:
        query += f" AND data.tag_name:{tag_name}"

    result = sumo_exp.get(path="/search", query=query, size=size, select=select)
    # print(result)

    hits = result["hits"]["hits"]
    total_hits = result["hits"]["total"]["value"]

    if result["hits"]["total"]["relation"] != "eq":
        print("WARNING! Hits exceeded maximum returned hits, narrow the search query)")

    if result["hits"]["total"]["value"] > len(hits):
        print(f"=====\nShowing {len(hits)} of {total_hits} hits")

    surfaces = hits

    # Compile metadata (dataframe)
    object_names = []
    object_ids = []
    tag_names = []
    contents = []
    time1s = []
    time2s = []
    is_predictions = []
    is_observations = []
    fmu_contexts = []
    iterations = []
    realizations = []
    operations = []
    filenames = []

    for surface in surfaces:
        stage = default
        status = False
        iteration_name = default
        realization_name = default
        operation_name = default

        fmu = surface.get("_source").get("fmu", default)
        if fmu:
            context = fmu.get("context", default)
            if context != default:
                stage = context.get("stage", default)
        iteration = fmu.get("iteration", default)
        realization = fmu.get("realization", default)
        aggregation = fmu.get("aggregation", default)

        data = surface.get("_source").get("data", default)
        tagname = data.get("tagname", default)
        is_prediction = data.get("is_prediction")
        is_observation = data.get("is_observation")

        if iteration != default:
            iteration_name = iteration.get("name", default)

        if realization != default:
            realization_name = realization.get("id", default)

        if aggregation != default:
            operation_name = aggregation.get("operation", default)

        time1 = default
        time2 = default
        times = data.get("time")

        if times and type(times) == dict:
            time1 = times.get("t0").get("value")[:10]
            time2 = times.get("t1", default)

            if time2 != default:
                time2 = time2.get("value")[:10]
        elif times and type(times) == list:
            time1 = times[0].get("value")[:10]

            if len(times) == 2:
                time2 = times[1].get("value")[:10]

        if mode == "aggregations":
            if operation_name != default:
                status = True
        elif mode == "observations":
            if is_observation:
                status = True
        else:
            status = True

        filename = surface.get("_source").get("file").get("relative_path")

        if status:
            object_names.append(data.get("name"))
            object_ids.append(surface.get("_id"))
            tag_names.append(tagname)
            contents.append(data.get("content"))
            fmu_contexts.append(stage)
            time1s.append(time1)
            time2s.append(time2)
            iterations.append(iteration_name)
            realizations.append(realization_name)
            operations.append(operation_name)
            is_predictions.append(is_prediction)
            is_observations.append(is_observation)
            filenames.append(filename)

    dataframe = pd.DataFrame()
    dataframe["Object name"] = object_names
    dataframe["Tag name"] = tag_names
    dataframe["Content"] = contents
    dataframe["Time 1"] = time1s
    dataframe["Time 2"] = time2s
    dataframe["Prediction"] = is_predictions
    dataframe["Observation"] = is_observations
    dataframe["Context"] = fmu_contexts
    dataframe["Iteration"] = iterations
    dataframe["Realization"] = realizations
    dataframe["Operation"] = operations
    dataframe["Object id"] = object_ids
    dataframe["Relative path"] = filenames

    return dataframe


def sort_intervals(intervals):
    t1_list = []
    t2_list = []

    for interval in intervals:
        t1 = interval[:10]
        t1_list.append(t1)

        t2 = interval[11:]
        t2_list.append(t2)

    if t1 > t2:
        interval_mode = "reverse"
    else:
        interval_mode = "normal"

    all_dates = t1_list + t2_list

    unique_dates = list(set(all_dates))
    unique_dates.sort()

    incremental_list = []

    for i in range(1, len(unique_dates)):
        if interval_mode == "reverse":
            interval = unique_dates[i] + "-" + unique_dates[i - 1]
        else:
            interval = unique_dates[i - 1] + "-" + unique_dates[i]

        if interval in intervals:
            incremental_list.append(interval)

    other_intervals = []

    for interval in intervals:
        if interval not in incremental_list:
            other_intervals.append(interval)

    sorted_all = sorted(incremental_list) + sorted(other_intervals)

    return sorted_all


def decode_time_interval(time):
    val0 = None
    val1 = None

    if time is not None and type(time) == dict:
        if len(time) == 1:
            t0 = time.get("t0")
            val0 = t0.get("value")[:10]

        elif len(time) == 2:
            t0 = time.get("t0")
            val0 = t0.get("value")[:10]
            t1 = time.get("t1")
            val1 = t1.get("value")[:10]

    elif time is not None and type(time) == list:
        val0 = time[0].get("value")[:10]

        if len(time) == 2:
            val1 = time[1].get("value")[:10]

    return [val0, val1]


def get_tag_values(surfaces, tag_name):
    tag_list = []
    default = "---"
    tag = default

    i = 0
    for surface in surfaces:
        i = i + 1
        # print(i, tag_name, surface.name)
        if tag_name == "name":
            tag = surface.name
            # print(i, tag_name, surface.name, len(surfaces))
        elif tag_name == "attribute":
            tag = surface.tag_name
        elif tag_name == "aggregation":
            aggregation = surface._metadata.get("fmu").get("aggregation")
            if aggregation is not None:
                tag = surface._metadata.get("fmu").get("aggregation").get("operation")
            else:
                tag = None
        elif tag_name == "time":
            time = surface._metadata.get("data").get("time")
            time_list = decode_time_interval(time)

            if time_list[1] is not None:
                if time_list[0] < time_list[1]:
                    time_string = time_list[0] + " - " + time_list[1]
                    tag = time_string
                else:
                    time_string = time_list[1] + " - " + time_list[0]
                    tag = time_string
            else:
                tag = None
        elif tag_name == "time_interval":
            time = surface._metadata.get("data").get("time")
            time_list = decode_time_interval(time)

            if time_list[1] is not None:
                if time_list[0] > time_list[1]:
                    time_string = time_list[0] + "-" + time_list[1]
                else:
                    time_string = time_list[1] + "-" + time_list[0]
                tag = time_string
            else:
                tag = None

        if tag is not None and tag not in tag_list:
            tag_list.append(tag)

    unique_tags = list(set(tag_list))

    return sorted(unique_tags)


def create_selector_lists(my_case, mode):
    iterations = my_case.get_iterations()

    iteration_names = []
    for iteration in iterations:
        iteration_name = iteration.get("name")
        iteration_names.append(iteration_name)

    realization_names = []
    realizations = my_case.get_realizations(iteration_id=0)
    for realization in realizations:
        realization_name = realization.get("name")
        realization_names.append(realization_name)

    sorted_realizations = sort_realizations(realization_names)

    map_types = ["observed", "simulated", "aggregated"]
    map_dict = {}
    default = "---"

    possible_statistics = ["max", "min", "mean", "p10", "p50", "p90", "std"]
    statistics = []

    print("  mode:", mode)

    for map_type in map_types:
        map_dict[map_type] = {}
        map_type_dict = {}

        if map_type == "observed":
            surfaces = my_case.get_objects("surface", stages=["case"])
            time = get_tag_values(surfaces, "time")
            surfaces = my_case.get_objects(
                "surface", time_intervals=time, stages=["case"]
            )
            print(" ", map_type, ":", len(surfaces))

        elif map_type == "simulated":
            surfaces = my_case.get_objects(
                object_type="surface",
                object_names=[],
                tag_names=[],
                time_intervals=[],
                iteration_ids=[0],
                realization_ids=[0],
            )
            time = get_tag_values(surfaces, "time")

            surfaces = my_case.get_objects(
                object_type="surface",
                object_names=[],
                tag_names=[],
                time_intervals=time,
                iteration_ids=[0],
                realization_ids=[0],
            )

            if mode == "timelapse":
                if len(time) == 0:
                    print(" ", map_type, ":", 0)
                    return None
                else:
                    print(" ", map_type, ":", len(surfaces))

        elif map_type == "aggregated":
            if len(time) > 0:
                surfaces = my_case.get_objects(
                    object_type="surface",
                    object_names=[names[0]],
                    tag_names=[],
                    time_intervals=[time[0]],
                    iteration_ids=[0],
                    aggregations=possible_statistics,
                )
        else:
            print("ERROR: Not supported map_type", map_type)
            return None

        names = get_tag_values(surfaces, "name")
        attributes = get_tag_values(surfaces, "attribute")
        time_intervals = get_tag_values(surfaces, "time_interval")

        map_type_dict["name"] = names
        map_type_dict["attribute"] = attributes
        map_type_dict["interval"] = time_intervals

        if map_type == "simulated":
            map_type_dict["ensemble"] = iteration_names
            map_type_dict["realization"] = sorted_realizations
        elif map_type == "aggregated":
            statistics = get_tag_values(surfaces, "aggregation")
            map_type_dict["aggregation"] = statistics
        else:
            map_type_dict["ensemble"] = [default]
            map_type_dict["realization"] = [default]

        map_dict[map_type] = map_type_dict

    return map_dict


def extract_time_interval(time):
    t0 = ""
    t1 = ""
    time_string = ""

    if time is not None and type(time) == dict:
        t0 = time.get("t0").get("value")[0:10]

        if len(time) == 2:
            t1 = time.get("t1").get("value")[0:10]
            time_string = t1 + " - " + t0
        else:
            time_string = t0
    elif time is not None and type(time) == list:
        t0 = time[0].get("value")[:10]

        if len(time) == 2:
            t1 = time[1].get("value")[:10]
            time_string = t0 + " - " + t1
        else:
            time_string = t0

    return time_string


def get_time_string(time_interval):
    if time_interval[0] is not None:
        if time_interval[1] is not None:
            time_string = time_interval[0] + " - " + time_interval[1]
        else:
            time_string = time_interval[0]
    else:
        time_string = None

    return time_string


def get_surface_id(
    sumo_explorer: Explorer = None,
    case_name: str = None,
    surface_type: str = "realization",
    surface_name: str = None,
    attribute: str = None,
    time_interval: list = [],
    iteration_name: str = "iter-0",
    realization_id: int = 0,
    aggregation: str = "mean",
):

    my_case = sumo_explorer.sumo.cases.filter(name=case_name)[0]

    if my_case is None:
        print("WARNING: SUMO case not found", case_name)
        return None

    time_string = get_time_string(time_interval)

    if surface_type == "observed":
        surfaces = my_case.observation.surfaces.filter(
            name=surface_name,
            tagname=attribute,
        )

        for surface in surfaces:
            time = surface.time
            # surfaces = my_case.get_objects(
            #     object_type="surface",
            #     object_names=[surface_name],
            #     tag_names=[attribute],
            #     stages=["case"],
            #     time_intervals=[time_string],
            # )
    elif surface_type == "realization":
        iter_id = None

        iterations = my_case.get_iterations()
        for iteration in iterations:
            iter_name = iteration.get("name")
            if iter_name == iteration_name:
                iter_id = iteration.get("id")
                break

        if iter_id == None:
            print("WARNING: Iteration  not found", iteration_name)
            return None

        if time_string is not None:
            time_list = [time_string]
        else:
            time_list = []

        surfaces = my_case.get_objects(
            object_type="surface",
            object_names=[surface_name],
            tag_names=[attribute],
            time_intervals=time_list,
            iteration_ids=[iter_id],
            realization_ids=[realization_id],
        )
    elif surface_type == "aggregation":
        iter_id = None

        iterations = my_case.get_iterations()
        for iteration in iterations:
            iter_name = iteration.get("name")
            if iter_name == iteration_name:
                iter_id = iteration.get("id")
                break

        if iter_id == None:
            print("WARNING: Iteration  not found", iteration_name)
            return None

        if time_string is not None:
            time_list = [time_string]
        else:
            time_list = []

        surfaces = my_case.get_objects(
            object_type="surface",
            object_names=[surface_name],
            tag_names=[attribute],
            time_intervals=time_list,
            iteration_ids=[iter_id],
            aggregations=[aggregation],
        )
    else:
        print("ERROR: Surface type not supported", surface_type)
        print(
            case_name,
            surface_type,
            surface_name,
            attribute,
            time_interval,
        )
        return None

    if len(surfaces) == 0:
        print("WARNING: Selected surface not found")
        print(
            case_name, surface_type, surface_name, attribute, time_interval, aggregation
        )
        return None
    elif len(surfaces) > 1:
        print("WARNING: Multiple surfaces found", len(surfaces))
        print("Returning the first one:")
        print(case_name, surface_type, surface_name, attribute, time_interval)
        selected_surface = surfaces[0]
    else:
        selected_surface = surfaces[0]

    surface_id = selected_surface.sumo_id

    return surface_id


def get_observed_surface(
    sumo_explorer: Explorer = None,
    case_name: str = None,
    surface_name: str = None,
    attribute: str = None,
    time_interval: list = [],
):
    surface_id = get_surface_id(
        sumo_explorer=sumo_explorer,
        case_name=case_name,
        surface_type="observed",
        surface_name=surface_name,
        attribute=attribute,
        time_interval=time_interval,
    )

    return surface_id


def get_realization_surface(
    sumo_explorer: Explorer = None,
    case_name: str = None,
    surface_name: str = None,
    attribute: str = None,
    time_interval: list = [],
    iteration_name: str = "iter-0",
    realization_id: int = 0,
):
    surface_id = get_surface_id(
        sumo_explorer,
        case_name,
        "realization",
        surface_name,
        attribute,
        time_interval,
        iteration_name,
        realization_id,
    )

    return surface_id


def get_aggregated_surface(
    sumo_explorer: Explorer = None,
    case_name: str = None,
    surface_name: str = None,
    attribute: str = None,
    time_interval: list = [],
    iteration_name: str = "iter-0",
    operation: str = "mean",
):
    surface_id = get_surface_id(
        sumo_explorer=sumo_explorer,
        case_name=case_name,
        surface_type="aggregation",
        surface_name=surface_name,
        attribute=attribute,
        time_interval=time_interval,
        iteration_name=iteration_name,
        aggregation=operation,
    )

    return surface_id


def get_polygon_name(sumo_polygons, surface_name):

    for polygon in sumo_polygons:
        if polygon.name == surface_name and "fault" in polygon.tag_name:
            return polygon.name

        else:
            for polygon in sumo_polygons:
                if surface_name.lower() in polygon.name.lower():
                    return polygon.name

    return None


def get_iteration_id(iterations, iteration_name):
    for iteration in iterations:
        iter_name = iteration.get("name")
        if iter_name == iteration_name:
            iter_id = iteration.get("id")
            return iter_id

    return


def print_sumo_objects(sumo_objects):
    if len(sumo_objects) > 0:
        index = 0
        for sumo_object in sumo_objects:
            # index = sumo_objects._curr_index(sumo_object)
            object_type = sumo_object._metadata.get("class")
            content = sumo_object._metadata.get("data").get("content")
            iteration = sumo_object._metadata.get("fmu").get("iteration")

            if iteration:
                iter_name = iteration.get("name")
            else:
                iter_name = None

            realization = sumo_object._metadata.get("fmu").get("realization")

            if realization:
                real_name = realization.get("name")
            else:
                real_name = None

            aggregation = sumo_object._metadata.get("fmu").get("aggregation")

            if aggregation:
                operation = aggregation.get("operation")
            else:
                operation = None

            time = sumo_object._metadata.get("data").get("time")
            time_list = decode_time_interval(time)

            print(
                "  ",
                index,
                object_type,
                sumo_object.name,
                sumo_object.tagname,
                "time=",
                time_list,
                "operation=",
                operation,
                "iter=",
                iter_name,
                "real=",
                real_name,
            )
            index += 1
    else:
        "WARNING: No SUMO objects"
