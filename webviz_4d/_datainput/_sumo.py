import pandas as pd

from fmu.sumo.explorer.objects.case import Case
from fmu.sumo.explorer.objects.polygons_collection import PolygonsCollection
from fmu.sumo.explorer.timefilter import TimeType, TimeFilter
from webviz_4d._datainput._metadata import get_realization_id


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
    val0 = False
    val1 = False

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
        if tag_name == "name":
            tag = surface.name
        elif tag_name == "attribute":
            tag = surface.tagname
        elif tag_name == "aggregation":
            aggregation = surface._metadata.get("fmu").get("aggregation")

            if aggregation is not None:
                tag = surface._metadata.get("fmu").get("aggregation").get("operation")
            else:
                tag = None
        elif tag_name == "time":
            time = surface._metadata.get("data").get("time")

            if time is not None:
                tag = decode_time_interval(time)
            else:
                tag = None
        elif tag_name == "time_interval":
            time = surface._metadata.get("data").get("time")
            time_list = decode_time_interval(time)

            if time_list[1]:
                if time_list[0] > time_list[1]:
                    time_string = time_list[0] + "-" + time_list[1]
                else:
                    time_string = time_list[1] + "-" + time_list[0]
                tag = time_string
            else:
                tag = None

        if tag is not None and tag not in tag_list:
            tag_list.append(tag)

    # unique_tags = list(set(tag_list))

    return tag_list


def create_selector_lists(my_case, mode):
    iterations = my_case.iterations
    print(iterations)

    iteration_names = []
    for iteration in iterations:
        iteration_names.append(iteration.get("name"))

    iter_name = iteration_names[0]

    # time filter for intervals
    time = TimeFilter(time_type=TimeType.INTERVAL)

    realization_surfaces = my_case.surfaces.filter(
        stage="realization", iteration=iter_name, time=time
    )
    print("Surfaces in iteration:", iter_name, len(realization_surfaces))
    realizations = realization_surfaces.realizations

    sorted_realizations = []

    for realization in sorted(realizations):
        sorted_realizations.append("realization-" + str(realization))

    map_types = ["observed", "simulated", "aggregated"]
    map_dict = {}
    default = "---"

    statistics = []

    for map_type in map_types:
        map_dict[map_type] = {}
        map_type_dict = {}

        if map_type == "observed":
            surfaces = my_case.surfaces.filter(stage="case")
        elif map_type == "simulated":
            surfaces = realization_surfaces
        elif map_type == "aggregated":
            surfaces = my_case.surfaces.filter(stage="iteration", iteration=iter_name)
        else:
            print("ERROR: Not supported map_type", map_type)
            return None

        # print(map_type)
        # print("  all surfaces:", len(surfaces))

        if mode == "timelapse":
            timelapse_surfaces = surfaces.filter(time=time)
            surfaces = timelapse_surfaces
            print(map_type, "- timelapse surfaces:", len(timelapse_surfaces))

        names = surfaces.names
        attributes = surfaces.tagnames
        time_intervals = convert_intervals(surfaces.intervals)

        map_type_dict["name"] = sorted(names)
        map_type_dict["attribute"] = sorted(attributes)
        map_type_dict["interval"] = sorted(time_intervals)

        if map_type == "simulated":
            map_type_dict["ensemble"] = sorted(iteration_names)
            map_type_dict["realization"] = sorted_realizations
        elif map_type == "aggregated":
            statistics = sorted(surfaces.aggregations)
            map_type_dict["aggregation"] = statistics
        else:
            map_type_dict["ensemble"] = [default]
            map_type_dict["realization"] = [default]

        map_dict[map_type] = map_type_dict

    return map_dict


def convert_intervals(intervals):
    converted_intervals = []

    for interval in intervals:
        time1 = interval[0][:10]
        time2 = interval[1][:10]

        converted_intervals.append(time2 + "-" + time1)

    return converted_intervals


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


def time_filter(surfaces, time_interval):
    for surface in surfaces:
        time = surface._metadata.get("data").get("time")
        time_list = decode_time_interval(time)

        if time_list == time_interval:
            return surface

    return None


def get_observed_surface(
    case: Case = None,
    surface_name: str = None,
    attribute: str = None,
    time_interval: list = [],
):
    time = create_time_filter(time_interval, True)
    surfaces = case.surfaces.filter(
        stage="case", name=surface_name, tagname=attribute, time=time
    )

    if len(surfaces) == 1:
        selected_surface = surfaces[0]
    else:
        print("WARNING: Number of surfaces found =", str(len(surfaces)))
        selected_surface = None

    return selected_surface


def get_realization_surface(
    case: Case = None,
    surface_name: str = None,
    attribute: str = None,
    time_interval: list = [],
    iteration_name: str = "iter-0",
    realization: int = 0,
):
    time = create_time_filter(time_interval, True)

    selected_surfaces = case.surfaces.filter(
        stage="realization",
        name=surface_name,
        tagname=attribute,
        time=time,
        iteration=iteration_name,
        realization=realization,
    )

    if len(selected_surfaces) == 1:
        selected_surface = selected_surfaces[0]
    else:
        print("WARNING: Number of surfaces found =", str(len(selected_surfaces)))
        selected_surface = None

    return selected_surface


def get_aggregated_surface(
    case: Case = None,
    surface_name: str = None,
    attribute: str = None,
    time_interval: list = [],
    iteration_name: str = "iter-0",
    operation: str = "mean",
):
    time = create_time_filter(time_interval, True)

    selected_surfaces = case.surfaces.filter(
        name=surface_name,
        tagname=attribute,
        time=time,
        iteration=iteration_name,
        aggregation=operation,
    )

    if len(selected_surfaces) == 1:
        selected_surface = selected_surfaces[0]
    else:
        print("WARNING: Number of surfaces found =", str(len(selected_surfaces)))
        selected_surface = None

    return selected_surface


def create_time_filter(time_interval, exact):
    if len(time_interval) == 0:
        time = TimeFilter(time_type=TimeType.NONE)
    elif len(time_interval) == 1:
        time = TimeFilter(
            time_type=TimeType.TIMESTAMP, start=time_interval[0], exact=exact
        )
    elif len(time_interval) == 2:
        if time_interval[0] is False and time_interval[1] is False:
            time = TimeFilter(time_type=TimeType.NONE)
        elif time_interval[1] is False:
            time = TimeFilter(
                time_type=TimeType.TIMESTAMP, start=time_interval[0], exact=exact
            )
        else:
            time = TimeFilter(
                time_type=TimeType.INTERVAL,
                start=time_interval[0],
                end=time_interval[1],
                exact=exact,
            )
    else:
        print("ERROR: Wrong time interval specification:", time_interval)
        time = None

    return time


def get_observed_cube(
    case: Case = None,
    name: str = None,
    tagname: str = None,
    time_interval: list = [],
):
    time = create_time_filter(time_interval, True)
    cubes = case.cubes.filter(stage="case", name=name, tagname=tagname, time=time)

    if len(cubes) == 1:
        selected_cube = cubes[0]
    else:
        print("WARNING: Number of seismic cubes found =", str(len(cubes)))
        selected_cube = None

    return selected_cube


def get_observed_cubes(
    case: Case = None,
    names: str = [],
    tagnames: str = [],
    time_interval: list = [],
    iterations: list = [],
    realizations: list = [],
):
    time = create_time_filter(time_interval, True)

    if iterations is None and realizations is None:
        selected_cubes = case.cubes.filter(
            stage="case", name=names, tagname=tagnames, time=time
        )
    else:
        cubes = case.cubes.filter(
            names=names,
            tagnames=tagnames,
            time_interval=time,
            iterations=iterations,
            realizations=realizations,
        )
        selected_cubes = []

        for cube in cubes:
            if cube.metadata.get("is_observation"):
                selected_cubes.append(cube)

    return selected_cubes


def get_realization_cube(
    case: Case = None,
    name: str = None,
    tagname: str = None,
    time_interval: list = [],
    iteration_name: str = "iter-0",
    realization: int = 0,
):
    time = create_time_filter(time_interval, True)

    selected_cubes = case.cubes.filter(
        stage="realization",
        name=name,
        tagname=tagname,
        time=time,
        iteration=iteration_name,
        realization=realization,
    )

    if len(selected_cubes) == 1:
        selected_cube = selected_cubes[0]
    else:
        print("WARNING: Number of seismic cubes found =", str(len(selected_cubes)))

        if len(selected_cubes) > 1:
            print_sumo_objects(selected_cubes)

        selected_cube = None

    return selected_cube


def get_polygon_name(sumo_polygons, surface_name):
    # Check if there exists a polygon with the same name as the surface
    name = None

    for polygon in sumo_polygons:
        if polygon.name.lower() == surface_name.lower():
            name = polygon.name
        elif surface_name.lower() in polygon.name.lower():
            name = polygon.name

    return name


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
        names = []
        tagnames = []

        for sumo_object in sumo_objects:
            # object_type = sumo_object._metadata.get("class")
            content = sumo_object._metadata.get("data").get("content")
            iteration = sumo_object.iteration
            realization = sumo_object.realization

            aggregation = sumo_object._metadata.get("fmu").get("aggregation")

            if aggregation:
                operation = aggregation.get("operation")
            else:
                operation = None

            time = sumo_object._metadata.get("data").get("time")
            time_list = decode_time_interval(time)

            names.append(sumo_object.name)
            tagnames.append(sumo_object.tagname)

            print(
                "  ",
                index,
                sumo_object.name,
                sumo_object.tagname,
                "content=",
                content,
                "time=",
                time_list,
                "operation=",
                operation,
                "iter=",
                iteration,
                "real=",
                realization,
            )
            index += 1

        df = pd.DataFrame()
        df["Name"] = names
        df["Tagname"] = tagnames

    else:
        "WARNING: No SUMO objects"


def check_timelapse(surfaces, times):
    timelapse_times = []

    for time in times:
        if time[0] and time[1]:
            timelapse_times.append(time)

    timelapse_surfaces = []
    for surface in surfaces:
        surface_time = get_tag_values([surface], "time")

        if len(surface_time) > 0 and surface_time[0] in timelapse_times:
            timelapse_surfaces.append(surface)

    return timelapse_surfaces


def get_sumo_top_res_surface(my_case, shared_settings):
    top_res_surface = shared_settings.get("top_res_surface")
    iter_name = my_case.iterations[0].get("name")

    if top_res_surface is not None:
        name = top_res_surface.get("name")
        tagname = top_res_surface.get("tag_name")
        time_interval = [False, False]

        surface = get_realization_surface(
            case=my_case,
            surface_name=name,
            attribute=tagname,
            time_interval=time_interval,
            iteration_name=iter_name,
        )

    if surface:
        return surface.to_regular_surface()
    else:
        print(
            "ERROR: Top reservoir surface not loaded from SUMO:",
            name,
            tagname,
            time_interval,
        )
        return None


def open_surface_with_xtgeo(surface):
    if surface:
        surface_object = surface.to_regular_surface()
    else:
        surface_object = None
        print("ERROR: non-existing surface")

    return surface_object


def get_sumo_interval_list(interval):
    t1 = interval[-10:]
    t2 = interval[:10]

    if t1 < t2:
        interval_list = [t1, t2]
    else:
        interval_list = [t2, t1]

    return interval_list


def get_selected_surface(
    case: Case = None,
    map_type: str = "",
    surface_name: str = "",
    attribute: str = "",
    time_interval: list = [],
    iteration_name: str = "iter-0",
    realization: str = "",
):
    if map_type == "observed":
        surface = get_observed_surface(
            case=case,
            surface_name=surface_name,
            attribute=attribute,
            time_interval=time_interval,
        )
    elif (
        map_type == "simulated" and "realization" not in realization
    ):  # aggregated surface
        surface = get_aggregated_surface(
            case=case,
            surface_name=surface_name,
            attribute=attribute,
            time_interval=time_interval,
            iteration_name=iteration_name,
            operation=realization,
        )

    else:
        realization_split = realization.split("-")
        real_id = realization_split[1]

        surface = get_realization_surface(
            case=case,
            surface_name=surface_name,
            attribute=attribute,
            time_interval=time_interval,
            iteration_name=iteration_name,
            realization=real_id,
        )

    if surface is not None:
        surface = surface.to_regular_surface()

    return surface


def get_sumo_zone_polygons(
    case: Case = None,
    sumo_polygons: PolygonsCollection = None,
    polygon_settings: str = "",
    map_type: str = "",
    surface_name: str = "",
    iteration_name: str = "",
    realization: str = "",
):
    default_polygon_name = polygon_settings.get("name")
    default_polygon_iter = polygon_settings.get("iter")
    default_polygon_real = polygon_settings.get("real")

    polygon_usage = polygon_settings.get("polygon_usage")

    zones_settings = polygon_usage.get("zones")
    polygon_iteration = None
    polygon_realization = None
    polygons = []

    polygon_name = get_polygon_name(sumo_polygons, surface_name)

    if polygon_name is None and zones_settings:
        polygon_name = default_polygon_name

    if polygon_name:
        if map_type == "simulated":
            if "realization" in realization:
                polygon_iteration = iteration_name
                polygon_realization = realization
            else:
                aggregated_settings = polygon_usage.get("aggregated")

                if aggregated_settings:
                    polygon_iteration = iteration_name
                    polygon_realization = default_polygon_real
        else:  # map_type == observed
            observed_settings = polygon_usage.get("observed")

            if observed_settings:
                polygon_iteration = default_polygon_iter
                polygon_realization = default_polygon_real

        if polygon_iteration:
            polygon_real_id = get_realization_id(polygon_realization)

            polygons = case.polygons.filter(
                name=polygon_name,
                iteration=polygon_iteration,
                realization=polygon_real_id,
            )

    return polygons
