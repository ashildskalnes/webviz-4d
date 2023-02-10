import os
import pandas as pd
import json
import fmu.sumo.explorer._utils as explorer_utils
from webviz_config.common_cache import CACHE
from webviz_4d._datainput._sumo import print_sumo_objects


supported_polygons = {
    "owc_outline": "Initial OWC",
    "goc_outline": "Initial GOC",
    "faults": "Faults",
    "prm_receivers": "PRM receivers",
    "sumo_faults": "Faults",
    "sumo_goc_outline": "Initial GOC",
    "sumo_fwl_outline": "Initial FWL",
    "sumo_outline": "Initial FWL",
}
default_colors = {
    "owc_outline": "blue",
    "goc_outline": "red",
    "faults": "gray",
    "prm_receivers": "darkgray",
    "sumo_faults": "gray",
    "sumo_goc_outline": "red",
    "sumo_fwl_outline": "blue",
}

checked = {
    "Initial FWL": True,
    "Initial OWC": True,
    "Initial GOC": True,
    "Faults": True,
    "PRM receivers": False,
}

key_list = list(supported_polygons.keys())
val_list = list(supported_polygons.values())


def get_position_data(polyline):
    """Return x- and y-values for a selected polygon"""
    positions_txt = polyline["coordinates"]
    positions = json.loads(positions_txt)

    return positions


def get_fault_polyline(fault, tooltip, color):
    """Create polyline data - fault polylines, color and tooltip"""
    if color is None:
        color = default_colors.get("faults")

    positions = get_position_data(fault)

    if positions:
        return {
            "type": "polyline",
            "color": color,
            "positions": positions,
            "tooltip": tooltip,
        }


def get_prm_polyline(prm, color):
    """Create polyline data - prm receiver polylines, color and tooltip"""
    if color is None:
        color = default_colors.get("prm_receivers")

    positions = get_position_data(prm)
    year = prm["year"]
    line = prm["line"]
    tooltip = str(year) + "-line " + str(line)

    if positions:
        return {
            "type": "polyline",
            "color": color,
            "positions": positions,
            "tooltip": tooltip,
        }


def get_sumo_polyline(sumo_layer, name, color):
    """Create polyline data - SUMO faults, color and tooltip"""
    if color is None:
        color = default_colors.get("sumo_faults")

    positions = sumo_layer["coordinates"]
    tooltip = name

    if positions:
        return {
            "type": "polyline",
            "color": color,
            "positions": positions,
            "tooltip": tooltip,
        }


def get_contact_polyline(contact, key, color):
    """Create polyline data - owc polyline, color and tooltip"""
    data = []
    tooltip = supported_polygons[key]
    label = supported_polygons[key]

    coordinates_txt = contact["coordinates"][0]
    coordinates = json.loads(coordinates_txt)

    if color is None:
        position = val_list.index(label)
        key = key_list[position]
        color = default_colors.get(key)

    for i in range(0, len(coordinates)):
        positions = coordinates[i]

        if positions:
            polyline_data = {
                "type": "polyline",
                "color": color,
                "positions": positions,
                "tooltip": tooltip,
            }

            data.append(polyline_data)

    return data


def make_new_polyline_layer(dataframe, key, label, color):
    """Make layeredmap fault layer"""
    data = []
    name = supported_polygons.get(key)

    if dataframe.empty:
        return None

    if "sumo" in key:
        for _index, row in dataframe.iterrows():
            polyline_data = get_sumo_polyline(row, label, color)

            if polyline_data:
                data.append(polyline_data)

    elif "outline" in key:
        data = get_contact_polyline(dataframe, key, color)
    elif key == "prm_receivers":
        for _index, row in dataframe.iterrows():
            polyline_data = get_prm_polyline(row, color)

            if polyline_data:
                data.append(polyline_data)

    else:
        for _index, row in dataframe.iterrows():
            polyline_data = get_fault_polyline(row, label, color)

            if polyline_data:
                data.append(polyline_data)
    # else:
    #     print("WARNING: Unknown polygon type", key)

    if data:
        checked_state = checked.get(name)
        layer = {
            "name": name,
            "checked": checked_state,
            "base_layer": False,
            "data": data,
        }
    else:
        layer = None

    return layer


def load_polygons(csv_files, polygon_colors):
    polygon_layers = []

    for csv_file in csv_files:
        polygon_df = pd.read_csv(csv_file)
        name = polygon_df["name"].unique()[0]
        file_name = os.path.basename(csv_file)

        if name in supported_polygons.keys() or file_name in supported_polygons.keys():
            default_color = default_colors.get(name)

            if polygon_colors:
                color = polygon_colors.get(name, default_color)
            else:
                color = default_color

            polygon_layer = make_new_polyline_layer(polygon_df, name, name, color)
            polygon_layers.append(polygon_layer)

    return polygon_layers


def load_zone_polygons(csv_files, polygon_colors):
    polygon_layers = []

    for csv_file in csv_files:
        polygon_df = pd.read_csv(csv_file)
        label = os.path.basename(csv_file).replace(".csv", "")
        print("  ", label)

        name = "faults"
        default_color = default_colors.get(name)

        if polygon_colors:
            color = polygon_colors.get(name, default_color)
        else:
            color = default_color

        polygon_layer = make_new_polyline_layer(polygon_df, name, label, color)
        polygon_layers.append(polygon_layer)

    return polygon_layers


def get_zone_layer(polygon_layers, zone_name):
    if polygon_layers is not None:
        for layer in polygon_layers:
            data = layer["data"]
            tooltip = data[0]["tooltip"]

            if tooltip == zone_name:
                return layer

    return None


def create_sumo_layer(polygon_df):
    """Create sumo layer"""
    all_positions = []
    positions = []
    ids = []

    for _index, row in polygon_df.iterrows():
        position = [row["X"], row["Y"]]

        if _index == 0:
            poly_id = row["ID"]

        if row["ID"] == poly_id:
            position = [row["X"], row["Y"]]
            positions.append(position)
        else:
            all_positions.append(positions)
            positions = []
            poly_id = row["ID"]
            position = [row["X"], row["Y"]]
            positions.append(position)
            ids.append(poly_id)

    # Add last line
    all_positions.append(positions)
    ids.append(poly_id)

    layer_df = pd.DataFrame()
    layer_df["id"] = ids
    layer_df["geometry"] = "Polygon"
    layer_df["coordinates"] = all_positions

    return layer_df


def load_sumo_polygons(polygons, polygon_colors):

    polygon_layers = []

    for polygon in polygons:
        if "fault" in polygon.tagname:
            name = "sumo_faults"
        elif "outline" in polygon.tagname and "GOC" in polygon.tagname:
            name = "sumo_goc_outline"
        elif "outline" in polygon.tagname and "FWL" in polygon.tagname:
            name = "sumo_fwl_outline"
        elif "outline" in polygon.tagname:
            name = "sumo_outline"
        else:
            print("WARNING: Unknown polygon type", polygon.tagname)
            return None

        default_color = default_colors.get(name)

        if polygon_colors:
            color = polygon_colors.get(name, default_color)
        else:
            color = default_color

        # print("DEBUG:", polygon.tagname, name, color)

        polygon_df = create_sumo_layer(polygon.to_dataframe())
        polygon_layer = make_new_polyline_layer(
            polygon_df, name, polygon.tagname, color
        )

        if polygon_layer is not None:
            polygon_layers.append(polygon_layer)

    return polygon_layers


def load_sumo_fault_polygon(polygon, polygon_colors):
    polygon_layer = None
    name = "sumo_faults"
    default_color = default_colors.get(name)

    if polygon_colors:
        color = polygon_colors.get(name, default_color)
    else:
        color = default_color

    print("  ", polygon.name, polygon.tagname)

    polygon_df = polygon.to_dataframe()
    polygon_layer = make_new_polyline_layer(polygon_df, name, polygon.tagname, color)

    return polygon_layer


def get_fault_polygon_tag(polygons):
    fault_tag = None

    for polygon in polygons:
        # print(polygon.name, polygon.tagname)

        if "fault" in polygon.tagname:
            return polygon.tagname

    if fault_tag is None:
        print("WARNING: No fault polygons found")

    return fault_tag
