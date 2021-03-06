import io
import glob
import xtgeo
import json
import pandas as pd
import yaml
import os
import statistics
from pandas import json_normalize
from webviz_config.common_cache import CACHE
from pathlib import Path
from webviz_4d._datainput.common import find_files


def load_well(well_path):
    """ Return a well object (xtgeo) for a given file (RMS ascii format) """
    return xtgeo.Well(well_path, mdlogname="MD")
    
    
def load_all_wells(wellfolder, wellsuffix):
    """ For all wells in a folder return
        - a list of dataframes with the well trajectories
        - dataframe with metadata for all the wells 
        - a dataframe with production/injection depths (screens or perforated) """
    all_wells_list = []

    print("Loading wells from " + str(wellfolder) + " ...")

    wellfiles = (
        json.load(find_files(wellfolder, wellsuffix))
        if wellfolder is not None
        else None
    )

    if not wellfiles:
        raise Exception("No wellfiles found")

    for wellfile in wellfiles:
        # print(wellfile + " ...")
        try:
            well = load_well(wellfile)
            # print("    - loaded")
        except ValueError:
            continue
        well.dataframe = well.dataframe[["X_UTME", "Y_UTMN", "Z_TVDSS", "MD"]]
        well.dataframe["WELLBORE_NAME"] = well.name
        all_wells_list.append(well.dataframe)

    all_wells_df = pd.concat(all_wells_list)

    _well_info, depths_df = extract_well_metadata(wellfolder)
    metadata_file = os.path.join(wellfolder, "wellbore_info.csv")
    metadata = pd.read_csv(metadata_file)
    # print('load_all_wells ',metadata)

    return (all_wells_df, metadata, depths_df)
    

def extract_well_metadata(directory):
    """ Compile all metadata for wells in a given folder (+ sub-folders) """
    well_info = []
    depth_info = []

    yaml_files = glob.glob(str(directory) + "/**/.*.yaml", recursive=True)

    for yaml_file in yaml_files:
        with open(yaml_file, "r") as stream:
            data = yaml.safe_load(stream)
            # print('data',data)

            well_info.append(data[0])

            if len(data) > 1 and data[1]:
                for item in data[1:]:
                    depth_info.append(item)

    well_info_df = json_normalize(well_info)

    depth_df = json_normalize(depth_info)

    if not depth_df.empty:
        depth_df.sort_values(by=["interval.wellbore", "interval.mdTop"], inplace=True)

    return well_info_df, depth_df


def load_all_wells(wellfolder, wellsuffix):
    all_wells_list = []

    wellfiles = (
        json.load(find_files(wellfolder, wellsuffix))
        if wellfolder is not None
        else None
    )

    if not wellfiles:
        print("ERROR: No well files found in folder", wellfolder)
        return None, None, None

    print("Loading wells from " + str(wellfolder) + " ...")
    for wellfile in wellfiles:
        try:
            well = load_well(wellfile)
        except ValueError:
            continue

        if "MD" in well.dataframe.columns:
            well.new_df = well.dataframe[["X_UTME", "Y_UTMN", "Z_TVDSS", "MD"]]
        else:
            qc = well.geometrics()

            if qc:
                well.new_df = well.dataframe.rename(columns={"Q_MDEPTH": "MD"})
            else:
                print("ERROR: Measured depth values not found in well:", well.name)
                well.new_df = pd.DataFrame()

        if not well.new_df.empty:
            well.dataframe = well.new_df[["X_UTME", "Y_UTMN", "Z_TVDSS", "MD"]]
            well.dataframe["WELLBORE_NAME"] = well.name
            all_wells_list.append(well.dataframe)

    all_wells_df = pd.concat(all_wells_list)

    well_info, interval_df = extract_well_metadata(wellfolder)

    try:
        metadata_file = os.path.join(wellfolder, "wellbore_info.csv")
        metadata = pd.read_csv(metadata_file)
    except:
        metadata = None
    # print('load_all_wells ',metadata)

    return (all_wells_df, metadata, interval_df)


def get_position_data(well_dataframe, md_start, md_end):
    """ Return x- and y-values for a well between given depths """
    
    well_dataframe = well_dataframe[well_dataframe["MD"] >= md_start]
    
    if md_end:
        well_dataframe = well_dataframe[well_dataframe["MD"] <= md_end]
        
    positions = well_dataframe[["X_UTME", "Y_UTMN"]].values

    return positions


def get_well_polyline(
    wellbore, short_name, well_dataframe, well_type, fluid, info, md_start, md_end, selection, colors
): 
    """ Extract polyline data - well trajectory, color and tooltip """
    color = "black"
    
    if colors:
        color = colors["default"]

    tooltip = str(short_name) + " : " + well_type

    status = False

    if fluid and not pd.isna(fluid):
        tooltip = tooltip + " (" + info + ")"

    if selection:
        if (
            ("reservoir" in selection)
            and not pd.isna(fluid)
            and md_start > 0
        ):
            positions = get_position_data(well_dataframe, md_start, md_end)
            status = True

        elif selection == "planned" and well_type == selection:
            if colors:
                color = colors[selection]

            positions = get_position_data(well_dataframe, md_start, md_end)
            status = True

        elif well_type == selection and not pd.isna(fluid) and md_start > 0:
            ind = fluid.find(",")

            if ind > 0:
                fluid = "mixed"

            if colors:
                color = colors[fluid + "_" + selection]

            positions = get_position_data(well_dataframe, md_start, md_end)
            status = True

        elif pd.isna(fluid):
            positions = get_position_data(well_dataframe, md_start, md_end)
            status = True
            
        elif selection == "active": 
            positions =get_position_data(well_dataframe, md_start, md_end)
            status = True  
            
        elif selection == "production_start": 
            positions =get_position_data(well_dataframe, md_start, md_end)
            color = colors[fluid + "_production"]
            status = True  
            
        elif selection == "production_completed": 
            positions =get_position_data(well_dataframe, md_start, md_end)
            color = colors[fluid + "_production"]
            status = True      
            
        elif selection == "injection_start": 
            positions =get_position_data(well_dataframe, md_start, md_end)
            color = colors[fluid + "_injection"]
            status = True  
            
        elif selection == "injection_completed": 
            positions =get_position_data(well_dataframe, md_start, md_end)
            color = colors[fluid + "_injection"]
            status = True              

    else:
        positions = get_position_data(well_dataframe, md_start, md_end)
        status = True

    if status:
        return {
            "type": "polyline",
            "color": color,
            "positions": positions,
            "tooltip": tooltip,
        }


def filter_well_layer(well_layer, limit):
    filtered_data = []
    data = well_layer["data"]
    volumes = []
    
    for item in data:
        tooltip = item["tooltip"]
        content = tooltip.split(":")
        wellbore_name = content[0]
        index = content[1].find("oil")
        volume_info = content[1][index:].split()
        fluid = volume_info[0]
        volume = float(volume_info[1])
        unit = volume_info[2]
        volumes.append(volume)
    
    limit = statistics.mean(volumes)
    print(limit)
        
    for item in data:
        tooltip = item["tooltip"]
        content = tooltip.split(":")
        wellbore_name = content[0]
        index = content[1].find("oil")
        volume_info = content[1][index:].split()
        fluid = volume_info[0]
        volume = float(volume_info[1])
        unit = volume_info[2]   
         
        if volume > limit:
            color = item["color"]
            positions = item["positions"]
            polyline_data = {
                "type": "polyline",
                "color": color,
                "positions": positions,
                "tooltip": tooltip,
            }
            filtered_data.append(polyline_data)
      
    label = "Producers - filtered (" + str(int(limit)) + unit + ")"
                   
    return {"name": label, "checked": False, "base_layer": False, "data": filtered_data}

@CACHE.memoize(timeout=CACHE.TIMEOUT)
def make_new_well_layer(
    interval,
    wells_df,
    metadata_df,
    colors=None,
    selection=None,
    label="Drilled wells",
):
    """Make layeredmap wells layer"""
    data = []
    if colors is None:
        color = "black"

    wellbores = wells_df["WELLBORE_NAME"].values
    list_set = set(wellbores)
    # convert the set to the list
    unique_wellbores = list(list_set)

    for wellbore in unique_wellbores:
        # print('wellbore ',wellbore)
        md_start = 0
        md_end = None
        fluid = ""
        info = ""
        short_name = wellbore
        well_type = ""
        polyline_data = None

        well_dataframe = wells_df[wells_df["WELLBORE_NAME"] == wellbore]

        if not metadata_df is None:
            well_metadata = metadata_df[metadata_df["wellbore.rms_name"] == wellbore]
            # print(well_metadata)

            md_top_res = well_metadata["wellbore.pick_md"].values
            if selection and len(md_top_res) > 0:
                md_start = min(md_top_res)

            short_name = well_metadata["wellbore.short_name"].values
            if short_name:
                short_name = short_name[0]

            well_type = well_metadata["wellbore.type"].values
            if well_type:
                well_type = well_type[0]

            if well_type == "planned":
                # info = well_metadata["wellbore.list_name"].values
                info = ""
                start_date = None
                stop_date = None
            else:
                info = well_metadata["wellbore.fluids"].values

        if info:
            info = info[0]

        plot = False
        if (
            selection
            and well_type == selection
            and (selection == "production" or selection == "injection")
        ):
            if interval and not pd.isna(start_date) and not pd.isna(stop_date):
                interval_start = interval[0:4] + interval[5:7] + interval[8:10]
                interval_stop = interval[11:15] + interval[16:18] + interval[19:21]

                if interval_start >= start_date and interval_start <= stop_date:
                    plot = True

                elif interval_stop >= start_date and interval_stop <= stop_date:
                    plot = True

                if plot:

                    polyline_data = get_well_polyline(
                        wellbore,
                        short_name,
                        well_dataframe,
                        well_type,
                        fluid,
                        info,
                        md_start,
                        md_end,
                        selection,
                        colors,
                    )
        elif selection == "reservoir_section" or selection == "planned":
            polyline_data = get_well_polyline(
                wellbore,
                short_name,
                well_dataframe,
                well_type,
                fluid,
                info,
                md_start,
                md_end,
                selection,
                colors,
            )
        elif not selection:
            polyline_data = get_well_polyline(
                wellbore,
                short_name,
                well_dataframe,
                well_type,
                fluid,
                info,
                md_start,
                md_end,
                selection,
                colors,
            )

        if polyline_data:
            data.append(polyline_data)

    return {"name": label, "checked": False, "base_layer": False, "data": data}
    
