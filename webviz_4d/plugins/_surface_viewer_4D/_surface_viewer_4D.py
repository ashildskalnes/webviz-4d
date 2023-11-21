import sys
from typing import List, Tuple, Callable
from pathlib import Path
import datetime
import json
import os
import numpy as np
import pandas as pd
import logging
from pprint import pprint

from fmu.sumo.explorer import Explorer

from webviz_config import WebvizPluginABC

from webviz_4d._datainput._surface import (
    make_surface_layer,
    load_surface,
    get_top_res_surface,
)

from webviz_4d._datainput.common import (
    read_config,
    get_well_colors,
    get_update_dates,
    get_plot_label,
    get_dates,
    get_default_interval,
)

from webviz_4d._datainput.well import (
    load_all_wells,
    load_smda_metadata,
    load_smda_wellbores,
    load_planned_wells,
    load_pdm_info,
    create_basic_well_layers,
    get_surface_picks,
    create_production_layers,
)
from webviz_4d._datainput._production import (
    make_new_well_layer,
)

from webviz_4d._private_plugins.surface_selector import SurfaceSelector
from webviz_4d._datainput._colormaps import load_custom_colormaps
from webviz_4d._datainput._polygons import (
    load_polygons,
    load_zone_polygons,
    get_zone_layer,
    load_sumo_polygons,
    get_fault_polygon_tag,
)

from webviz_4d._datainput._metadata import (
    get_all_map_defaults,
)

from webviz_4d._datainput._sumo import (
    create_selector_lists,
    get_sumo_interval_list,
    get_selected_surface,
    get_sumo_zone_polygons,
)

from webviz_4d._providers.wellbore_provider._provider_impl_file import (
    ProviderImplFile,
)

from webviz_4d._datainput._auto4d import create_auto4d_lists, load_metadata

from ._webvizstore import read_csv, read_csvs, find_files, get_path
from ._callbacks import (
    set_first_map,
    set_second_map,
    set_third_map,
    change_maps_from_button,
)
from ._layout import set_layout

import warnings

warnings.filterwarnings("ignore")


class SurfaceViewer4D(WebvizPluginABC):
    """### SurfaceViewer4D"""

    def __init__(
        self,
        app,
        well_folder: Path = None,
        production_data: Path = None,
        polygons_folder: Path = None,
        colormaps_folder: Path = None,
        map1_defaults: dict = None,
        map2_defaults: dict = None,
        map3_defaults: dict = None,
        map_suffix: str = ".gri",
        default_interval: str = None,
        settings_file: Path = None,
        delimiter: str = "--",
        surface_metadata_file: Path = None,
        attribute_maps_file: Path = None,
        interval_mode: str = "normal",
        selector_file: Path = None,
    ):
        super().__init__()
        logging.getLogger("").setLevel(level=logging.WARNING)
        self.shared_settings = app.webviz_settings["shared_settings"]
        self.fmu_directory = self.shared_settings["fmu_directory"]
        self.sumo_name = self.shared_settings.get("sumo_name")
        self.auto4d_directory = self.shared_settings.get("auto4d_directory")
        self.label = self.shared_settings.get("label", self.fmu_directory)

        self.basic_well_layers = self.shared_settings.get("basic_well_layers", None)
        self.additional_well_layers = self.shared_settings.get("additional_well_layers")
        self.top_res_surface_settings = self.shared_settings.get("top_reservoir")

        self.selector_file = selector_file
        self.production_data = production_data
        self.wellfolder = well_folder
        self.polygons_folder = polygons_folder
        self.colormaps_folder = colormaps_folder
        self.map_suffix = map_suffix
        self.interval_mode = interval_mode
        self.default_interval = default_interval

        settings_folder = os.path.dirname(os.path.abspath(settings_file))
        self.define_defaults()
        self.load_settings_info(settings_file)

        # Include custom colormaps if wanted
        self.get_additional_colormaps()

        # Read attribute maps settings (min-/max-values)
        self.attribute_maps_file = attribute_maps_file
        if self.attribute_maps_file is not None:
            self.colormap_settings = read_csv(csv_file=self.attribute_maps_file)
            print("Colormaps settings loaded from file", self.attribute_maps_file)

        # Get maps information
        if self.sumo_name:
            self.api_usage["sumo"] = True
            env = "prod"
            self.sumo = Explorer(env=env, keep_alive="20m")
            # self.sumo = Explorer(env=env)
            cases = self.sumo.cases.filter(name=self.sumo_name)

            if len(cases) == 1:
                self.my_case = cases[0]
            else:
                print("ERROR: Number of selected cases =", len(cases))
                print("       Execution stopped")
                exit(1)

            self.field_name = self.my_case.field
            self.label = "SUMO case: " + self.sumo_name
            self.iterations = self.my_case.iterations
            print("SUMO case:", self.my_case.name, self.field_name)

            # print("Create selection lists ...")
            # time_mode = "timelapse"
            # self.selection_list = create_selector_lists(
            #     self.my_case,
            #     time_mode,
            # )

            # if self.selection_list is None:
            #     sys.exit("ERROR: Sumo case doesn't contain any timelapse surfaces")
        if self.auto4d_directory:
            self.label = "auto4d maps: " + self.auto4d_directory
            print("auto4d maps:", self.auto4d_directory)
            self.surface_metadata = load_metadata(self.auto4d_directory)

            print("Create auto4d selection lists ...")
            self.selection_list = create_auto4d_lists(
                self.surface_metadata, interval_mode
            )

            if self.selection_list is None:
                sys.exit("ERROR: No timelapse surfaces found in", self.auto4d_directory)
        else:
            # Read maps metadata from file
            self.my_case = None
            self.surface_metadata_file = surface_metadata_file
            print("Reading maps metadata from", self.surface_metadata_file)
            self.surface_metadata = (
                read_csv(csv_file=self.surface_metadata_file)
                if self.surface_metadata_file is not None
                else None
            )

            self.selection_list = read_config(get_path(path=self.selector_file))

        self.top_res_surface = get_top_res_surface(
            self.top_res_surface_settings, self.my_case
        )

        if self.default_interval is None:
            map_types = ["observed", "simulated"]
            self.default_interval = get_default_interval(self.selection_list, map_types)

        map_default_list = [map1_defaults, map2_defaults, map3_defaults]
        self.map_defaults = get_all_map_defaults(self.selection_list, map_default_list)

        self.selected_intervals = [
            map1_defaults.get("interval"),
            map2_defaults.get("interval"),
            map3_defaults.get("interval"),
        ]

        # Load polygons
        if self.sumo_name:
            print("Polygons in SUMO ...")
            iter_name = self.top_res_surface_settings.get("iter")
            top_res_name = self.top_res_surface_settings.get("name")
            real = self.top_res_surface_settings.get("real")

            items = real.split("-")
            real_id = items[1]

            self.sumo_polygons = self.my_case.polygons.filter(
                iteration=iter_name, realization=real_id, name=top_res_name
            )

            for polygon in self.sumo_polygons:
                print("  - ", polygon.name, polygon.tagname)

            self.fault_polygon_tag = get_fault_polygon_tag(self.sumo_polygons)
        elif self.polygons_folder is not None:
            self.polygon_files = [
                get_path(Path(fn))
                for fn in json.load(find_files(self.polygons_folder, ".csv"))
            ]
            print("Reading polygons from:", self.polygons_folder)
            self.polygon_layers = load_polygons(self.polygon_files, self.polygon_colors)

            # Load zone fault if existing
            self.zone_faults_folder = Path(os.path.join(self.polygons_folder, "rms"))
            self.zone_faults_files = [
                get_path(Path(fn))
                for fn in json.load(find_files(self.zone_faults_folder, ".csv"))
            ]

            print("Reading zone polygons from:", self.zone_faults_folder)
            self.zone_polygon_layers = load_zone_polygons(
                self.zone_faults_files, self.polygon_colors
            )
        # Read update dates and well data
        #    self.drilled_wells_df: dataframe with wellpaths (x- and y positions) for all drilled wells
        #    self.drilled_wells_info: dataframe with metadata for all drilled wells

        if "SMDA" in str(self.wellfolder):
            omnia_env = ".omniaapi"
            home = os.path.expanduser("~")
            env_path = os.path.expanduser(os.path.join(home, omnia_env))
            self.smda_provider = ProviderImplFile(env_path, "SMDA")
            self.pozo_provider = ProviderImplFile(env_path, "POZO")
            self.pdm_provider = ProviderImplFile(env_path, "PDM")

            print("Loading drilled well data from SMDA ...")
            self.drilled_wells_info = load_smda_metadata(
                self.smda_provider, self.field_name
            )

            self.drilled_wells_df = load_smda_wellbores(
                self.smda_provider, self.field_name
            )

            self.surface_picks = get_surface_picks(
                self.drilled_wells_df, self.top_res_surface
            )

            if "planned" in self.basic_well_layers:
                print("Loading planned well data from POZO ...")
                planned_wells = load_planned_wells(self.pozo_provider, self.field_name)
                self.planned_wells_info = planned_wells.metadata.dataframe
                self.planned_wells_df = planned_wells.trajectories.dataframe

            self.planned_wells_info = pd.DataFrame()
            self.planned_wells_df = pd.DataFrame()

            self.well_basic_layers = create_basic_well_layers(
                self.basic_well_layers,
                self.planned_wells_info,
                self.planned_wells_df,
                self.drilled_wells_info,
                self.drilled_wells_df,
                self.surface_picks,
                self.well_colors,
            )

            self.pdm_wells_info = load_pdm_info(self.pdm_provider, self.field_name)
            pdm_wellbores = self.pdm_wells_info["WB_UWBI"].tolist()
            self.pdm_wells_df = self.drilled_wells_df[
                self.drilled_wells_df["unique_wellbore_identifier"].isin(pdm_wellbores)
            ]

            self.intervals = None
            self.interval_names = None
            self.all_interval_layers = []
            self.interval_well_layers = []

            self.well_update = str(datetime.date.today())
            self.production_update = str(datetime.date.today())

        if "SMDA" not in str(self.wellfolder):
            print("Loading well data from", self.wellfolder)

            self.wellbore_info = read_csv(
                csv_file=Path(self.wellfolder) / "wellbore_info.csv"
            )
            update_dates = get_update_dates(
                welldata=get_path(Path(self.wellfolder) / ".welldata_update.yaml"),
                productiondata=get_path(
                    Path(self.wellfolder) / ".production_update.yaml"
                ),
            )
            self.well_update = update_dates["well_update_date"]
            self.production_update = update_dates["production_last_date"]
            self.all_wells_info = read_csv(
                csv_file=Path(self.wellfolder) / "wellbore_info.csv"
            )

            self.all_wells_info["file_name"] = self.all_wells_info["file_name"].apply(
                lambda x: get_path(Path(x))
            )
            self.all_wells_df = load_all_wells(self.all_wells_info)
            self.drilled_wells_files = list(
                self.wellbore_info[self.wellbore_info["layer_name"] == "Drilled wells"][
                    "file_name"
                ]
            )
            self.drilled_wells_df = self.all_wells_df.loc[
                self.all_wells_df["layer_name"] == "Drilled wells"
            ]
            self.drilled_wells_info = self.all_wells_info.loc[
                self.all_wells_info["layer_name"] == "Drilled wells"
            ]
            self.pdm_wells_info = self.drilled_wells_info.loc[
                self.drilled_wells_info["wellbore.pdm_name"] != ""
            ]

            self.pdm_wells_df = load_all_wells(self.pdm_wells_info)

            self.well_layer_dir = Path(os.path.join(settings_folder, "well_layers"))
            layer_overview_file = get_path(
                Path(os.path.join(self.well_layer_dir, "well_layers.yaml"))
            )
            self.well_layers_overview = read_config(layer_overview_file)

            self.well_basic_layers = []
            self.all_interval_layers = []

            print("Loading all well layers ...")
            self.layer_files = []
            basic_layers = self.well_layers_overview.get("basic")

            for key, value in basic_layers.items():
                layer_file = get_path(
                    Path(os.path.join(self.well_layer_dir, "basic", value))
                )
                label = self.basic_well_layers.get(key)

                well_layer = make_new_well_layer(
                    layer_file,
                    self.all_wells_df,
                    label,
                )

                if well_layer:
                    self.well_basic_layers.append(well_layer)
                    self.layer_files.append(layer_file)

            self.intervals = self.well_layers_overview.get("additional")
            self.interval_names = []

            for interval in self.intervals:
                interval_layers = self.create_additional_well_layers(interval)
                self.all_interval_layers.append(interval_layers)
                self.interval_names.append(interval)

        if "PDM" in str(production_data):
            self.interval_well_layers = create_production_layers(
                field_name=self.field_name,
                pdm_provider=self.pdm_provider,
                interval_4d=self.default_interval,
                wellbore_trajectories=self.drilled_wells_df,
                surface_picks=self.surface_picks,
                layer_options=self.additional_well_layers,
                well_colors=self.well_colors,
                prod_interval="Day",
            )

            for interval_layer in self.interval_well_layers:
                data = interval_layer.get("data")
                print("  ", interval_layer.get("name"), len(data))
        else:
            # Find production and injection layers for the default interval
            if self.interval_names:
                index = self.interval_names.index(self.default_interval)
                self.interval_well_layers = self.all_interval_layers[index]
            else:
                self.interval_well_layers = None

        # Create selectors (attributes, names and dates) for all 3 maps
        self.selector = SurfaceSelector(
            app, self.selection_list, self.map_defaults[0], self.default_interval
        )
        self.selector2 = SurfaceSelector(
            app, self.selection_list, self.map_defaults[1], self.default_interval
        )
        self.selector3 = SurfaceSelector(
            app, self.selection_list, self.map_defaults[2], self.default_interval
        )
        self.set_callbacks(app)

    def define_defaults(self):
        self.number_of_maps = 3
        self.observations = "observed"
        self.simulations = "simulated"
        self.statistics = ["mean", "min", "max", "p10", "p50", "p90", "std"]
        self.wellsuffix = ".w"

        self.surface_layer = None
        self.colormap_settings = None
        self.attribute_settings = {}
        self.well_update = ""
        self.production_update = ""
        self.selected_names = [None, None, None]
        self.selected_attributes = [None, None, None]
        self.selected_ensembles = [None, None, None]
        self.selected_realizations = [None, None, None]
        self.well_base_layers = []
        self.interval_well_layers = {}
        self.polygon_layers = None
        self.zone_polygon_layers = None

        # Define default well layers
        default_basic_well_layers = {
            "planned": "Planned wells",
            "drilled_wells": "Drilled wells",
            "reservoir_section": "Reservoir sections",
            "active_production": "Current producers",
            "active_injection": "Current injectors",
        }

        if self.basic_well_layers is None:
            self.basic_well_layers = default_basic_well_layers

        self.api_usage = {
            "sumo": False,
            "pozo": False,
            "smda": False,
            "pdm": False,
            "ssdl": False,
        }

    def load_settings_info(self, settings_path):
        if settings_path:
            settings = read_config(get_path(path=settings_path))

            map_settings = settings.get("map_settings")
            self.attribute_settings = map_settings.get("attribute_settings")
            self.default_colormap = map_settings.get("default_colormap", "seismic")
            self.well_colors = get_well_colors(settings)
            self.polygon_colors = settings.get("polygon_colors")
            self.date_labels = settings.get("date_labels")

    def get_additional_colormaps(self):
        if self.colormaps_folder is not None:
            colormap_files = [
                get_path(Path(fn))
                for fn in json.load(find_files(self.colormaps_folder, ".csv"))
            ]
            print("Reading custom colormaps from:", self.colormaps_folder)
            load_custom_colormaps(colormap_files)

    def add_webvizstore(self) -> List[Tuple[Callable, list]]:
        store_functions: List[Tuple[Callable, list]] = [
            (
                read_csvs,
                [{"folder": self.prod_folder, "csv_files": self.prod_names}],
            )
        ]
        for fn in [
            self.surface_metadata_file,
            self.attribute_maps_file,
        ]:
            if fn is not None:
                store_functions.append(
                    (
                        read_csv,
                        [
                            {"csv_file": fn},
                        ],
                    )
                )
        if self.colormaps_folder is not None:
            store_functions.append(
                (find_files, [{"folder": self.colormaps_folder, "suffix": ".csv"}])
            )
            store_functions.append(
                (get_path, [{"path": fn} for fn in self.colormap_files])
            )

        if self.polygons_folder is not None:
            store_functions.append(
                (find_files, [{"folder": self.polygons_folder, "suffix": ".csv"}])
            )
            store_functions.append(
                (get_path, [{"path": fn} for fn in self.polygon_files])
            )

            store_functions.append(
                (find_files, [{"folder": self.zone_faults_folder, "suffix": ".csv"}])
            )
            store_functions.append(
                (get_path, [{"path": fn} for fn in self.zone_faults_files])
            )

        if self.selector_file is not None:
            store_functions.append((get_path, [{"path": self.selector_file}]))

        if self.wellfolder is not None:
            store_functions.append(
                (read_csv, [{"csv_file": Path(self.wellfolder) / "wellbore_info.csv"}])
            )
            for fn in list(self.wellbore_info["file_name"]):
                store_functions.append((get_path, [{"path": Path(fn)}]))

            store_functions.append(
                (
                    get_path,
                    [
                        {"path": Path(self.wellfolder) / ".welldata_update.yaml"},
                        {"path": Path(self.wellfolder) / ".production_update.yaml"},
                    ],
                )
            )
            store_functions.append(
                (
                    get_path,
                    [
                        {"path": Path(self.well_layer_dir) / "well_layers.yaml"},
                    ],
                )
            )

            for fn in self.layer_files:
                store_functions.append((get_path, [{"path": Path(fn)}]))

        for fn in list(self.surface_metadata["filename"]):
            store_functions.append((get_path, [{"path": Path(fn)}]))

        if self.settings_path is not None:
            store_functions.append((get_path, [{"path": self.settings_path}]))

        return store_functions

    def ensembles(self, map_number):
        map_type = self.map_defaults[map_number]["map_type"]
        return self.selection_list[map_type]["iteration"]

    def realizations(self, map_number):
        map_type = self.map_defaults[map_number]["map_type"]
        realization_list = self.selection_list[map_type]["realization"]

        if map_type == "simulated":
            if "aggregated" in self.selection_list.keys():  # SUMO
                aggregations = self.selection_list["aggregated"]["aggregation"]
                realization_list = realization_list + aggregations

        return realization_list

    @property
    def layout(self):
        return set_layout(parent=self)

    def get_heading(self, map_ind, observation_type):
        if self.map_defaults[map_ind]["map_type"] == observation_type:
            txt = "Observed map: "
            info = "-"
        else:
            txt = "Simulated map: "
            info = (
                self.selected_ensembles[map_ind]
                + " "
                + self.selected_realizations[map_ind]
            )

        heading = (
            txt
            + self.selected_attributes[map_ind]
            + " ("
            + self.selected_names[map_ind]
            + ")"
        )

        sim_info = info
        label = get_plot_label(self.date_labels, self.selected_intervals[map_ind])

        return heading, sim_info, label

    def get_real_runpath(self, data, ensemble, real, map_type):
        selected_interval = data["date"]
        name = data["name"]
        attribute = data["attr"]

        if self.interval_mode == "normal":
            time2 = selected_interval[0:10]
            time1 = selected_interval[11:]
        else:
            time1 = selected_interval[0:10]
            time2 = selected_interval[11:]

        self.surface_metadata.replace(np.nan, "", inplace=True)

        try:
            selected_metadata = self.surface_metadata[
                (self.surface_metadata["fmu_id.realization"] == real)
                & (self.surface_metadata["fmu_id.iteration"] == ensemble)
                & (self.surface_metadata["map_type"] == map_type)
                & (self.surface_metadata["data.time.t1"] == time1)
                & (self.surface_metadata["data.time.t2"] == time2)
                & (self.surface_metadata["data.name"] == name)
                & (self.surface_metadata["data.attribute"] == attribute)
            ]

            filepath = selected_metadata["filename"].values[0]
            path = get_path(Path(filepath))

        except:
            path = ""
            print("WARNING: selected map not found. Selection criteria are:")
            print(map_type, real, ensemble, name, attribute, time1, time2)

        return path

    def create_additional_well_layers(self, interval):
        interval_overview = self.well_layers_overview.get("additional").get(interval)
        interval_well_layers = []

        if get_dates(interval)[0] <= self.production_update:
            for key, value in interval_overview.items():
                layer_dir = Path(self.well_layer_dir / "additional" / interval)
                well_layer_file = get_path(Path(layer_dir / value))
                label = self.additional_well_layers.get(key)

                well_layer = make_new_well_layer(
                    well_layer_file,
                    self.pdm_wells_df,
                    label,
                )

                if well_layer is not None:
                    interval_well_layers.append(well_layer)
                    self.layer_files.append(well_layer_file)

        return interval_well_layers

    def get_map_scaling(self, data, map_type, realization):
        min_max = None
        colormap_settings = self.colormap_settings

        if self.colormap_settings is not None:
            interval = data["date"]
            interval = (
                interval[0:4]
                + interval[5:7]
                + interval[8:10]
                + "_"
                + interval[11:15]
                + interval[16:18]
                + interval[19:21]
            )

            zone = data.get("name")

            selected_data = colormap_settings[
                (colormap_settings["map type"] == map_type)
                & (colormap_settings["attribute"] == data["attr"])
                & (colormap_settings["interval"] == interval)
                & (colormap_settings["name"] == zone)
            ]

            if "std" in realization:
                settings = selected_data[selected_data["realization"] == "std"]
            else:
                settings = selected_data[
                    selected_data["realization"] == "realization-0"
                ]

            min_max = settings[["lower_limit", "upper_limit"]]

        return min_max

    def make_map(self, data, ensemble, real, attribute_settings, map_idx):
        data = json.loads(data)
        selected_zone = data.get("name")
        selected_interval = data["date"]
        name = data["name"]
        attribute = data["attr"]

        attribute_settings = json.loads(attribute_settings)
        map_type = self.map_defaults[map_idx]["map_type"]

        surface = None

        if self.sumo_name and self.auto4d_directory is None:
            interval_list = get_sumo_interval_list(selected_interval)
            surface = get_selected_surface(
                case=self.my_case,
                map_type=map_type,
                surface_name=name,
                attribute=attribute,
                time_interval=interval_list,
                iteration_name=ensemble,
                realization=real,
            )

            if surface is None:
                heading = "Selected map doesn't exist"
                sim_info = "-"
                surface_layers = []
                label = "-"
        if self.auto4d_directory:
            surface_file = self.get_real_runpath(data, ensemble, real, map_type)

            if os.path.isfile(surface_file):
                surface = load_surface(surface_file)

        else:
            surface_file = self.get_real_runpath(data, ensemble, real, map_type)

            if os.path.isfile(surface_file):
                surface = load_surface(surface_file)

        if self.auto4d_directory:
            surface_file = self.get_real_runpath(data, ensemble, real, map_type)

            if os.path.isfile(surface_file):
                surface = load_surface(surface_file)

        if surface:
            metadata = self.get_map_scaling(data, map_type, real)

            surface_layers = [
                make_surface_layer(
                    surface,
                    name=data["attr"],
                    color=attribute_settings.get(data["attr"], {}).get(
                        "color", self.default_colormap
                    ),
                    min_val=attribute_settings.get(data["attr"], {}).get("min", None),
                    max_val=attribute_settings.get(data["attr"], {}).get("max", None),
                    unit=attribute_settings.get(data["attr"], {}).get("unit", ""),
                    hillshading=False,
                    min_max_df=metadata,
                )
            ]

            # Check if there are polygon layers available for the selected zone, iteration and and realization
            if self.sumo_name:
                self.polygons = get_sumo_zone_polygons(
                    case=self.my_case,
                    sumo_polygons=self.sumo_polygons,
                    polygon_settings=self.top_res_surface_settings,
                    map_type=map_type,
                    surface_name=name,
                    iteration_name=ensemble,
                    realization=real,
                )

                self.polygon_layers = load_sumo_polygons(
                    self.polygons, self.polygon_colors
                )

                if self.polygon_layers is not None:
                    for polygon_layer in self.polygon_layers:
                        layer_name = polygon_layer["name"]
                        layer = polygon_layer

                        surface_layers.append(layer)
                else:
                    print("WARNING: No SUMO zone polygons found")
            else:
                for polygon_layer in self.polygon_layers:
                    layer_name = polygon_layer["name"]
                    layer = polygon_layer

                    if layer_name == "Faults":
                        zone_layer = get_zone_layer(
                            self.zone_polygon_layers, selected_zone
                        )

                        if zone_layer:
                            layer = zone_layer

                    surface_layers.append(layer)

            print("Basic well layers")
            if self.basic_well_layers:
                for well_layer in self.well_basic_layers:
                    print(" ", well_layer["name"])
                    surface_layers.append(well_layer)

            interval = data["date"]

            # Load new interval layers if selected interval has changed
            if (
                interval != self.selected_intervals[map_idx]
                or interval != self.default_interval
            ):
                if get_dates(interval)[0] <= self.production_update:
                    if "PDM" not in str(self.production_data):
                        index = self.interval_names.index(interval)
                        self.interval_well_layers = self.all_interval_layers[index]
                        self.selected_intervals[map_idx] = interval
                    else:
                        self.interval_well_layers = create_production_layers(
                            field_name=self.field_name,
                            pdm_provider=self.pdm_provider,
                            interval_4d=interval,
                            wellbore_trajectories=self.drilled_wells_df,
                            surface_picks=self.surface_picks,
                            layer_options=self.additional_well_layers,
                            well_colors=self.well_colors,
                            prod_interval="Day",
                        )
                else:
                    self.interval_well_layers = []

            if self.interval_well_layers:
                for interval_layer in self.interval_well_layers:
                    surface_layers.append(interval_layer)
                    print(" ", interval_layer["name"])

            self.selected_names[map_idx] = data["name"]
            self.selected_attributes[map_idx] = data["attr"]
            self.selected_ensembles[map_idx] = ensemble
            self.selected_realizations[map_idx] = real
            self.selected_intervals[map_idx] = interval

            heading, sim_info, label = self.get_heading(map_idx, self.observations)
        else:
            heading = "Selected map doesn't exist"
            sim_info = "-"
            surface_layers = []
            label = "-"
            self.interval_status = False

        # print("make map", time.time() - t0)

        return (
            heading,
            sim_info,
            surface_layers,
            label,
        )

    def set_callbacks(self, app):
        set_first_map(parent=self, app=app)
        set_second_map(parent=self, app=app)
        set_third_map(parent=self, app=app)
        change_maps_from_button(parent=self, app=app)
