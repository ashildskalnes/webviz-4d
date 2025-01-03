import io
import sys
from pathlib import Path
import datetime
import json
import os
import numpy as np
import pandas as pd
import xtgeo
import logging
import time
from pprint import pprint
import prettytable as pt

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
    get_plot_label,
    get_dates,
    print_metadata,
)

from webviz_4d._datainput.well import (
    load_smda_metadata,
    load_smda_wellbores,
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
from webviz_4d._datainput._polygons import load_sumo_polygons

from webviz_4d._datainput._metadata import get_all_map_defaults

from webviz_4d._datainput._sumo import (
    create_sumo_lists,
    get_sumo_interval_list,
    get_selected_surface,
    get_sumo_zone_polygons,
    load_sumo_observed_metadata,
    get_sumo_tagname,
)

from webviz_4d._datainput._osdu import (
    get_osdu_metadata_attributes,
    convert_metadata,
)

from webviz_4d._datainput._rddms import (
    create_rddms_lists,
)

from webviz_4d._datainput._fmu import get_fmu_metadata, get_fmu_filename

import webviz_4d._providers.wellbore_provider.wellbore_provider as wb
from webviz_4d._providers.wellbore_provider._provider_impl_file import (
    ProviderImplFile,
)
from webviz_4d._providers.osdu_provider._provider_impl_file import DefaultOsduService
from webviz_4d._providers.rddms_provider._provider_impl_file import DefaultRddmsService
from webviz_4d._datainput._auto4d import create_auto4d_lists, load_auto4d_metadata_new

from webviz_4d._datainput.common import find_files, get_path
from webviz_4d._datainput._auto4d import (
    get_auto4d_metadata,
    get_auto4d_filename,
)
from webviz_4d._datainput._osdu import (
    get_osdu_metadata,
    get_osdu_dataset_id,
)

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
        colormaps_folder: Path = None,
        map1_defaults: dict = None,
        map2_defaults: dict = None,
        map3_defaults: dict = None,
        map_suffix: str = ".gri",
        default_interval: str = None,
        settings_file: Path = None,
        interval_mode: str = "normal",
    ):
        super().__init__()
        logging.getLogger("").setLevel(level=logging.WARNING)
        self.config = app.webviz_settings
        self.shared_settings = self.config["shared_settings"]
        # self.shared_settings = app.webviz_settings["shared_settings"]
        md_version = self.shared_settings.get("metadata_version")
        self.field_name = self.shared_settings.get("field_name")
        self.label = self.shared_settings.get("label")
        print("Field name:", self.field_name)
        print("Label:", self.label)

        self.basic_well_layers = self.shared_settings.get("basic_well_layers", None)
        self.additional_well_layers = self.shared_settings.get("additional_well_layers")
        self.top_res_surface_settings = self.shared_settings.get("top_reservoir")

        self.production_data = production_data
        self.wellfolder = well_folder
        self.colormaps_folder = colormaps_folder
        self.map_suffix = map_suffix
        self.interval_mode = interval_mode
        self.default_interval = default_interval

        settings_folder = os.path.dirname(os.path.abspath(settings_file))
        self.define_defaults()
        self.load_settings_info(settings_file)

        field_name = self.shared_settings.get("field_name")

        self.sumo_status = False
        self.fmu_status = False
        self.osdu_status = False
        self.auto4d_status = False
        self.rddms_status = False

        # Include custom colormaps if wanted
        self.get_additional_colormaps()

        sumo = self.shared_settings.get("sumo")
        sumo_env = sumo.get("env_name")
        self.sumo_case_name = sumo.get("case_name")

        self.sumo = Explorer(env=sumo_env, keep_alive="20m")
        cases = self.sumo.cases.filter(name=self.sumo_case_name)

        if len(cases) == 1:
            self.my_case = cases[0]
            self.iterations = self.my_case.iterations
            print()
            print("SUMO case:", self.my_case.name, self.field_name)
        else:
            print("ERROR: Number of selected cases =", len(cases))
            print("       Execution stopped")
            exit(1)

        self.data_sources = []
        self.metadata_lists = []
        self.selection_lists = []

        map_defaults = [map1_defaults, map2_defaults, map3_defaults]

        # Check surface maps data sources
        for map_default in map_defaults:
            data_source = map_default.get("data_source")
            self.data_sources.append(data_source)

            print("Data source:", data_source)

            if data_source == "sumo":
                metadata = load_sumo_observed_metadata(self.my_case)
                selection_list = create_sumo_lists(metadata, interval_mode)

            elif data_source == "auto4d_file":
                metadata, selection_list = get_auto4d_metadata(self.config)
            elif data_source == "fmu":
                metadata, selection_list = get_fmu_metadata(self.config, field_name)

            elif data_source == "osdu":
                osdu_service = DefaultOsduService()
                metadata, selection_list = get_osdu_metadata(
                    self.config, osdu_service, field_name
                )
            else:
                print("ERROR: Data source not supported:", data_source)

            self.metadata_lists.append(metadata)
            self.selection_lists.append(selection_list)
            # print("Selection list")
            # pprint(selection_list)
            # print()

            sumo = self.shared_settings.get("sumo")

            if self.field_name.upper() != self.my_case.field.upper():
                print(
                    "WARNING: Field name mismatch", self.field_name, self.my_case.field
                )

            self.sumo_surfaces = sumo.get("sumo_surfaces")

            if self.sumo_surfaces:
                self.sumo_status = True

                print("Surfaces from SUMO: ")
                print("  Loading metadata ...")

                self.surface_metadata = load_sumo_observed_metadata(self.my_case)
                self.selection_list = create_sumo_lists(
                    self.surface_metadata, self.interval_mode
                )

        fmu = self.shared_settings.get("fmu")
        if fmu and fmu.get("directory"):
            self.fmu_status = True
            self.surface_metadata, self.selection_list = get_fmu_metadata(
                self.config, field_name
            )

        auto4d = self.shared_settings.get("auto4d_file")
        if auto4d and auto4d.get("directory"):
            self.auto4d_status = True
            self.surface_metadata, self.selection_list = get_auto4d_metadata(
                self.config
            )

            if self.selection_list is None:
                sys.exit(
                    "ERROR: No timelapse surfaces found in", auto4d.get("directory")
                )

        osdu = self.shared_settings.get("osdu")
        if osdu and osdu.get("instance"):
            self.osdu_status = True
            self.osdu_service = DefaultOsduService()
            self.surface_metadata, self.selection_list = get_osdu_metadata(
                self.config, self.osdu_service, field_name
            )

            if self.selection_list is None:
                sys.exit("ERROR: No timelapse surfaces found in OSDU")

        rddms = self.shared_settings.get("rddms")
        if rddms and rddms.get("metadata_version"):
            self.rddms_status = True
            self.metadata_version = rddms.get("metadata_version")
            self.coverage = rddms.get("coverage")
            self.dataspace = rddms.get("dataspace")
            self.rddms_service = DefaultRddmsService()  # type: ignore
            self.osdu_service = DefaultOsduService()  # type: ignore

            self.label = "RDDMS - " + self.dataspace + ": " + self.field_name

            print("Surfaces from RDDMS: ")
            print(
                "  Searching for seismic 4D attribute maps in RDDMS",
                self.dataspace,
                self.metadata_version,
                self.field_name,
                " ...",
            )

            cache_file = "rddms_metadata_cache.csv"
            metadata_file_cache = os.path.join(settings_folder, cache_file)

            if os.path.isfile(metadata_file_cache):
                print("  Reading cached metadata from", metadata_file_cache)
                metadata = pd.read_csv(metadata_file_cache)
            else:
                print("  Reading metadata from RDDMS ...")
                attribute_horizons = self.rddms_service.get_attribute_horizons(
                    self.dataspace, self.field_name
                )
                metadata = get_osdu_metadata_attributes(attribute_horizons)
                selected_attribute_maps = metadata.loc[
                    (
                        (metadata["MetadataVersion"] == self.metadata_version)
                        & (metadata["FieldName"] == self.field_name)
                        & (metadata["SeismicCoverage"] == self.coverage)
                    )
                ]

                updated_metadata = self.osdu_service.update_reference_dates(
                    selected_attribute_maps
                )
                # updated_metadata.to_csv(metadata_file_cache)

            self.surface_metadata = convert_metadata(updated_metadata)

            self.selection_list = create_rddms_lists(
                self.surface_metadata, interval_mode
            )

            if self.selection_list is None:
                sys.exit("ERROR: No timelapse surfaces found in RDDMS")

        self.top_res_surface = get_top_res_surface(
            self.top_res_surface_settings, self.my_case
        )

        self.map_default_list = [map1_defaults, map2_defaults, map3_defaults]
        self.map_defaults = get_all_map_defaults(
            self.selection_lists, self.map_default_list
        )

        self.selected_intervals = [
            map1_defaults.get("interval"),
            map2_defaults.get("interval"),
            map3_defaults.get("interval"),
        ]

        # Load polygons
        if self.my_case.name:
            print()
            print("Polygons from SUMO ...")
            iter_name = self.top_res_surface_settings.get("iter")
            top_res_name = self.top_res_surface_settings.get("name")
            real = self.top_res_surface_settings.get("real")

            items = real.split("-")
            real_id = items[1]

            self.sumo_polygons = self.my_case.polygons.filter(
                iteration=iter_name, realization=real_id, name=top_res_name
            )

        if "SMDA" in str(self.wellfolder):
            omnia_env = ".omniaapi"
            home = os.path.expanduser("~")
            env_path = os.path.expanduser(os.path.join(home, omnia_env))
            self.smda_provider = ProviderImplFile(env_path, "SMDA")
            # self.pozo_provider = ProviderImplFile(env_path, "POZO")
            self.pdm_provider = ProviderImplFile(env_path, "PDM")

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
                self.planned_wells_info = self.smda_provider.planned_wellbore_metadata(
                    field=self.field_name,
                )
                self.planned_wells_df = self.smda_provider.planned_trajectories(
                    self.planned_wells_info.dataframe,
                )
            else:
                self.planned_wells_info = wb.PlannedWellboreMetadata(pd.DataFrame())
                self.planned_wells_df = wb.Trajectories(
                    coordinate_system="", dataframe=pd.DataFrame()
                )

            self.well_basic_layers = create_basic_well_layers(
                self.basic_well_layers,
                self.planned_wells_info.dataframe,
                self.planned_wells_df.dataframe,
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

        self.selector = SurfaceSelector(
            app, self.selection_lists[0], self.map_defaults[0], self.default_interval
        )
        self.selector2 = SurfaceSelector(
            app, self.selection_lists[1], self.map_defaults[1], self.default_interval
        )
        self.selector3 = SurfaceSelector(
            app, self.selection_lists[2], self.map_defaults[2], self.default_interval
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

    def load_settings_info(self, settings_path):
        if settings_path:
            settings = read_config(settings_path)

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

    def ensembles(self, map_number):
        map_type = self.map_defaults[map_number]["map_type"]
        selection_list = self.selection_lists[map_number]
        return selection_list[map_type]["seismic"]

    def realizations(self, map_number):
        map_type = self.map_defaults[map_number]["map_type"]
        selection_list = self.selection_lists[map_number]
        realization_list = selection_list[map_type]["difference"]

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
            info = self.data_sources[map_ind]
        else:
            txt = "Simulated map: "
            info = (
                self.selected_ensembles[map_ind]
                + " "
                + self.selected_realizations[map_ind]
            )

        zone_name = self.selected_names[map_ind]
        if zone_name == "3D+TAasgard+JS+Z22+Merge_EQ20231_PH2DG3":
            zone_name = "FullReservoirEnvelope"

        heading = txt + self.selected_attributes[map_ind] + " (" + zone_name + ")"

        sim_info = info
        label = get_plot_label(self.date_labels, self.selected_intervals[map_ind])

        return heading, sim_info, label

    def get_auto4d_filename(self, data, ensemble, real, map_type):
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
                (self.surface_metadata["difference"] == real)
                & (self.surface_metadata["seismic"] == ensemble)
                & (self.surface_metadata["map_type"] == map_type)
                & (self.surface_metadata["time1"] == time1)
                & (self.surface_metadata["time2"] == time2)
                & (self.surface_metadata["strat_zone"] == name)
                & (self.surface_metadata["attribute"] == attribute)
            ]

            filepath = selected_metadata["filename"].values[0]
            path = get_path(Path(filepath))

        except:
            path = ""
            print("WARNING: Selected file not found in Auto4d directory")
            print("  Selection criteria are:")
            print("  -  ", map_type, name, attribute, time1, time2, ensemble, real)

        return path

    def get_fmu_filename(self, data, ensemble, real, map_type):
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
                (self.surface_metadata["difference"] == real)
                & (self.surface_metadata["seismic"] == ensemble)
                & (self.surface_metadata["map_type"] == map_type)
                & (self.surface_metadata["time1"] == time1)
                & (self.surface_metadata["time2"] == time2)
                & (self.surface_metadata["name"] == name)
                & (self.surface_metadata["attribute"] == attribute)
            ]

            path = selected_metadata["filename"].values[0]

        except:
            path = ""
            print("WARNING: selected map not found. Selection criteria are:")
            print(map_type, real, ensemble, name, attribute, time1, time2)
            # print(selected_metadata)

        return path

    def get_rddms_dataset_id(self, data, ensemble, real, map_type):
        selected_interval = data["date"]
        name = data["name"]
        attribute = data["attr"]

        if selected_interval[0:10] > selected_interval[11:]:
            time2 = selected_interval[0:10]
            time1 = selected_interval[11:]
        else:
            time1 = selected_interval[0:10]
            time2 = selected_interval[11:]

        self.surface_metadata.replace(np.nan, "", inplace=True)

        try:
            selected_metadata = self.surface_metadata[
                (self.surface_metadata["difference"] == real)
                & (self.surface_metadata["seismic"] == ensemble)
                & (self.surface_metadata["map_type"] == map_type)
                & (self.surface_metadata["time1"] == time1)
                & (self.surface_metadata["time2"] == time2)
                & (self.surface_metadata["name"] == name)
                & (self.surface_metadata["attribute"] == attribute)
            ]

            print()
            print("Selected dataset info:")
            print(map_type, real, ensemble, name, attribute, time1, time2)

            if len(selected_metadata) > 1:
                print("WARNING number of datasets", len(selected_metadata))
                print(selected_metadata)

            dataset_id = selected_metadata["dataset_id"].values[0]
            return dataset_id
        except:
            dataset_id = None
            print("WARNING: Selected map not found in RDDMS. Selection criteria are:")
            print(map_type, real, ensemble, name, attribute, time1, time2)

        return dataset_id

    def get_osdu_dataset_id(self, data, ensemble, real, map_type):
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
                (self.surface_metadata["difference"] == real)
                & (self.surface_metadata["seismic"] == ensemble)
                & (self.surface_metadata["map_type"] == map_type)
                & (self.surface_metadata["time1"] == time1)
                & (self.surface_metadata["time2"] == time2)
                & (self.surface_metadata["name"] == name)
                & (self.surface_metadata["attribute"] == attribute)
            ]

            # print("Selected dataset info:")
            # print(map_type, real, ensemble, name, attribute, time1, time2)

            if len(selected_metadata) > 1:
                print("WARNING number of datasets", len(selected_metadata))
                # print(selected_metadata)

            dataset_id = selected_metadata["dataset_id"].values[0]
            return dataset_id
        except:
            dataset_id = None
            print("WARNING: Selected map not found in OSDU. Selection criteria are:")
            print(map_type, real, ensemble, name, attribute, time1, time2)

        return dataset_id

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

    def get_auto_scaling(self, surface, attribute_type):
        min_max = [None, None]
        attribute_settings = self.attribute_settings
        settings = attribute_settings.get(attribute_type)
        auto_scaling = settings.get("auto_scaling", 10)

        if attribute_settings and attribute_type in attribute_settings.keys():
            colormap_type = attribute_settings.get(attribute_type).get("type")
            surface_max_val = surface.values.max()
            surface_min_val = surface.values.min()

            scaled_value = abs(surface.values.std())

            if "mean" in attribute_type.lower():
                scaled_value = (abs(surface_min_val) + abs(surface_max_val)) / 2

            max_val = scaled_value * auto_scaling

            if colormap_type == "diverging":
                min_val = -max_val
            elif colormap_type == "positive":
                min_val = 0
            elif colormap_type == "negative":
                min_val = -max_val
                max_val = 0
            else:
                min_val = -max_val
            min_max = [min_val, max_val]

        return min_max

    def print_surface_info(self, map_idx, tic, toc, surface):
        print()
        print(
            f"Map number {map_idx+1}: downloaded the surface in {toc - tic:0.2f} seconds"
        )
        number_cells = surface.nrow * surface.ncol
        print(f"Surface size: {surface.nrow} x {surface.ncol} = {number_cells}")

    def load_selected_surface_fmu(self, data, ensemble, real, coverage, map_idx):
        # Load surface from one of the data sources based on the selected metadata

        name = data["name"]
        attribute = data["attr"]
        map_type = self.map_defaults[map_idx]["map_type"]
        surface_metadata = self.metadata_lists[map_idx]

        selected_interval = data["date"]
        time1 = selected_interval[0:10]
        time2 = selected_interval[11:]

        data_source = self.data_sources[map_idx]

        print(
            "DEBUG load_selected_surface_fmu",
            map_idx,
            data_source,
            ensemble,
            real,
            coverage,
        )

        metadata_keys = [
            "map_index",
            "map_type",
            "surface_name",
            "attribute",
            "time_interval",
            "seismic",
            "difference",
        ]

        surface = None

        if data_source == "fmu":
            surface_file = get_fmu_filename(
                data, ensemble, real, map_type, surface_metadata
            )

            if os.path.isfile(surface_file):
                print("Loading surface from", data_source)
                tic = time.perf_counter()
                surface = load_surface(surface_file)
                toc = time.perf_counter()

        if surface is not None:
            self.print_surface_info(map_idx, tic, toc, surface)
        else:
            metadata_values = [
                map_idx,
                map_type,
                name,
                attribute,
                [time1, time2],
                ensemble,
                real,
            ]
            print("Selected map not found in", data_source)
            print("  Selection criteria:")

            for index, metadata in enumerate(metadata_keys):
                print("  - ", metadata, ":", metadata_values[index])

        return surface

    def make_map(self, data, ensemble, real, attribute_settings, map_idx):
        data = json.loads(data)
        name = data["name"]
        attribute = data["attr"]

        attribute_settings = json.loads(attribute_settings)
        map_type = self.map_defaults[map_idx]["map_type"]
        coverage = self.map_defaults[map_idx]["coverage"]

        if self.data_sources[map_idx] == "fmu":
            surface = self.load_selected_surface_fmu(
                data, ensemble, real, coverage, map_idx
            )
        elif self.data_sources[map_idx] == "auto4d_file":
            surface = self.load_selected_surface_auto4d(
                data, ensemble, real, coverage, map_idx
            )
        if self.data_sources[map_idx] == "sumo":
            surface = self.load_selected_surface_sumo(
                data, ensemble, real, coverage, map_idx
            )

        if surface:
            # metadata = self.get_map_scaling(data, map_type, real)
            min_max = self.get_auto_scaling(surface, attribute)

            min_val = min_max[0]
            max_val = min_max[1]

            surface_layers = [
                make_surface_layer(
                    surface,
                    name=data["attr"],
                    color=attribute_settings.get(data["attr"], {}).get(
                        "color", self.default_colormap
                    ),
                    min_val=min_val,
                    max_val=max_val,
                    unit=attribute_settings.get(data["attr"], {}).get("unit", ""),
                    hillshading=False,
                    # min_max_df=metadata,
                )
            ]

            # Check if there are polygon layers available for the selected zone, iteration and and realization
            if self.sumo:
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
                        layer = polygon_layer

                        surface_layers.append(layer)

            # print("Basic well layers")
            if self.basic_well_layers:
                for well_layer in self.well_basic_layers:
                    # print("DEBUG planned well layer")
                    # print(well_layer["name"])
                    # if well_layer["name"] == "Planned wells":
                    #     print(well_layer["name"])
                    #     print(well_layer["type"]["positions"])
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
                    # print(" ", interval_layer["name"])

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

    def load_selected_surface_auto4d(self, data, ensemble, real, coverage, map_idx):
        # Load surface from one of the data sources based on the selected metadata

        name = data["name"]
        attribute = data["attr"]
        map_type = self.map_defaults[map_idx]["map_type"]
        surface_metadata = self.metadata_lists[map_idx]

        selected_interval = data["date"]
        time1 = selected_interval[0:10]
        time2 = selected_interval[11:]

        data_source = self.data_sources[map_idx]

        outfile = "/private/ashska/" + str(map_idx) + "_" + data_source + ".csv"
        surface_metadata.to_csv(outfile)
        print("metadata written to file", outfile)

        metadata_keys = [
            "map_index",
            "map_type",
            "surface_name",
            "attribute",
            "time_interval",
            "seismic",
            "difference",
        ]

        surface = None

        if data_source == "auto4d_file":
            surface_file = get_auto4d_filename(
                surface_metadata, data, ensemble, real, map_type, coverage
            )

            if os.path.isfile(surface_file):
                print("Loading surface from", data_source)
                tic = time.perf_counter()
                surface = load_surface(surface_file)
                toc = time.perf_counter()

        if surface is not None:
            self.print_surface_info(map_idx, tic, toc, surface)
        else:
            metadata_values = [
                map_idx,
                map_type,
                name,
                attribute,
                [time1, time2],
                ensemble,
                real,
            ]
            print("Selected map not found in", data_source)
            print("  Selection criteria:")

            for index, metadata in enumerate(metadata_keys):
                print("  - ", metadata, ":", metadata_values[index])

        return surface

    def load_selected_surface_sumo(self, data, ensemble, real, coverage, map_idx):
        # Load surface from one of the data sources based on the selected metadata

        name = data["name"]
        attribute = data["attr"]
        map_type = self.map_defaults[map_idx]["map_type"]
        surface_metadata = self.metadata_lists[map_idx]

        selected_interval = data["date"]
        time1 = selected_interval[0:10]
        time2 = selected_interval[11:]

        data_source = self.data_sources[map_idx]

        outfile = str(map_idx) + "_" + data_source + ".csv"
        surface_metadata.to_csv(outfile)
        print("metadata written to file", outfile)

        metadata_keys = [
            "map_index",
            "map_type",
            "surface_name",
            "attribute",
            "time_interval",
            "seismic",
            "difference",
        ]

        surface = None

        if data_source == "sumo":
            interval_list = get_sumo_interval_list(selected_interval)

            tagname = get_sumo_tagname(
                surface_metadata, name, ensemble, attribute, real, interval_list
            )

            tic = time.perf_counter()
            surface = get_selected_surface(
                case=self.my_case,
                map_type=map_type,
                surface_name=name,
                attribute=tagname,
                time_interval=interval_list,
                iteration_name=None,
                realization=None,
            )
            toc = time.perf_counter()

        if surface is not None:
            self.print_surface_info(map_idx, tic, toc, surface)
        else:
            metadata_values = [
                map_idx,
                map_type,
                name,
                attribute,
                [time1, time2],
                ensemble,
                real,
            ]
            print("Selected map not found in", data_source)
            print("  Selection criteria:")

            for index, metadata in enumerate(metadata_keys):
                print("  - ", metadata, ":", metadata_values[index])

        return surface

    def set_callbacks(self, app):
        set_first_map(parent=self, app=app)
        set_second_map(parent=self, app=app)
        set_third_map(parent=self, app=app)
        change_maps_from_button(parent=self, app=app)
