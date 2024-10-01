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
from io import BytesIO
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
    get_plot_label,
    get_dates,
)

from webviz_4d._datainput.well import (
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
from webviz_4d._datainput._polygons import load_sumo_polygons

from webviz_4d._datainput._metadata import get_all_map_defaults

from webviz_4d._datainput._sumo import (
    create_sumo_lists,
    get_sumo_interval_list,
    get_selected_surface,
    get_sumo_zone_polygons,
    load_sumo_observed_metadata,
)

from webviz_4d._datainput._osdu import (
    get_osdu_metadata_attributes,
    convert_metadata,
    create_osdu_lists,
)

from webviz_4d._datainput._fmu import (
    load_fmu_metadata,
    create_fmu_lists,
)

from webviz_4d._providers.wellbore_provider._provider_impl_file import (
    ProviderImplFile,
)
from webviz_4d._providers.osdu_provider._provider_impl_file import DefaultOsduService
from webviz_4d._providers.rddms_provider._provider_impl_file import DefaultRddmsService
from webviz_4d._datainput._auto4d import create_auto4d_lists, load_auto4d_metadata

from webviz_4d._datainput.common import find_files, get_path

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
        attribute_maps_file: Path = None,
        interval_mode: str = "normal",
        selector_file: Path = None,
    ):
        super().__init__()
        logging.getLogger("").setLevel(level=logging.WARNING)
        self.shared_settings = app.webviz_settings["shared_settings"]
        self.field_name = self.shared_settings.get("field_name")
        self.label = self.shared_settings.get("label")
        print("Field name:", self.field_name)
        print("Label:", self.label)

        self.basic_well_layers = self.shared_settings.get("basic_well_layers", None)
        self.additional_well_layers = self.shared_settings.get("additional_well_layers")
        self.top_res_surface_settings = self.shared_settings.get("top_reservoir")

        self.selector_file = selector_file
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
        self.attribute_maps_file = attribute_maps_file

        # Check surface maps data source
        sumo = self.shared_settings.get("sumo")
        if sumo:
            self.sumo_case_name = sumo.get("case_name")
            sumo_env = sumo.get("env_name")

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
            fmu_directory = fmu.get("directory")
            observed_maps = self.shared_settings.get("observed_maps")
            map_dir = observed_maps.get("map_directories")[0]

            print("Surfaces from FMU files: ")
            print("  Loading metadata ...")

            self.surface_metadata = load_fmu_metadata(
                fmu_directory, map_dir, field_name
            )
            self.selection_list = create_fmu_lists(self.surface_metadata, interval_mode)

        auto4d = self.shared_settings.get("auto4d")
        if auto4d and auto4d.get("directory"):
            self.auto4d_status = True
            auto4d_directory = auto4d.get("directory")
            file_ext = auto4d.get("metadata_format")
            md_version = auto4d.get("metadata_version")

            acquisition_dates = auto4d.get("acquisition_dates")
            selections = auto4d.get("selections")

            print("Surfaces from Auto4d files: ")
            print("  Loading metadata ...")

            self.surface_metadata = load_auto4d_metadata(
                auto4d_directory, file_ext, md_version, selections, acquisition_dates
            )

            self.selection_list = create_auto4d_lists(
                self.surface_metadata, interval_mode
            )

            if self.selection_list is None:
                sys.exit("ERROR: No timelapse surfaces found in", auto4d_directory)

        osdu = self.shared_settings.get("osdu")
        if osdu and osdu.get("metadata_version"):
            self.osdu_status = True
            self.metadata_version = osdu.get("metadata_version")
            self.coverage = osdu.get("coverage")
            self.osdu_service = DefaultOsduService()  # type: ignore
            self.label = "OSDU: " + self.field_name + " coverage:" + self.coverage

            print("Surfaces from OSDU Core: ")
            print("  Loading metadata ...")
            print(self.label, self.metadata_version, self.coverage)

            osdu_key = "tags.AttributeMap.FieldName"
            osdu_value = self.field_name

            cache_file = "metadata_cache_" + self.coverage + ".csv"
            metadata_file_cache = os.path.join(settings_folder, cache_file)

            if os.path.isfile(metadata_file_cache):
                print("  Reading cached metadata from", metadata_file_cache)
                metadata = pd.read_csv(metadata_file_cache)
                updated_metadata = metadata.loc[
                    metadata["AttributeMap.Coverage"] == self.coverage
                ]
            else:
                print("  Reading metadata from OSDU Core ...")
                attribute_horizons = self.osdu_service.get_attribute_horizons(
                    osdu_key, osdu_value
                )
                metadata = get_osdu_metadata_attributes(attribute_horizons)
                selected_attribute_maps = metadata.loc[
                    (
                        (metadata["MetadataVersion"] == self.metadata_version)
                        & (metadata["Name"] == metadata["AttributeMap.Name"])
                        & (metadata["AttributeMap.FieldName"] == self.field_name)
                        & (metadata["AttributeMap.Coverage"] == self.coverage)
                    )
                ]

                updated_metadata = self.osdu_service.update_reference_dates(
                    selected_attribute_maps
                )
                updated_metadata.to_csv(metadata_file_cache)

            validA = updated_metadata.loc[updated_metadata["AcquisitionDateA"] != ""]
            attribute_metadata = validA.loc[validA["AcquisitionDateB"] != ""]

            self.surface_metadata = convert_metadata(attribute_metadata)

            self.selection_list = create_osdu_lists(
                self.surface_metadata, interval_mode
            )

            if self.selection_list is None:
                sys.exit("ERROR: No timelapse surfaces found in OSDU")

        rddms = self.shared_settings.get("rddms")
        if rddms and rddms.get("metadata_version"):
            self.rddms_status = True
            self.metadata_version = rddms.get("metadata_version")
            self.coverage = rddms.get("coverage")
            self.rddms_service = DefaultRddmsService()  # type: ignore
            self.osdu_service = DefaultOsduService()  # type: ignore
            self.dataspace = rddms.get("dataspace")
            self.label = "RDDMS: " + self.field_name + " coverage:" + self.coverage

            print("Surfaces from RDDMS: ")
            print("  Loading metadata ...")
            print(self.label, self.metadata_version, self.coverage)

            cache_file = "rddms_metadata_cache_" + self.coverage + ".csv"
            metadata_file_cache = os.path.join(settings_folder, cache_file)

            if os.path.isfile(metadata_file_cache):
                print("  Reading cached metadata from", metadata_file_cache)
                metadata = pd.read_csv(metadata_file_cache)
                updated_metadata = metadata.loc[
                    metadata["SeismicCoverage"] == self.coverage
                ]
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
                updated_metadata.to_csv(metadata_file_cache)

            validA = updated_metadata.loc[updated_metadata["AcquisitionDateA"] != ""]
            attribute_metadata = validA.loc[validA["AcquisitionDateB"] != ""]

            self.surface_metadata = convert_metadata(attribute_metadata)

            self.selection_list = create_osdu_lists(
                self.surface_metadata, interval_mode
            )

            print("DEBUG selection_list")
            pprint(self.selection_list)

            if self.selection_list is None:
                sys.exit("ERROR: No timelapse surfaces found in RDDMS")

        self.top_res_surface = get_top_res_surface(
            self.top_res_surface_settings, self.my_case
        )

        map_default_list = [map1_defaults, map2_defaults, map3_defaults]
        self.map_defaults = get_all_map_defaults(self.selection_list, map_default_list)

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
            self.pozo_provider = ProviderImplFile(env_path, "POZO")
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
                planned_wells = load_planned_wells(
                    self.pozo_provider, self.field_name.upper()
                )
                self.planned_wells_info = planned_wells[0]
                self.planned_wells_df = planned_wells[1]
            else:
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
        return self.selection_list[map_type]["seismic"]

    def realizations(self, map_number):
        map_type = self.map_defaults[map_number]["map_type"]
        realization_list = self.selection_list[map_type]["difference"]

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
                & (self.surface_metadata["time.t1"] == time1)
                & (self.surface_metadata["time.t2"] == time2)
                & (self.surface_metadata["name"] == name)
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
                & (self.surface_metadata["time.t1"] == time1)
                & (self.surface_metadata["time.t2"] == time2)
                & (self.surface_metadata["name"] == name)
                & (self.surface_metadata["attribute"] == attribute)
            ]

            path = selected_metadata["filename"].values[0]

        except:
            path = ""
            print("WARNING: selected map not found. Selection criteria are:")
            print(map_type, real, ensemble, name, attribute, time1, time2)
            print(selected_metadata)

        return path

    def get_osdu_dataset_id(self, data, ensemble, real, map_type):
        selected_interval = data["date"]
        name = data["name"]
        attribute = data["attr"]

        print("DEBUG get_osdu_dataset_id")
        print(name, self.surface_metadata["name"])

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
                & (self.surface_metadata["time.t1"] == time1)
                & (self.surface_metadata["time.t2"] == time2)
                & (self.surface_metadata["name"] == name)
                & (self.surface_metadata["attribute"] == attribute)
            ]

            print("Selected dataset info:")
            print(map_type, real, ensemble, name, attribute, time1, time2)

            if len(selected_metadata) > 1:
                print("WARNING number of datasets", len(selected_metadata))
                print(selected_metadata)

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
        auto_scaling = settings.get("auto_scaling", 15)

        if attribute_settings and attribute_type in attribute_settings.keys():
            colormap_type = attribute_settings.get(attribute_type).get("type")
            surface_max_val = surface.values.max()
            surface_min_val = surface.values.min()

            scaled_value = abs(surface.values.mean())

            if "mean" in attribute_type.lower():
                scaled_value = (abs(surface_min_val) + abs(surface_max_val)) / 2

            max_val = scaled_value * auto_scaling

            # print(
            #     "DEBUG auto_scaling",
            #     surface_min_val,
            #     surface_max_val,
            #     max_val,
            # )

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

    def load_selected_surface(self, data, ensemble, real, map_idx):
        # Load surface from one of the data sources based on the selected metadata

        name = data["name"]
        attribute = data["attr"]
        map_type = self.map_defaults[map_idx]["map_type"]

        selected_interval = data["date"]
        time1 = selected_interval[0:10]
        time2 = selected_interval[11:]

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

        if self.sumo_status:
            data_source = "SUMO"
            interval_list = get_sumo_interval_list(selected_interval)
            time1 = interval_list[0]
            time2 = interval_list[1]

            selected_row = self.surface_metadata[
                (self.surface_metadata["name"] == name)
                & (self.surface_metadata["seismic"] == ensemble)
                & (self.surface_metadata["attribute"] == attribute)
                & (self.surface_metadata["difference"] == real)
                & (self.surface_metadata["time.t1"] == time1)
                & (self.surface_metadata["time.t2"] == time2)
                & (self.surface_metadata["map_type"] == map_type)
            ]

            if selected_row.empty:
                surface = None
            else:
                tagname = selected_row["tagname"].values[0]
                print("Loading surface from", data_source)
                tic = time.perf_counter()
                surface = get_selected_surface(
                    case=self.my_case,
                    map_type=map_type,
                    surface_name=name,
                    attribute=tagname,
                    time_interval=interval_list,
                    iteration_name=ensemble,
                    realization=real,
                )
                toc = time.perf_counter()

        if self.rddms_status:
            data_source = "OSDU"
            dataset_id = self.get_osdu_dataset_id(data, ensemble, real, map_type)

            if dataset_id is not None:
                print("Loading surface from", data_source)
                surface = None
                # tic = time.perf_counter()
                # dataset = self.rddms_service.get_horizon_map(file_id=dataset_id)
                # blob = io.BytesIO(dataset.content)
                # surface = xtgeo.surface_from_file(blob)
                # toc = time.perf_counter()

        if self.osdu_status:
            data_source = "OSDU"
            dataset_id = self.get_osdu_dataset_id(data, ensemble, real, map_type)

            if dataset_id is not None:
                print("Loading surface from", data_source)
                tic = time.perf_counter()
                dataset = self.osdu_service.get_horizon_map(file_id=dataset_id)
                blob = io.BytesIO(dataset.content)
                surface = xtgeo.surface_from_file(blob)
                toc = time.perf_counter()

        if self.auto4d_status:
            data_source = "AUTO4D"
            surface_file = self.get_auto4d_filename(data, ensemble, real, map_type)

            if os.path.isfile(surface_file):
                print("Loading surface from", data_source)
                tic = time.perf_counter()
                surface = load_surface(surface_file)
                toc = time.perf_counter()

        if self.fmu_status:
            data_source = "FMU"
            surface_file = self.get_fmu_filename(data, ensemble, real, map_type)
            # print("DEBUG surface_file", surface_file)

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

        surface = self.load_selected_surface(data, ensemble, real, map_idx)

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
                        layer_name = polygon_layer["name"]
                        layer = polygon_layer

                        surface_layers.append(layer)

            # print("Basic well layers")
            if self.basic_well_layers:
                for well_layer in self.well_basic_layers:
                    # print(" ", well_layer["name"])
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

    def set_callbacks(self, app):
        set_first_map(parent=self, app=app)
        set_second_map(parent=self, app=app)
        set_third_map(parent=self, app=app)
        change_maps_from_button(parent=self, app=app)
