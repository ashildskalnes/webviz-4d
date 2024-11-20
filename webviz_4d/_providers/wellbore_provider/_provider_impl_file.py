import logging
from re import T
from typing import Optional
from pandas import DataFrame, concat

from webviz_4d._providers.wellbore_provider._smda import (
    smda_connect,
    extract_metadata,
    extract_trajectories,
    extract_picks,
    extract_planned_metadata,
    extract_planned_trajectories,
)
from webviz_4d._providers.wellbore_provider._pdm import (
    pdm_connect,
    extract_pdm_filter,
    extract_field_production,
    extract_field_injection,
    extract_production,
    extract_injection,
    extract_pdm_wellbores,
    extract_cumulative_volumes,
)
from webviz_4d._providers.wellbore_provider._ssdl import (
    ssdl_connect,
    extract_default_model,
    extract_faultlines,
    extract_outlines,
    extract_ssdl_completion,
    extract_ssdl_perforation,
    extract_ssdl_wellbores,
)

# from webviz_4d._providers.wellbore_provider._pozo import (
#     pozo_connect,
#     extract_planned_data,
#     extract_plannedWell_position,
# )

import webviz_4d._providers.wellbore_provider.wellbore_provider as wb

LOGGER = logging.getLogger(__name__)
MAX_ITEMS = 9000

SMDA_API = "https://api.gateway.equinor.com/smda/v2.0/smda-api/"
# POZO_API = "https://wfmwellapiprod.azurewebsites.net/"
SSDL_API = "https://api.gateway.equinor.com/subsurfacedata/v3/api/v3.0/"
PDM_API = "https://api.gateway.equinor.com/pdm-internal-api/v3/api"


class ProviderImplFile(wb.WellboreProvider):
    def __init__(self, omnia_path, db_name) -> None:
        if db_name == "SMDA":
            self.smdahandle = smda_connect(omnia_path)
            self.smda_address = wb.SmdaAddress(api=SMDA_API, session=self.smdahandle)
        # elif db_name == "POZO":
        #     self.pozohandle = pozo_connect(omnia_path)
        #     self.pozo_address = wb.PozoAddress(api=POZO_API, session=self.pozohandle)
        elif db_name == "SSDL":
            self.ssdlhandle = ssdl_connect(omnia_path)
            self.ssdl_address = wb.SsdlAddress(api=SSDL_API, session=self.ssdlhandle)
        elif db_name == "PDM":
            self.pdmhandle = pdm_connect(omnia_path)
            self.pdm_address = wb.PdmAddress(api=PDM_API, session=self.pdmhandle)
        else:
            print("ERROR:", db_name, "is not a valid database name")
            exit()

    @property
    def smda_adress(self):
        return self.smda_address

    @property
    def pdm_adress(self):
        return self.pdm_address

    @property
    def pozo_adress(self):
        return self.pozo_address

    @property
    def ssdl_adress(self):
        return self.ssdl_address

    def drilled_wellbore_metadata(
        self,
        field: str,
    ) -> wb.DrilledWellboreMetadata:
        metadata = None

        filter = "field_identifier=" + field
        metadata_df = extract_metadata(self.smda_address, filter)

        if not metadata_df.empty:
            metadata = wb.DrilledWellboreMetadata(metadata_df)

        return metadata

    def drilled_wellbore_trajectory(
        self,
        wellbore_name: str,
        md_min: Optional[float] = 0,
        md_max: Optional[float] = None,
    ) -> wb.Trajectory:
        trajectory = None

        if wellbore_name:
            filter = "unique_wellbore_identifier=" + wellbore_name
            trajectory_df, crs = extract_trajectories(self.smda_address, filter)

            if not trajectory_df.empty:
                trajectory_df.sort_values(by=["md"], inplace=True)

                if md_min > 0:
                    if md_max:
                        selected_trajectory_df = trajectory_df[
                            (trajectory_df["md"] >= md_min)
                            & (trajectory_df["md"] <= md_max)
                        ]
                    else:
                        selected_trajectory_df = trajectory_df[
                            trajectory_df["md"] >= md_min
                        ]
                else:
                    selected_trajectory_df = trajectory_df

                trajectory = wb.Trajectory(
                    coordinate_system=crs,
                    x_arr=selected_trajectory_df["easting"].to_numpy(),
                    y_arr=selected_trajectory_df["northing"].to_numpy(),
                    z_arr=selected_trajectory_df["tvd_msl"].to_numpy(),
                    md_arr=selected_trajectory_df["md"].to_numpy(),
                )

        return trajectory

    def planned_wellbore_trajectory(
        self,
        wellbore_name: str,
        md_min: Optional[float] = 0,
        md_max: Optional[float] = None,
    ) -> wb.Trajectory:
        trajectory = None
        metadata = None

        if wellbore_name:
            trajectories_df = extract_planned_trajectories(
                self.smda_address, metadata, wellbore_name
            )

            trajectory_df = trajectories_df[0]

            if not trajectory_df.empty:
                trajectory_df.sort_values(by=["md"], inplace=True)

                if md_min > 0:
                    if md_max:
                        selected_trajectory_df = trajectory_df[
                            (trajectory_df["md"] >= md_min)
                            & (trajectory_df["md"] <= md_max)
                        ]
                    else:
                        selected_trajectory_df = trajectory_df[
                            trajectory_df["md"] >= md_min
                        ]
                else:
                    selected_trajectory_df = trajectory_df

                trajectory = wb.Trajectory(
                    coordinate_system="",
                    x_arr=selected_trajectory_df["easting"].to_numpy(),
                    y_arr=selected_trajectory_df["northing"].to_numpy(),
                    z_arr=selected_trajectory_df["tvd_msl"].to_numpy(),
                    md_arr=selected_trajectory_df["md"].to_numpy(),
                )

        return trajectory

    def drilled_trajectories(
        self,
        field_name: str,
    ) -> wb.Trajectories:
        trajectories = None

        filter = "field_identifier=" + field_name
        dataframe, crs = extract_trajectories(self.smda_address, filter)
        trajectories = wb.Trajectories(crs, dataframe)

        return trajectories

    def get_wellbore_picks(
        self,
        field_name: str,
        pick_identifiers: Optional[str] = None,
        wellbore_name: Optional[str] = None,
        interpreter: Optional[str] = None,
    ) -> wb.WellborePicks:
        dataframe = DataFrame()
        well_picks = None
        filter = "field_identifier=" + field_name

        if pick_identifiers is not None:
            picks = pick_identifiers[0]

            for i in range(1, len(pick_identifiers)):
                picks = picks + "," + pick_identifiers[i]

            filter = filter + "&pick_identifier=" + picks

        if wellbore_name is not None:
            filter = filter + "&unique_wellbore_identifier=" + wellbore_name

        if interpreter is not None:
            filter = filter + "&interpreter=" + interpreter

        dataframe = extract_picks(self.smda_address, filter)
        well_picks = wb.WellborePicks(dataframe)

        return well_picks

    def get_production_volumes(
        self,
        field_name: str,
        wellbore_names: Optional[list] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        interval: Optional[str] = "Day",
    ) -> wb.DailyProductionVolumes:
        dataframe = DataFrame()
        volumes = None

        filter = extract_pdm_filter(
            field_name, wellbore_names, start_date, end_date, interval
        )
        dataframe = extract_production(self.pdm_address, filter, interval)

        volumes = wb.DailyProductionVolumes(dataframe)

        return volumes

    def get_field_prod_data(
        self,
        field_name: str,
        wellbore_names: Optional[list] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        interval: Optional[str] = "Day",
        field_uuid: Optional[str] = None,
    ) -> wb.ProductionVolumes:
        volumes = None

        filter = extract_pdm_filter(
            field_name, wellbore_names, start_date, end_date, interval, field_uuid
        )
        volumes = extract_field_production(self.pdm_address, filter, interval)

        return volumes

    def get_field_inj_data(
        self,
        field_name: str,
        wellbore_names: Optional[list] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        interval: Optional[str] = "Day",
        field_uuid: Optional[str] = None,
    ) -> wb.InjectionVolumes:
        volumes = None

        filter = extract_pdm_filter(
            field_name, wellbore_names, start_date, end_date, interval, field_uuid
        )
        volumes = extract_field_injection(self.pdm_address, filter, interval)

        return volumes

    def get_injection_volumes(
        self,
        field_name: str,
        wellbore_names: Optional[list] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        interval: Optional[str] = "Day",
    ) -> wb.DailyInjectionVolumes:
        dataframe = DataFrame()
        volumes = None

        filter = extract_pdm_filter(
            field_name, wellbore_names, start_date, end_date, interval
        )
        dataframe = extract_injection(self.pdm_address, filter)

        volumes = wb.DailyInjectionVolumes(dataframe)

        return volumes

    def get_pdm_wellbores(
        self, field_name: str, pdm_wellbores: Optional[list] = None
    ) -> wb.PdmDates:
        dataframe = DataFrame()
        dates = None

        filter = extract_pdm_filter(field_name, pdm_wellbores)
        dataframe = extract_pdm_wellbores(self.pdm_address, filter)

        dates = wb.PdmDates(dataframe)

        return dates

    def planned_trajectories(
        self,
        metadata: str,
    ) -> wb.PlannedTrajectories:
        trajectories = None
        print("Loading planned well trajectories from SMDA")

        dataframe = extract_planned_trajectories(
            self.smda_address,
            metadata,
            "",
        )

        unique_wellbores = dataframe["unique_wellbore_identifier"].unique()

        trajectories = wb.Trajectories(dataframe=dataframe, coordinate_system="")
        print("  - Planned wellbores:", len(unique_wellbores))

        return trajectories

    def get_prod_data(
        self,
        daily_volumes: wb.DailyProductionVolumes,
        first_date: str,
        second_date: str,
        pdm_well_names: Optional[list] = None,
        interval: Optional[str] = "Day",
    ) -> wb.ProductionVolumes:
        volumes = None
        dataframe = DataFrame()
        wb_uuids = []
        oil_volumes = []
        gas_volumes = []
        water_volumes = []
        oil_unit = "SM3"
        gas_unit = "SM3"
        water_unit = "M3"

        daily_df = daily_volumes.dataframe
        prod_wellbores = []

        if daily_df.empty:
            print("ERROR: The daily production volumes are empty")
        else:
            if not pdm_well_names:
                pdm_well_names = daily_df["WB_UWBI"].unique()

            for pdm_name in pdm_well_names:
                wellbore_volumes = daily_df[daily_df["WB_UWBI"] == pdm_name]

                if not wellbore_volumes.empty:
                    oil_volume = extract_cumulative_volumes(
                        wellbore_volumes,
                        "WB_OIL_VOL_SM3",
                        first_date,
                        second_date,
                        interval,
                    )
                    oil_volumes.append(oil_volume)

                    gas_volume = extract_cumulative_volumes(
                        wellbore_volumes,
                        "WB_GAS_VOL_SM3",
                        first_date,
                        second_date,
                        interval,
                    )
                    gas_volumes.append(gas_volume)

                    water_volume = extract_cumulative_volumes(
                        wellbore_volumes,
                        "WB_WATER_VOL_M3",
                        first_date,
                        second_date,
                        interval,
                    )
                    water_volumes.append(water_volume)

                    wb_uuid = wellbore_volumes["WB_UUID"].values[0]
                    wb_uuids.append(wb_uuid)
                    prod_wellbores.append(pdm_name)

            dataframe["WB_UWBI"] = prod_wellbores
            dataframe["WB_UUID"] = wb_uuids
            dataframe["OIL_VOL"] = oil_volumes
            dataframe["GAS_VOL"] = gas_volumes
            dataframe["WATER_VOL"] = water_volumes

        volumes = wb.ProductionVolumes(
            oil_unit,
            gas_unit,
            water_unit,
            first_date,
            second_date,
            dataframe,
        )

        return volumes

    def get_inj_data(
        self,
        daily_volumes: wb.DailyInjectionVolumes,
        first_date: str,
        second_date: str,
        pdm_well_names: Optional[list] = None,
        interval: Optional[str] = "Day",
    ) -> wb.InjectionVolumes:
        volumes = None
        dataframe = DataFrame()
        wb_uuids = []
        gi_volumes = []
        wi_volumes = []
        gi_unit = "M3"
        wi_unit = "M3"
        co2_unit = "M3"

        daily_df = daily_volumes.dataframe
        inj_wellbores = []

        if not daily_df.empty and pdm_well_names is None:
            pdm_well_names = daily_df["WB_UWBI"].unique()

            for pdm_name in pdm_well_names:
                injection_type = "GI"
                wellbore_volumes = daily_df[
                    (daily_df["WB_UWBI"] == pdm_name)
                    & (daily_df["INJ_TYPE"] == injection_type)
                ]

                if not wellbore_volumes.empty:
                    gi_volume = extract_cumulative_volumes(
                        wellbore_volumes,
                        "WB_INJ_VOL",
                        first_date,
                        second_date,
                        interval,
                    )
                    gi_volumes.append(gi_volume)

                    injection_type = "WI"
                    wellbore_volumes = daily_df[
                        (daily_df["WB_UWBI"] == pdm_name)
                        & (daily_df["INJ_TYPE"] == injection_type)
                    ]

                    wi_volume = extract_cumulative_volumes(
                        wellbore_volumes,
                        "WB_INJ_VOL",
                        first_date,
                        second_date,
                        interval,
                    )
                    wi_volumes.append(wi_volume)

                    wb_uuid = wellbore_volumes["WB_UUID"]
                    wb_uuids.append(wb_uuid)
                    inj_wellbores.append(pdm_name)

            dataframe = DataFrame()
            dataframe["WB_UWBI"] = inj_wellbores
            dataframe["WB_UUID"] = wb_uuids
            dataframe["GI_VOL"] = gi_volumes
            dataframe["WI_VOL"] = wi_volumes

            volumes = wb.InjectionVolumes(
                gi_unit,
                wi_unit,
                co2_unit,
                first_date,
                second_date,
                dataframe,
            )

            return volumes

    def get_faultlines(
        self,
        field_uuid: str,
    ) -> wb.Polygons:
        polygons = None
        dataframe = DataFrame()

        filter = "Field/" + field_uuid + "/model"

        selected_model = extract_default_model(self.ssdl_address, filter)

        dataframe = DataFrame()

        if selected_model is not None:
            model_uuid = selected_model["model_uuid"][0]
            filter = "Field/%s/faultlines" % model_uuid

            dataframe = extract_faultlines(self.ssdl_address, filter)
            dataframe["name"] = "faults"

        polygons = wb.Polygons(dataframe)

        return polygons

    def get_ssdl_wellbores(
        self,
        field_uuid: str,
    ) -> wb.DrilledWellboreMetadata:
        filter = "Field/" + field_uuid + "/wellbore"
        dataframe = extract_ssdl_wellbores(self.ssdl_address, filter)
        metadata = wb.DrilledWellboreMetadata(dataframe)

        return metadata

    def get_field_outlines(
        self,
        field_uuid: str,
    ) -> wb.Polygons:
        polygons = None
        dataframe = DataFrame()

        filter = "Field/" + field_uuid + "/model"
        selected_model = extract_default_model(self.ssdl_address, filter)

        dataframe = DataFrame()

        if selected_model is not None:
            model_uuid = selected_model["model_uuid"][0]
            print("Selected model:", selected_model["model_identifier"][0])
            frames = []
            outline_types = ["OWC", "GOC", "UNSPECIFIED"]

            for outline in outline_types:
                filter = "Field/" + model_uuid + "/outlines/" + outline
                df = extract_outlines(self.ssdl_address, filter)

                if not df.empty:
                    frames.append(df)
        if frames:
            dataframe = concat(frames)
        else:
            dataframe = DataFrame()

        polygons = wb.Polygons(dataframe)

        return polygons

    def get_wellbore_completions(
        self,
        field_uuid: str,
        wellbore_uuids: Optional[list] = None,
    ) -> wb.Completions:
        completions = None
        dataframe = DataFrame()
        frames = []

        if wellbore_uuids is None:
            wellbores = self.get_ssdl_wellbores(field_uuid)
            wellbores_df = wellbores.dataframe
            wellbore_uuids = wellbores_df["wellbore_uuid"].to_list()

        for uuid in wellbore_uuids:
            filter = "Wellbores/" + uuid + "/completion"
            completion = extract_ssdl_completion(self.ssdl_address, filter)
            frames.append(completion)

        dataframe = concat(frames)

        completions = wb.Completions(dataframe)

        return completions

    def get_wellbore_perforations(
        self,
        field_uuid: str,
        wellbore_uuids: Optional[list] = None,
    ) -> wb.Perforations:
        perforations = None
        dataframe = DataFrame()
        frames = []

        if wellbore_uuids is None:
            wellbores = self.get_ssdl_wellbores(field_uuid)
            wellbores_df = wellbores.dataframe
            wellbore_uuids = wellbores_df["wellbore_uuid"].to_list()

        for uuid in wellbore_uuids:
            filter = "Wellbores/" + uuid + "/perforations"
            perforation = extract_ssdl_perforation(self.ssdl_address, filter)
            frames.append(perforation)

        dataframe = concat(frames)

        perforations = wb.Perforations(dataframe)

        return perforations

    def get_field_uuid(self, field_name):
        wellbores = self.get_smda_wellbores(field_name)
        wellbore = wellbores.iloc[0]

        return wellbore.field_uuid

    def get_smda_wellbores(
        self,
        field_name: str,
        wellbore_names: Optional[str] = None,
    ) -> DataFrame:
        metadata = self.drilled_wellbore_metadata(
            field=field_name,
        )

        if metadata:
            dataframe = metadata.dataframe

            if wellbore_names:
                wellbore_metadata = dataframe[
                    dataframe["unique_wellbore_identifier"].isin(wellbore_names)
                ]
                wellbore_uuids = wellbore_metadata[
                    ["uuid", "unique_wellbore_identifier", "field_uuid"]
                ]

            else:
                wellbore_uuids = dataframe
        else:
            print("ERROR: Not able to find wellbore metata for field:", field_name)
            wellbore_uuids = DataFrame()

        return wellbore_uuids

    def get_all_names(wellbore_uuids, csd_wellbores, pdm_wellbores):
        wellbores_overview = DataFrame()

        return wellbores_overview

    def planned_wellbore_metadata(
        self,
        field: str,
    ) -> wb.PlannedWellboreMetadata:
        metadata = None

        filter = "field_identifier=" + field
        print("Loading planned well metadata from SMDA")
        metadata_df = extract_planned_metadata(self.smda_address, filter)

        if not metadata_df.empty:
            metadata = wb.PlannedWellboreMetadata(metadata_df)

        return metadata
