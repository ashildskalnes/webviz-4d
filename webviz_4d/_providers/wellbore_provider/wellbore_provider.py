import abc
from typing import Optional
from dataclasses import dataclass
from typing import List
from requests import Session

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class SmdaAddress:
    api: str
    session: Session


@dataclass(frozen=True)
class PozoAddress:
    api: str
    session: Session


@dataclass(frozen=True)
class PdmAddress:
    api: str
    session: Session


@dataclass(frozen=True)
class SsdlAddress:
    api: str
    session: Session


@dataclass
class Trajectory:
    coordinate_system: str
    x_arr: np.ndarray
    y_arr: np.ndarray
    z_arr: np.ndarray
    md_arr: np.ndarray


@dataclass
class Trajectories:
    coordinate_system: str
    dataframe: pd.DataFrame(
        columns=[
            "wellbore_uuid",
            "unique_wellbore_identifier",
            "easting",
            "northing",
            "tvd_msl",
            "md",
        ]
    )


@dataclass
class WellborePicks:
    dataframe: pd.DataFrame(
        columns=[
            "unique_wellbore_identifier",
            "pick_identifier",
            "interpreter",
            "md",
            "tvd_msl",
            "obs_no",
        ]
    )


@dataclass
class DrilledWellboreMetadata:
    dataframe: pd.DataFrame(
        columns=[
            "uuid",
            "unique_wellbore_identifier",
            "unique_well_identifier",
            "parent_wellbore",
            "purpose",
            "status",
            "content",
            "field_identifier",
            "field_uuid",
            "completion_date",
            "license_identifier",
        ]
    )


@dataclass
class PlannedWellboreMetadata:
    dataframe: pd.DataFrame(
        columns=[
            "name",
            "purpose",
            "status",
            "templateName",
            "fieldName",
            "wellTypeName",
            "updateDate",
        ]
    )


@dataclass
class PlannedTrajectories:
    dataframe: pd.DataFrame(
        columns=[
            "name",
            "field_name",
            "easting",
            "northing",
            "tvd_msl",
            "md",
        ]
    )


@dataclass
class PlannedWellbores:
    metadata: PlannedWellboreMetadata
    trajectories: Trajectories


@dataclass
class Completions:
    dataframe: pd.DataFrame(
        columns=[
            "wellbore_id",
            "wellbore_uuid",
            "symbol_name",
            "description",
            "md_top",
            "md_bottom",
            "field_id",
        ]
    )


@dataclass
class Perforations:
    dataframe: pd.DataFrame(
        columns=[
            "wellbore_id",
            "wellbore_uuid",
            "md_top",
            "md_bottom",
            "field_id",
        ]
    )


@dataclass
class DailyProductionVolumes:
    dataframe: pd.DataFrame(
        columns=[
            "WB_UWBI",
            "WB_UUID",
            "GOV_WB_NAME",
            "WELL_UWI",
            "PROD_DAY",
            "WB_OIL_VOL_SM3",
            "WB_GAS_VOL_SM3",
            "WB_WATER_VOL_M3",
            "GOV_FIELD_NAME",
        ]
    )


@dataclass
class DailyInjectionVolumes:
    dataframe: pd.DataFrame(
        columns=[
            "WB_UWBI",
            "WB_UUID",
            "GOV_WB_NAME",
            "WELL_UWI",
            "PROD_DAY",
            "INJ_TYPE",
            "WB_INJ_VOL",
            "GOV_FIELD_NAME",
        ]
    )


@dataclass
class PdmDates:
    dataframe: pd.DataFrame(columns=["WB_UWBI", "WB_START_DATE", "WB_END_DATE"])


@dataclass
class ProductionVolumes:
    oil_unit: str
    gas_unit: str
    water_unit: str
    first_date: str
    last_date: str
    dataframe: pd.DataFrame(
        columns=[
            "WB_UWBI",
            "WB_UUID",
            "OIL_VOL",
            "GAS_VOL",
            "WATER_VOL",
        ]
    )


@dataclass
class InjectionVolumes:
    gas_unit: str
    water_unit: str
    co2_unit: str
    first_date: str
    last_data: str
    dataframe: pd.DataFrame(
        columns=["WB_UWBI", "WB_UUID", "GI_VOL", "WI_VOL", "CI_VOL"]
    )


@dataclass
class FaultLines:
    dataframe: pd.DataFrame(columns=["name", "SEG I.D.", "geometry", "coordinates"])


@dataclass
class Polygons:
    dataframe: pd.DataFrame(columns=["name", "label", "geometry", "coordinates"])


# Class provides data for wellbores
class WellboreProvider(abc.ABC):
    @abc.abstractmethod
    def drilled_wellbore_metadata(
        self, smda_address: SmdaAddress, field: str, license: str
    ) -> DrilledWellboreMetadata:
        """Returns metadata for all drilled wellbores."""

    @abc.abstractmethod
    def drilled_wellbore_trajectory(
        self,
        smda_address: SmdaAddress,
        wellbore_name: str,
        md_min: Optional[float] = 0,
        md_max: Optional[float] = None,
    ) -> Trajectory:
        """Returns a wellbore trajectory (optionally between md_min and md_max)."""

    @abc.abstractmethod
    def planned_wellbore_metadata(self, PozoAddress) -> PlannedWellboreMetadata:
        """Returns metadata for all planned wellbores."""

    @abc.abstractmethod
    def get_wellbore_completions(self, SsdlAddress, wellbore_name) -> Completions:
        """Returns wellbore completions information for all or selected wellbore."""

    @abc.abstractmethod
    def get_wellbore_perforations(self, SsdlAddress, wellbore_name) -> Perforations:
        """Returns wellbore completions information for all or selected wellbore."""

    @abc.abstractmethod
    def get_prod_data(
        self,
        daily_volumes: DailyProductionVolumes,
        first_date: str,
        second_date: str,
        pdm_well_names: Optional[list] = None,
    ) -> ProductionVolumes:
        """Returns produced volumes (all fluids) for all or selected PDM wells in a time interval"""

    @abc.abstractmethod
    def get_field_prod_data(
        self,
        field_name: str,
        first_date: str,
        second_date: str,
    ) -> ProductionVolumes:
        """Returns produced volumes (all fluids) for all PDM wells for a field in a given time interval"""

    @abc.abstractmethod
    def get_inj_data(
        self,
        daily_volumes: DailyProductionVolumes,
        first_date: str,
        second_date: str,
        pdm_well_names: Optional[list] = None,
    ) -> ProductionVolumes:
        """Returns injected volumes (all fluids) for all or selected PDM wells in a time interval"""

    @abc.abstractmethod
    def get_pdm_wellbores(
        self, PdmAddress, field_name: str, pdm_wellbores: Optional[List] = None
    ) -> PdmDates:
        """Returns PDM names, start date and last dates (all fluids) for a selected field ."""

    @abc.abstractmethod
    def get_faultlines(
        self,
        SsdlAddress,
        field_name: str,
    ) -> FaultLines:
        """Returns Fault lines from the default model for a selected field ."""

    @abc.abstractmethod
    def get_field_outlines(
        self,
        SsdlAddress,
        field_name: str,
    ) -> Polygons:
        """Returns Fault lines from the default model for a selected field ."""
