from typing import Optional
from dataclasses import dataclass, field
from pydantic import BaseModel
from typing import Optional


@dataclass
class WellProductionData:
    eclipse_well_name: str
    wellbore_short_name: str
    mlt_name: str
    wellbore_uwi: str
    wellbore_uuid: str
    oil_production_volume: float
    gas_production_volume: float
    condensate_production_volume: float
    water_production_volume: float
    water_injection_volume: float
    gas_injection_volume: float
    co2_injection_volume: float
