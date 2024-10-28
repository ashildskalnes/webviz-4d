from typing import Optional
from dataclasses import dataclass, field
from pydantic import BaseModel
from typing import Optional


class Schema(BaseModel):
    id: str
    kind: str


@dataclass
class SeismicHorizon:
    id: str
    kind: str
    Name: str
    FieldID: str
    SeismicDomainTypeID: str
    Datasets: list[str]
    InterpretationName: str
    BinGridID: str
    SeismicTraceDataID: str
    InlineMin: float
    InlineMax: float
    InlineIncrement: float
    CrosslineMin: float
    CrosslineMax: float
    CrosslineIncrement: float


# class SeismicBinGrid(Schema):
#     name: str


@dataclass
class SeismicAttributeHorizon_033:
    id: str
    kind: str
    Name: str
    Source: Optional[str] = ""
    MetadataVersion: Optional[str] = ""
    AttributeMap_FieldName: Optional[str] = ""
    AttributeMap_AttributeType: Optional[str] = ""
    AttributeMap_Coverage: Optional[str] = ""
    AttributeMap_DifferenceType: Optional[str] = ""
    AttributeMap_MapType: Optional[str] = ""
    AttributeMap_Name: Optional[str] = ""
    AttributeMap_SeismicDifference: Optional[str] = ""
    AttributeMap_SeismicTraceContent: Optional[str] = ""
    CalculationWindow_WindowMode: Optional[str] = ""
    CalculationWindow_TopHorizonName: Optional[str] = ""
    CalculationWindow_BaseHorizonName: Optional[str] = ""
    CalculationWindow_TopHorizonOffset: Optional[str] = ""
    CalculationWindow_BaseHorizonOffset: Optional[str] = ""
    CalculationWindow_HorizonName: Optional[str] = ""
    CalculationWindow_HorizonOffsetShallow: Optional[str] = ""
    CalculationWindow_HorizonOffsetDeep: Optional[str] = ""
    SeismicProcessingTraces_SeismicVolumeA: Optional[str] = ""
    SeismicProcessingTraces_SeismicVolumeB: Optional[str] = ""
    DatasetIDs: Optional[str] = ""


@dataclass
class SeismicAttributeHorizon_042:
    id: str
    kind: str
    Name: str
    MetadataVersion: str
    FieldName: str
    SeismicBinGridName: str
    ApplicationName: str
    MapTypeDimension: str
    SeismicTraceDataSource: str
    SeismicTraceDataSourceNames: list
    SeismicTraceDomain: str
    SeismicTraceAttribute: str
    OsduSeismicTraceNames: list
    SeismicDifferenceType: str
    AttributeWindowMode: str
    HorizonDataSource: str
    HorizonSourceNames: list
    StratigraphicZone: str
    AttributeExtractionType: str
    AttributeDifferenceType: str
    SeismicCoverage: Optional[str] = "Unknown"
    SeismicTraceDataSourceIDs: Optional[list] = field(default_factory=lambda: ["", ""])
    StratigraphicColumn: Optional[str] = ""
    HorizonSourceIDs: Optional[list] = field(default_factory=lambda: ["", ""])
    HorizonOffsets: Optional[list] = field(default_factory=lambda: ["", ""])
    FixedWindowValues: Optional[list] = field(default_factory=lambda: ["", ""])
    DatasetIDs: Optional[list] = field(default_factory=lambda: ["", ""])


@dataclass
class SeismicAttributeInterpretation:
    id: str
    kind: str
    Name: str
    MetadataVersion: str
    Name: str
    FieldName: str
    SeismicBinGridName: str
    ApplicationName: str
    MapTypeDimension: str
    SeismicTraceDataSource: str
    SeismicTraceDataSourceNames: list
    SeismicTraceDomain: str
    SeismicTraceAttribute: str
    OsduSeismicTraceNames: list
    SeismicDifferenceType: str
    AttributeWindowMode: str
    HorizonDataSource: str
    HorizonSourceNames: list
    StratigraphicZone: str
    AttributeExtractionType: str
    AttributeDifferenceType: str
    SeismicCoverage: Optional[str] = "Unknown"
    SeismicTraceDataSourceIDs: Optional[list] = field(default_factory=lambda: ["", ""])
    StratigraphicColumn: Optional[str] = ""
    HorizonSourceIDs: Optional[list] = field(default_factory=lambda: ["", ""])
    HorizonOffsets: Optional[list] = field(default_factory=lambda: ["", ""])
    FixedWindowValues: Optional[list] = field(default_factory=lambda: ["", ""])
    DatasetIDs: Optional[list] = ""


@dataclass
class Dataset:
    id: str
    kind: str
    source: str


@dataclass
class SeismicAcquisitionSurvey:
    id: str
    kind: str
    ProjectName: str
    ProjectID: str
    ProjectBeginDate: str
    ProjectEndDate: str
    ProjectReferenceDate: str


@dataclass
class SeismicProject:
    id: str
    kind: str
    Name: str
    acquisition_survey_id: str


@dataclass
class SeismicTraceData:
    id: str
    kind: str
    Name: str
    InlineMin: float
    InlineMax: float
    CrosslineMin: float
    CrosslineMax: float
    SampleInterval: int
    SampleCount: int
    SeismicDomainTypeID: Optional[str] = ""
    PrincipalAcquisitionProjectID: Optional[str] = ""
    DatasetID: Optional[str] = ""
