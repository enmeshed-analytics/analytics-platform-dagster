from pydantic import BaseModel
from typing import Optional

class UKPNLiveFault(BaseModel):
    IncidentReference: str
    PowerCutType: str
    CreationDateTime: str
    NoCallsReported: int
    IncidentsCount: int
    NoCustomerAffected: int
    PostCodesAffected: str
    RestoredIncidents: Optional[str] = None
    UnplannedIncidents: Optional[str] = None
    PlannedIncidents: Optional[str] = None
    IncidentTypeTBCEstimatedFriendlyDescription: Optional[str] = None
    IncidentDescription: Optional[float] = None
    FullPostcodeData: Optional[str] = None
    IncidentCategoryCustomerFriendlyDescription: Optional[str] = None
    IncidentCategory: Optional[float] = None
    IncidentTypeName: Optional[str] = None
    IncidentType: int
    IncidentPriority: int
    StatusId: int
    RestoredDateTime: Optional[str] = None
    PlannedDate: Optional[str] = None
    ReceivedDate: Optional[str] = None
    NoPlannedCustomers: int
    PlannedIncidentReason: Optional[str] = None
    Message: Optional[float] = None
    MainMessage: Optional[str] = None
    geopoint: Optional[str] = None
    EstimatedRestorationDate: Optional[str] = None
    PrimaryFeeder: Optional[str] = None
    PrimaryName: Optional[str] = None
    PrimaryAlias: Optional[str] = None
    SecondaryFeeder: Optional[float] = None
    SecondaryName: Optional[str] = None
    SecondaryAlias: Optional[str] = None
    DeadDeviceAlias: Optional[str] = None

    class Config:
        allow_population_by_field_name = True
