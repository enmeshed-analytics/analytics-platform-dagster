from pydantic import BaseModel
from typing import Any

class UKPNLiveFault(BaseModel):
    IncidentReference: Any = None
    PowerCutType: Any = None
    CreationDateTime: Any = None
    NoCallsReported: Any = None
    IncidentsCount: Any = None
    NoCustomerAffected: Any = None
    PostCodesAffected: Any = None
    RestoredIncidents: Any = None
    UnplannedIncidents: Any = None
    PlannedIncidents: Any = None
    IncidentTypeTBCEstimatedFriendlyDescription: Any = None
    IncidentDescription: Any = None
    FullPostcodeData: Any = None
    IncidentCategoryCustomerFriendlyDescription: Any = None
    IncidentCategory: Any = None
    IncidentTypeName: Any = None
    IncidentType: Any = None
    IncidentPriority: Any = None
    StatusId: Any = None
    RestoredDateTime: Any = None
    PlannedDate: Any = None
    ReceivedDate: Any = None
    NoPlannedCustomers: Any = None
    PlannedIncidentReason: Any = None
    Message: Any = None
    MainMessage: Any = None
    geopoint: Any = None
    EstimatedRestorationDate: Any = None
    PrimaryFeeder: Any = None
    PrimaryName: Any = None
    PrimaryAlias: Any = None
    SecondaryFeeder: Any = None
    SecondaryName: Any = None
    SecondaryAlias: Any = None
    DeadDeviceAlias: Any = None

    class Config:
        extra = 'forbid'
