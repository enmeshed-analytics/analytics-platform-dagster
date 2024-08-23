# IN PROGRESS

import requests
import pandas as pd
from io import BytesIO
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

class UKPNLiveFault(BaseModel):
    IncidentReference: Optional[str] = None
    PowerCutType: Optional[str] = None
    CreationDateTime: Optional[str] = None
    NoCallsReported: Optional[int] = None
    IncidentsCount: Optional[int] = None
    NoCustomerAffected: Optional[int] = None
    PostCodesAffected: Optional[str] = None
    RestoredIncidents: Optional[str] = None
    UnplannedIncidents: Optional[str] = None
    PlannedIncidents: Optional[str] = None
    IncidentTypeTBCEstimatedFriendlyDescription: Optional[str] = None
    IncidentDescription: Optional[float] = None
    FullPostcodeData: Optional[str] = None
    IncidentCategoryCustomerFriendlyDescription: Optional[str] = None
    IncidentCategory: Optional[float] = None
    IncidentTypeName: Optional[str] = None
    IncidentType: Optional[int] = None
    IncidentPriority: Optional[int] = None
    StatusId: Optional[int] = None
    RestoredDateTime: Optional[str] = None
    PlannedDate: Optional[str] = None
    ReceivedDate: Optional[str] = None
    NoPlannedCustomers: Optional[int] = None
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


def ukpn_live_faults_bronze():
    data = requests.get("https://ukpowernetworks.opendatasoft.com/api/explore/v2.1/catalog/datasets/ukpn-live-faults/exports/xlsx?lang=en&timezone=Europe%2FLondon&use_labels=true")
    bytes = BytesIO(data.content)
    df = pd.read_excel(bytes)
    validate = df.to_dict()
    UKPNLiveFault.model_validate(validate)
    return df

v = ukpn_live_faults_bronze()
print(v.info())
