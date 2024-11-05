from typing import List, Dict, Any, Optional, Union
from pydantic import BaseModel, ConfigDict, Field

class AddressData(BaseModel):
    BuildingName: Optional[str] = None
    BuildingNumber: Optional[str] = None
    Country: Optional[str] = None
    County: Optional[str] = None
    DependantLocality: Optional[str] = None
    DoubleDependantLocality: Optional[str] = None
    PostCode: Optional[str] = None
    PostTown: Optional[str] = None
    Street: Optional[str] = None
    SubBuildingName: Optional[str] = None
    Thoroughfare: Optional[str] = None
    UPRN: Optional[str] = None

class Location(BaseModel):
    Address: Optional[AddressData] = None
    Latitude: float
    LocationLongDescription: Optional[str] = None
    LocationShortDescription: Optional[str] = None
    Longitude: float

class ConnectorData(BaseModel):
    ChargeMethod: Optional[str] = None
    ChargeMode: Optional[str] = None
    ChargePointStatus: Optional[str] = None
    ConnectorId: Optional[str] = None
    ConnectorType: Optional[str] = None
    Information: Optional[str] = None
    RatedOutputCurrent: Optional[str] = None
    RatedOutputVoltage: Optional[str] = None
    RatedOutputkW: Optional[str] = None
    TetheredCable: Optional[str] = None
    Validated: Optional[str] = None

class DeviceOrganization(BaseModel):
    OrganisationName: Optional[str] = None
    SchemeCode: Optional[str] = None
    TelephoneNo: Optional[str] = None
    Website: Optional[str] = None

class Hours(BaseModel):
    From: str
    To: str

class RegularOpening(BaseModel):
    Days: str
    Hours: Hours

class AnnualOpening(BaseModel):
    Date: str
    Hours: Hours

class DeviceAccessSchedule(BaseModel):
    RegularOpenings: Optional[List[RegularOpening]] = None
    AnnualOpenings: Optional[List[AnnualOpening]] = None

class ChargeDevice(BaseModel):
    AccessRestrictionDetails: Optional[str] = None
    AccessRestrictionFlag: Optional[bool] = None
    Accessible24Hours: Optional[bool] = None
    Attribution: Optional[str] = None
    Bearing: Optional[str] = None
    ChargeDeviceId: Optional[str] = None
    ChargeDeviceLocation: Location
    ChargeDeviceManufacturer: Optional[str] = None
    ChargeDeviceModel: Optional[str] = None
    ChargeDeviceName: Optional[str] = None
    ChargeDeviceRef: Optional[str] = None
    ChargeDeviceStatus: Optional[str] = None
    ChargeDeviceText: Optional[str] = None
    Connector: List[ConnectorData]
    DateCreated: Optional[str] = None
    DateDeleted: Optional[str] = None
    DateUpdated: Optional[str] = None
    DeviceAccess: Union[List, Dict[str, List[Dict[str, Any]]]] = Field(default_factory=list)
    DeviceController: DeviceOrganization
    DeviceNetworks: Optional[str] = None
    DeviceOwner: DeviceOrganization
    DeviceValidated: Optional[str] = None
    LocationType: Optional[str] = None
    OnStreetFlag: Optional[bool] = None
    ParkingFeesDetails: Optional[str] = None
    ParkingFeesFlag: Optional[bool] = None
    ParkingFeesUrl: Optional[str] = None
    PaymentDetails: Optional[str] = None
    PaymentRequiredFlag: Optional[bool] = None
    PhysicalRestrictionFlag: Optional[bool] = None
    PhysicalRestrictionText: Optional[str] = None
    PublishStatus: Optional[str] = None
    PublishStatusID: Optional[str] = None
    RecordLastUpdated: Optional[str] = None
    RecordLastUpdatedBy: Optional[str] = None
    RecordModerated: Optional[str] = None
    SubscriptionDetails: Optional[str] = None
    SubscriptionRequiredFlag: Optional[bool] = None

class SchemeData(BaseModel):
    OrganisationName: Optional[str] = None
    Website: Optional[str] = None
    TelephoneNo: Optional[str] = None

class Scheme(BaseModel):
    SchemeCode: Optional[str] = None
    SchemeData: SchemeData

class ChargeDeviceResponse(BaseModel):
    """Top level model"""
    Scheme: Scheme
    ChargeDevice: List[ChargeDevice]
    model_config = ConfigDict(strict=True, extra='forbid')
