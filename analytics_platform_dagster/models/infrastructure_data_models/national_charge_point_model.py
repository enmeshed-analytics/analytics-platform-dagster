from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

class GeoPoint(BaseModel):
    lon: float
    lat: float

class ChargeDevice(BaseModel):
    chargedeviceref: str
    chargedevicename: str
    rated_output_kw: str
    connector_type: Optional[str] = None
    devicenetworks: str
    chargedevicemanufacturer: str
    chargedevicemodel: str
    publishstatusid: int
    datecreated: datetime
    dateupdated: datetime
    deviceaccess: Optional[str] = None
    chargedevicestatus: str
    publishstatus: str
    devicevalidated: int
    recordmoderated: str
    recordlastupdated: Optional[datetime] = None
    recordlastupdatedby: Optional[str] = None
    paymentrequiredflag: str
    paymentdetails: Optional[str] = None
    subscriptionrequiredflag: str
    subscriptiondetails: Optional[str] = None
    parkingfeesflag: str
    parkingfeesdetails: Optional[str] = None
    parkingfeesurl: Optional[str] = None
    accessrestrictionflag: str
    accessrestrictiondetails: Optional[str] = None
    physicalrestrictionflag: str
    physicalrestrictiontext: Optional[str] = None
    onstreetflag: str
    locationtype: str
    bearing: Optional[str] = None
    accessible24hours: str
    latitude: str
    longitude: str
    geo_point: GeoPoint

class ChargeDeviceResponse(BaseModel):
    total_count: int
    results: List[ChargeDevice]