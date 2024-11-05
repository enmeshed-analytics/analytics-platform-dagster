from pydantic import BaseModel
from typing import Optional

class GeoPoint(BaseModel):
    lon: float
    lat: float

class ChargeDevice(BaseModel):
    chargedeviceref: str
    chargedevicename: str
    rated_output_kw: str
    connector_type: Optional[str] = None
    devicenetworks: str
    chargedevicemanufacturer: Optional[str] = None
    chargedevicemodel: Optional[str] = None
    publishstatusid: int
    datecreated: str
    dateupdated: str
    deviceaccess: Optional[str] = None
    chargedevicestatus: str
    publishstatus: str
    devicevalidated: int
    recordmoderated: str
    recordlastupdated: Optional[str] = None
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