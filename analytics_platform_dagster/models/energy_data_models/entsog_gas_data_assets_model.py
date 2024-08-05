from pydantic import BaseModel
from typing import Optional, List, Union, Any


class Entsog(BaseModel):
    id: Optional[str] = None
    dataSet: Optional[Any] = None
    indicator: Optional[str] = None
    periodType: Optional[str] = None
    periodFrom: Optional[str] = None
    periodTo: Optional[str] = None
    operatorKey: Optional[str] = None
    tsoEicCode: Optional[str] = None
    operatorLabel: Optional[str] = None
    pointKey: Optional[str] = None
    pointLabel: Optional[str] = None
    tsoItemIdentifier: Optional[str] = None
    directionKey: Optional[str] = None
    unit: Optional[str] = None
    itemRemarks: Optional[str] = None
    generalRemarks: Optional[str] = None
    value: Optional[Union[int, str]] = None
    lastUpdateDateTime: Optional[str] = None
    isUnlimited: Optional[Any] = None
    flowStatus: Optional[str] = None
    interruptionType: Optional[str] = None
    restorationInformation: Optional[str] = None
    capacityType: Optional[str] = None
    capacityBookingStatus: Optional[str] = None
    isCamRelevant: Optional[Any] = None
    isNA: Optional[Any] = None
    originalPeriodFrom: Optional[str] = None
    isCmpRelevant: Optional[Any] = None
    bookingPlatformKey: Optional[str] = None
    bookingPlatformLabel: Optional[str] = None
    bookingPlatformURL: Optional[str] = None
    interruptionCalculationRemark: Optional[str] = None
    pointType: Optional[str] = None
    idPointType: Optional[Any] = None
    isArchived: Optional[Any] = None


class EntsogModel(BaseModel):
    data: List[Entsog]
