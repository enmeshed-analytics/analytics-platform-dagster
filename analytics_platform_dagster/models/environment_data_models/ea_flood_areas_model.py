from pydantic import BaseModel, Field
from typing import List, Optional

class FloodAreas(BaseModel):
    id: Optional[str] = Field(default=None, alias="@id")
    county: Optional[str] = None
    description: Optional[str] = None
    eaAreaName: Optional[str] = None
    floodWatchArea: Optional[str] = None
    fwdcode: Optional[str] = None
    label: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    notation: Optional[str] = None
    polygon: Optional[str] = None
    quickDialNumber: Optional[str] = None
    riverOrSea: Optional[str] = None

class EaFloodAreasResponse(BaseModel):
    at_context: str = Field(alias="@context")
    meta: dict
    items: List[FloodAreas]
