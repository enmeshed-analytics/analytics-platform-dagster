from pydantic import BaseModel, Field
from typing import Optional, List

class GreenBeltEntity(BaseModel):
    dataset: Optional[str] = None
    end_date: Optional[str] = Field(None, alias='end-date')
    entity: Optional[str] = None
    entry_date: Optional[str] = Field(None, alias='entry-date')
    geometry: Optional[str] = None
    green_belt_core: Optional[str] = Field(None, alias='green-belt-core')
    local_authority_district: Optional[str] = Field(None, alias='local-authority-district')
    name: Optional[str] = None
    organisation_entity: Optional[str] = Field(None, alias='organisation-entity')
    point: Optional[str] = None
    prefix: Optional[str] = None
    reference: Optional[str] = None
    start_date: Optional[str] = Field(None, alias='start-date')
    typology: Optional[str] = None

class GreenBeltResponse(BaseModel):
    entities: List[GreenBeltEntity]

    class Config:
        allow_populate_by_name = True
