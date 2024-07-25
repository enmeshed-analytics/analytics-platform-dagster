from pydantic import BaseModel, Field
from typing import List, Union

class GenerationMix(BaseModel):
    fuel: str
    perc: Union[float, int]

class Intensity(BaseModel):
    forecast: int
    index: str

class RegionData(BaseModel):
    from_: str = Field(..., alias="from")
    to: str
    intensity: Intensity
    generationmix: List[GenerationMix]

class Region(BaseModel):
    regionid: int
    dnoregion: str
    shortname: str
    data: List[RegionData]

class CarbonIntensityResponse(BaseModel):
    data: List[Region]
