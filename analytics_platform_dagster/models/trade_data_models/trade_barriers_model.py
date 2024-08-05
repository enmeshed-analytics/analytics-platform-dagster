from typing import List, Any, Optional
from pydantic import BaseModel


class Category(BaseModel):
    pass


class OverseasRegion(BaseModel):
    id: str
    name: str


class TradingBloc(BaseModel):
    code: str
    name: str
    overseas_regions: List[OverseasRegion]
    short_name: str


class Country(BaseModel):
    name: str
    trading_bloc: Optional[TradingBloc] = None


class Sector(BaseModel):
    name: str


class Barrier(BaseModel):
    categories: List[Category]
    caused_by_trading_bloc: Optional[Any] = None
    country: Country
    id: str
    is_resolved: bool
    last_published_on: str
    location: str
    reported_on: Optional[str] = None
    sectors: List[Sector]
    status_date: str
    summary: str
    title: str
    trading_bloc: Optional[str] = None


class TradingBarriers(BaseModel):
    barriers: List[Barrier]

    model_config = {"extra": "forbid"}
