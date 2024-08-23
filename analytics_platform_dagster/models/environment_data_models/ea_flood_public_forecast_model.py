from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

class FloodRiskTrend(BaseModel):
    day1: str
    day2: str
    day3: str
    day4: str
    day5: str

class Source(BaseModel):
    coastal: Optional[str] = None
    surface: Optional[str] = None
    river: Optional[str] = None
    ground: Optional[str] = None

class PublicForecast(BaseModel):
    england_forecast: str
    english_forecast: str
    id: int
    published_at: datetime
    wales_forecast_english: str
    wales_forecast_welsh: str
    welsh_forecast: str

class AocMap(BaseModel):
    id: int
    title: str

class RiskArea(BaseModel):
    id: int
    statement_id: int

class Statement(BaseModel):
    amendments: str
    aoc_maps: Optional[List[AocMap]]
    area_of_concern_url: str
    detailed_csv_url: str
    flood_risk_trend: FloodRiskTrend
    future_forecast: str
    headline: str
    id: int
    issued_at: datetime
    last_modified_at: datetime
    next_issue_due_at: datetime
    pdf_url: str
    png_thumbnails_with_days_url: str
    public_forecast: PublicForecast
    risk_areas: Optional[List[RiskArea]]
    sources: List[Source]

class FloodRiskData(BaseModel):
    statement: Statement
