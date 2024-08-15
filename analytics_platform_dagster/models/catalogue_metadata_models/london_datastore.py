from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any
from datetime import datetime


class Resource(BaseModel):
    format: str
    title: str
    url: str
    check_hash: Optional[str] = None
    check_http_status: Optional[int] = None
    check_mimetype: Optional[str] = None
    check_size: Optional[int] = None
    check_timestamp: Optional[datetime] = None
    description: Optional[str] = None
    london_release_date: Optional[str] = None
    london_res_geo: Optional[List[str]] = None
    order: Optional[int] = None
    searchDescription: Optional[str] = None
    searchFilename: Optional[str] = None
    searchTitle: Optional[str] = None
    temporal_coverage_from: Optional[str] = None
    temporal_coverage_to: Optional[str] = None


class DatasetItem(BaseModel):
    author: Optional[str] = None
    author_email: Optional[str] = None
    createdAt: Optional[datetime] = None
    description: Optional[str] = None
    id: str
    licence: Optional[str] = None
    licence_notes: Optional[str] = None
    maintainer: Optional[str] = None
    maintainer_email: Optional[str] = None
    resources: Dict[str, Resource]
    shares: Optional[Dict[str, Any]] = None
    sharing: Optional[str] = None
    slug: str
    state: Optional[str] = None
    tags: Optional[List[str]] = None
    title: str
    topics: Optional[List[str]] = None
    update_frequency: Optional[str] = None
    updatedAt: Optional[datetime] = None
    parent: Optional[str] = None
    london_smallest_geography: Optional[str] = None
    london_bounding_box: Optional[str] = None
    odi_certificate: Optional[str] = Field(None, alias="odi-certificate")


class LondonDatastoreCatalogue(BaseModel):
    items: List[DatasetItem]
