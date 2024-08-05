from pydantic import BaseModel, Field
from typing import Dict, List, Optional
from datetime import datetime


class Resource(BaseModel):
    check_hash: Optional[str] = None
    check_http_status: Optional[int] = None
    check_mimetype: Optional[str] = None
    check_size: Optional[int] = None
    check_timestamp: Optional[datetime] = None
    format: Optional[str] = None
    london_res_geo: List[str] = Field(default_factory=list)
    order: Optional[int] = None
    resource_type: Optional[str] = None
    searchDescription: Optional[str] = None
    searchFilename: Optional[str] = None
    searchTitle: Optional[str] = None
    title: Optional[str] = None
    url: Optional[str] = None


class Dataset(BaseModel):
    author: Optional[str] = None
    author_email: Optional[str] = None
    createdAt: Optional[datetime] = None
    description: Optional[str] = None
    id: str
    licence: Optional[str] = None
    licence_notes: Optional[str] = None
    london_smallest_geography: Optional[str] = None
    maintainer: Optional[str] = None
    maintainer_email: Optional[str] = None
    parent: Optional[str] = None
    resources: Dict[str, Resource] = Field(default_factory=dict)
    shares: Dict[str, Dict] = Field(default_factory=lambda: {"orgs": {}, "users": {}})
    sharing: Optional[str] = None
    slug: str
    state: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    title: str
    topics: List[str] = Field(default_factory=list)
    update_frequency: Optional[str] = None
    updatedAt: Optional[datetime] = None


class DataCollection(BaseModel):
    datasets: List[Dataset]
