from typing import Optional,  List
from pydantic import BaseModel, field_validator


class DBConfig(BaseModel):
    dbname: str
    db_schema: str
    user: str
    password: str
    host: str
    port: int = 5432


class StorageConfig(BaseModel):
    bucket: str
    laz_path: str
    fix_path: str
    reclassify_path: str
    dem_path: str
    etak_path: str
    ndvi_path: str


class LidarConfig(BaseModel):
    laz_mapsheets: List[int]
    laz_to_crs: Optional[str] = 'EPSG:3301'
    laz_year: int
    laz_type: str
    dem_year: int

    @field_validator('laz_type')
    def mets_or_tava(cls, value):
        if value not in ['mets', 'tava']:
            raise ValueError('laz_type must be either "mets" or "tava".')
        return value
