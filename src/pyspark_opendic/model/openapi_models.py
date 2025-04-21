from pydantic import BaseModel
from typing import Any, List, Optional
from typing import Literal

class Udo(BaseModel):
    type: str
    name: str
    props: Optional[dict[str, Any]] = None
    createdTimestamp: Optional[str] = None
    lastUpdatedTimestamp: Optional[str] = None
    entityVersion: Optional[int] = None

class Udos(BaseModel):
    objects: List[Udo]  # List of Udo objects

class CreateUdoRequest(BaseModel):
    udo: Udo  # A single Udo object that will be created

class DefineUdoRequest(BaseModel):
    udoType: str  # Type of the object
    properties: dict[str, str]  # Properties of the object

class PullUdoRequest(BaseModel):
    Udos: Udos  # A list of Udo objects
    platformMapping: 'PlatformMapping'  # The platform mapping for this request


class SnowflakePlatformMapping(BaseModel):
    jsonMapping: dict[str, str]  # Mapping in JSON format

class SparkPlatformMapping(BaseModel):
    jsonMapping: dict[str, str]  # Mapping in JSON format


class PullStatements(BaseModel):
    statements: List['Statement']  # A list of SQL statements

class Statement(BaseModel):
    definition: str  # The SQL statement definition

class PlatformMappingObjectDumpMapValue(BaseModel):
    propType: str
    format: str
    delimiter: str

class PlatformMapping(BaseModel):
    typeName: str
    platformName: str
    syntax: str
    objectDumpMap: dict[str, PlatformMappingObjectDumpMapValue]

class CreatePlatformMappingRequest(BaseModel):
    platformMapping: PlatformMapping