from typing import Any, List, Literal, Optional

from pydantic import BaseModel


class Udo(BaseModel):
    type: str
    name: str
    props: Optional[dict[str, Any]] = None
    createTimestamp: Optional[int] = None
    lastUpdateTimestamp: Optional[int] = None
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

class PlatformMapping(BaseModel):
    platformName: Literal['SNOWFLAKE', 'SPARK']  # The platform type (only SNOWFLAKE or SPARK)
    objectType: str  # The type of the object (e.g., Function, Role)

class PlatformMappings(BaseModel):
    items: List[PlatformMapping]  # A list of platform mappings

class SnowflakePlatformMapping(BaseModel):
    jsonMapping: dict[str, str]  # Mapping in JSON format

class SparkPlatformMapping(BaseModel):
    jsonMapping: dict[str, str]  # Mapping in JSON format

class CreatePlatformMappingRequest(BaseModel):
    platformMapping: PlatformMapping  # The platform mapping to be created

class PullStatements(BaseModel):
    statements: List['Statement']  # A list of SQL statements

class Statement(BaseModel):
    definition: str  # The SQL statement definition

class AddMappingRequest(BaseModel):
    udoType: str
    platform: str
    syntax: dict[str, Any]
    propertyMappings: dict[str, Any]
