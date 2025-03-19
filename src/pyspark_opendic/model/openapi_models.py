from pydantic import BaseModel
from typing import Any, List, Dict, Optional
from typing import Literal

class Udo(BaseModel):
    type: str
    name: str
    props: Optional[Dict[str, Any]] = None
    createTimestamp: Optional[int] = None
    lastUpdateTimestamp: Optional[int] = None
    entityVersion: Optional[int] = None

class Udos(BaseModel):
    objects: List[Udo]  # List of Udo objects

class CreateUdoRequest(BaseModel):
    udo: Udo  # A single Udo object that will be created

class DefineUdoRequest(BaseModel):
    udoType: str  # Type of the object
    properties: Dict[str, str]  # Properties of the object

class PullUdoRequest(BaseModel):
    Udos: Udos  # A list of Udo objects
    platformMapping: 'PlatformMapping'  # The platform mapping for this request

class PlatformMapping(BaseModel):
    platformName: Literal['SNOWFLAKE', 'SPARK']  # The platform type (only SNOWFLAKE or SPARK)
    objectType: str  # The type of the object (e.g., Function, Role)

class PlatformMappings(BaseModel):
    items: List[PlatformMapping]  # A list of platform mappings

class SnowflakePlatformMapping(BaseModel):
    jsonMapping: Dict[str, str]  # Mapping in JSON format

class SparkPlatformMapping(BaseModel):
    jsonMapping: Dict[str, str]  # Mapping in JSON format

class CreatePlatformMappingRequest(BaseModel):
    platformMapping: PlatformMapping  # The platform mapping to be created

class PullStatements(BaseModel):
    statements: List['Statement']  # A list of SQL statements

class Statement(BaseModel):
    definition: str  # The SQL statement definition