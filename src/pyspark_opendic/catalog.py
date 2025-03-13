from pyspark.sql.catalog import Catalog
from pyspark.sql import SparkSession
import re
import requests
import json
from pyspark_opendic.client import OpenDicClient

from pyspark_opendic.model.openapi_models import CreateUdoRequest
from pyspark_opendic.model.openapi_models import Udo, PlatformMapping, SnowflakePlatformMapping, SparkPlatformMapping  # Import updated models



class OpenDicCatalog(Catalog):
    def __init__(self, sparkSession : SparkSession, api_url : str):
        self.sparkSession = sparkSession
        
        credentials = sparkSession.conf.get("spark.sql.catalog.polaris.credential")
        self.client = OpenDicClient(api_url, credentials)
        
    def sql(self, sqlText : str):
        query_cleaned = sqlText.strip()

        # TODO: do some systematic syntax union - include alias 'as', etc.
        # TODO: add support for 'or replace' and 'temporary' keywords etc. on catalog-side - not a priority for now
        # Syntax: CREATE [OR REPLACE] [TEMPORARY] OPEN <object_type> <name> [IF NOT EXISTS] [AS <alias>] [PROPS { <properties> }]
        opendic_create_pattern = (
            r"^create"                              # "create" at the start
            r"(?:\s+or\s+replace)?"                  # Optional "or replace"
            r"(?:\s+temporary)?"                     # Optional "temporary"
            r"\s+open\s+(?P<object_type>\w+)"        # Required object type after "open"
            r"\s+(?P<name>\w+)"                      # Required name of the object
            r"(?:\s+if\s+not\s+exists)?"             # Optional "if not exists"
            r"(?:\s+as\s+(?P<alias>\w+))?"           # Optional alias after "as"
            r"(?:\s+props\s*(?P<properties>\{[\s\S]*\}))?" # Optional "props" keyword, but curly braces are mandatory if present - This is a JSON object
        )
        # TODO: Add pattern match for Show, Describe, Drop, etc.

        opendic_show_pattern = (
            r"^show"                                # "show" at the start
            r"\s+open\s+(?P<object_type>\w+)"        # Required object type after "open"
            r"s?"                                    # Optionally match a trailing "s"
        )

        # Check pattern matches
        create_match = re.match(opendic_create_pattern, query_cleaned, re.IGNORECASE)
        show_match = re.match(opendic_show_pattern, query_cleaned, re.IGNORECASE)


        if create_match:
            object_type = create_match.group('object_type')
            name = create_match.group('name')
            alias = create_match.group('alias')
            properties = create_match.group('properties')  

            # Parse props as JSON - this serves as a basic syntax check on the JSON input and default to None for consistency
            try:
                props = json.loads(properties) if properties else None
            except json.JSONDecodeError as e:
                return {
                    "error": "Invalid JSON syntax in properties",
                    "details": {"sql": sqlText, "exception_message": str(e)}
                }

            # Build Udo and CreateUdoRequest models
            udo = Udo(type=object_type, name=name, props=props)
            create_request = CreateUdoRequest(object=udo)

            # Serialize to JSON
            payload = create_request.model_dump_json()
            
            # Send Request
            try:
                response = self.client.post(f"/objects/{object_type}", payload)
            except requests.exceptions.HTTPError as e:
                return {"error": "HTTP Error", "exception message": str(e)}

            return {"success": "Object created successfully", "response": response}
        
        elif show_match:
            object_type = show_match.group('object_type')
            try :
                response = self.client.get(f"/objects/{object_type}")
            except requests.exceptions.HTTPError as e:
                return {"error": "HTTP Error", "exception message": str(e)}
            return {"success": "Object retrieved successfully", "response": response}

        # Fallback to Spark parser
        return self.sparkSession.sql(sqlText)

