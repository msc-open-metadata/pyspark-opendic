from pyspark.sql.catalog import Catalog
from pyspark.sql import SparkSession
import re
import requests
import json
from pyspark_opendic.client import OpenDicClient
from pyspark_opendic.model.create_object_request import CreateObjectRequest


class OpenDicCatalog(Catalog):
    def __init__(self, sparkSession : SparkSession, api_url : str):
        self.sparkSession = sparkSession
        self.client = OpenDicClient(api_url)
        #TODO: add handling for credentials for client OAuth

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

            # Handle properties as JSON if present
            try:
                # This serves to both check if the properties are valid JSON, and parse the raw JSON string into a Python dict (or None for consistency)
                properties = json.loads(properties) if properties else None
            except json.JSONDecodeError as e:
                return {
                    "error": "Invalid JSON syntax in properties",
                    "details": {"sql": sqlText, "exception_message": e.msg}
                }

            payload = CreateObjectRequest(object_type, name, alias, properties).to_json()
            
            try :
                response = self.client.post(f"/{object_type}", payload)
            except requests.exceptions.HTTPError as e:
                return {"error": "HTTP Error", "exception message": e.msg}
            return {"success": "Object created successfully", "response": response}
        
        elif show_match:
            object_type = show_match.group('object_type')
            try :
                response = self.client.get(f"/{object_type}")
            except requests.exceptions.HTTPError as e:
                return {"error": "HTTP Error", "exception message": e.msg}
            return {"success": "Object retrieved successfully", "response": response}

        # Fallback to Spark parser
        return self.sparkSession.sql(sqlText)

