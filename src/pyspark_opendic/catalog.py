from pyspark.sql.catalog import Catalog
from pyspark.sql import SparkSession
import re
import requests
import json
from pyspark_opendic.model.create_object_request import CreateObjectRequest


class OpenDicCatalog(Catalog):
    def __init__(self, sparkSession, api_url):
        self.api_url = api_url
        self.sparkSession = sparkSession
        #TODO: add handling for credentials for client OAuth

    def sql(self, sqlText : str):
        query_cleaned = sqlText.strip()

        # TODO: do some systematic syntax union - include alias 'as', etc.
        # TODO: add support for 'or replace' and 'temporary' keywords etc. on catalog-side
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
            properties_json = None

            # Handle properties as JSON if present
            if properties:
                try:
                    # Try parsing the properties as JSON
                    properties_json = json.loads(properties)
                except json.JSONDecodeError as e:
                    # Handle the error if JSON is invalid
                    return {"error": "Invalid JSON syntax in properties", "exception message": e.msg}

            payload = CreateObjectRequest(object_type, name, alias, properties_json).to_json()
            return payload #TODO: WE HAVE TO CHANGE THIS TO CALL REST CLIENT. Then maybe the test should be from the client instead? this was just for fast unit testing
            # TODO: Initialize openapi specific component e.g. the payload
            # TODO: Call REST client to create object
        elif show_match:
            object_type = show_match.group('object_type')
            # TODO: Call REST client to show object
            return "called show"

        # Fallback to Spark parser
        return self.sparkSession.sql(sqlText)

