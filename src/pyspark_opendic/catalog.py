import json
import re
from pydantic import ValidationError
import requests
from pyspark.sql import SparkSession
from pyspark.sql.catalog import Catalog

from pyspark_opendic.client import OpenDicClient
from pyspark_opendic.model.openapi_models import (  # Import updated models
    CreatePlatformMappingRequest,
    CreateUdoRequest,
    DefineUdoRequest,
    PlatformMapping,
    Udo,
)


class OpenDicCatalog(Catalog):
    def __init__(self, sparkSession : SparkSession, api_url : str):
        self.sparkSession = sparkSession

        credentials = sparkSession.conf.get("spark.sql.catalog.polaris.credential")
        if credentials is None:
            raise ValueError("spark.sql.catalog.polaris.credential is not set")
        self.client = OpenDicClient(api_url, credentials)

    def sql(self, sqlText : str):
        query_cleaned = sqlText.strip()

        # TODO: do some systematic syntax union - include alias 'as', etc.
        # TODO: add support for 'or replace' and 'temporary' keywords etc. on catalog-side - not a priority for now, so just ignore
        # TODO: patterns are constants as of now. Should not be defined inside the function itself. They should be moved outside or somewhere else.
        # Syntax: CREATE [OR REPLACE] [TEMPORARY] OPEN <object_type> <name> [IF NOT EXISTS] [AS <alias>] [PROPS { <properties> }]
        opendic_create_pattern = (
            r"^create"                                      # "create" at the start
            r"(?:\s+or\s+replace)?"                         # Optional "or replace"
            r"(?:\s+temporary)?"                            # Optional "temporary"
            r"\s+open\s+(?P<object_type>\w+)"               # Required object type after "open"
            r"\s+(?P<name>\w+)"                             # Required name of the object
            r"(?:\s+if\s+not\s+exists)?"                    # Optional "if not exists"
            r"(?:\s+as\s+(?P<alias>\w+))?"                  # Optional alias after "as"
            r"(?:\s+props\s*(?P<properties>\{[\s\S]*\}))?"  # Optional "props" keyword, but curly braces are mandatory if present - This is a JSON object
        )

        # Syntax: SHOW OPEN TYPES
        # Example: SHOW OPEN TYPES
        opendic_show_types_pattern = (
            r"^show"                                         # "show" at the start
            r"\s+open\s+types$"                              # Required "open types"
        )

        # Syntax: SHOW OPEN <object_type>[s]
        # Example: SHOW OPEN functions
        opendic_show_pattern = (
            r"^show"                                        # "show" at the start
            r"\s+open\s+(?P<object_type>(?!types$)\w+)"     # Required object type after "open" and not "TYPES"
            r"s?$"                                           # Optionally match a trailing "s"
        )

        # Syntax: SYNC OPEN <object_type>[s]
        # Example: SYNC OPEN functions
        opendic_sync_pattern = (
            r"^sync"                                        # "sync" at the start
            r"\s+open\s+(?P<object_type>\w+)"               # Required object type after "open"
            r"s?"                                           # Optionally match a trailing "s"
        )

        # Syntax: DEFINE OPEN <udoType> PROPS { <properties> }
        # Example: sql = 'DEFINE OPEN function PROPS { "language": "string", "version": "string", "def":"string"}'
        # TODO: can we somehow add validation for wheter the props are defined with data types? as above, "language": "string".. can we validate that string is a data type etc.?
        opendic_define_pattern = (
            r"^define"                                      # "DEFINE" at the start
            r"\s+open\s+(?P<udoType>\w+)"                   # Required UDO type (e.g., "function")
            r"(?:\s+props\s*(?P<properties>\{[\s\S]*\}))?"  # REQUIRED PROPS with JSON inside {}
        )

        # Syntax: DROP OPEN <object_type>
        # Example: DROP OPEN function
        opendic_drop_pattern = (
            r"^drop"                                        # "DROP" at the start
            r"\s+open\s+(?P<object_type>\w+)"               # Required object type after "open"
        )

        # Example:
        # ADD OPEN MAPPING function PLATFORM snowflake SYNTAX {
        #     CREATE OR ALTER <type> <signature>
        #     RETURNS <return_type>
        #     LANGUAGE <language>
        #     RUNTIME = <runtime>
        #     HANDLER = '<name>'
        #     AS $$
        #     <def>
        #     $$
        # } PROPS { "args": { "propType": "map", "format": "<key> <value>", "delimiter": ", " }, ... }
        opendic_add_mapping_pattern = (
            r"^add"
            r"\s+open\s+mapping"
            r"\s+(?P<object_type>\w+)"
            r"\s+platform\s+(?P<platform>\w+)"
            r"\s+syntax\s*\{\s*(?P<syntax>[\s\S]*?)\s*\}"
            r"\s+props\s*(?P<props>\{[\s\S]*?\})"
            r"$"
        )

        # Syntax: SHOW OPEN MAPPING <object_type> PLATFORM <platform>
        # Example: SHOW OPEN MAPPING function PLATFORM snowflake
        opendic_show_mapping_pattern = (
            r"^show"                                         # "show" at the start
            r"\s+open\s+mapping"                             # "open mapping"
            r"\s+(?P<object_type>\w+)"                       # Object type (e.g., function) - some defined UDO with a mapping TODO: we should have a check on Polaris side if a type and mapping exists.
            r"\s+platform\s+(?P<platform>\w+)"               # Platform name (e.g., snowflake)
            r"$"                                             # End of string
        )

        # Syntax: SHOW OPEN PLATFORMS FOR <object_type>
        # Example: SHOW OPEN PLATFORMS FOR function
        opendic_show_platforms_pattern = (
            r"^show"                                         # "show" at the start
            r"\s+open\s+platforms\s+for"                     # "open platforms for"
            r"\s+(?P<object_type>\w+)"                       # Object type (e.g., function)
            r"$"                                             # End of string
        )

        # Check pattern matches
        create_match = re.match(opendic_create_pattern, query_cleaned, re.IGNORECASE)
        show_types_match = re.match(opendic_show_types_pattern, query_cleaned, re.IGNORECASE)
        show_match = re.match(opendic_show_pattern, query_cleaned, re.IGNORECASE)
        sync_match = re.match(opendic_sync_pattern, query_cleaned, re.IGNORECASE)
        define_match = re.match(opendic_define_pattern, query_cleaned, re.IGNORECASE)
        drop_match = re.match(opendic_drop_pattern, query_cleaned, re.IGNORECASE)
        show_mapping_match = re.match(opendic_show_mapping_pattern, query_cleaned, re.IGNORECASE)
        show_mapping_platforms_match = re.match(opendic_show_platforms_pattern, query_cleaned, re.IGNORECASE)
        add_mapping_match = re.match(opendic_add_mapping_pattern, query_cleaned, re.IGNORECASE | re.DOTALL)

        if create_match:
            object_type = create_match.group('object_type')
            name = create_match.group('name')
            alias = create_match.group('alias')
            properties = create_match.group('properties')

            # Parse props as JSON - this serves as a basic syntax check on the JSON input and default to None for consistency
            try:
                create_props: dict[str, str] = json.loads(properties) if properties else {}
            
                # Build Udo and CreateUdoRequest Pydantic models            
                udo_object = Udo(type=object_type, name=name, props=create_props)
                create_request = CreateUdoRequest(udo=udo_object)
            
                # Serialize to JSON
                payload = create_request.model_dump()

                # Send Request
                response = self.client.post(f"/objects/{object_type}", payload)
                # Sync the object of said type after creation
                # sync_response = self.client.get(f"/objects/{object_type}/sync")
                # dump_handler_response = self.dump_handler(sync_response) # TODO: we should probably parse this to the PullStatements model we have for consistency and readability? not that important
            except requests.exceptions.HTTPError as e:
                return {"error": "HTTP Error", "exception message": str(e)}
            except ValidationError as e:
                return {"error": "Error creating object", "exception message": str(e)}
            except json.JSONDecodeError as e:
                return {
                    "error": "Invalid JSON syntax in properties",
                    "details": {"sql": sqlText, "exception_message": str(e)}
                }
            
            return {"success": "Object created successfully", "response": response}
                    # , "sync_response": dump_handler_response}

        elif show_types_match:
            try:
                response = self.client.get("/objects")
            except requests.exceptions.HTTPError as e:
                return {"error": "HTTP Error", "exception message": str(e)}

            return {"success": "Object types retrieved successfully", "response": response}

        elif show_match:
            object_type = show_match.group('object_type')
            try :
                response = self.client.get(f"/objects/{object_type}")
            except requests.exceptions.HTTPError as e:
                return {"error": "HTTP Error", "exception message": str(e)}

            return {"success": "Objects retrieved successfully", "response": response}
        
        elif show_mapping_match:
            object_type = show_mapping_match.group('object_type')
            platform = show_mapping_match.group('platform')
            try:
                response = self.client.get(f"/objects/{object_type}/platforms/{platform}")
            except requests.exceptions.HTTPError as e:
                return {"error": "HTTP Error", "exception message": str(e)}
            
            return {"success": "Mapping retrieved successfully", "response": response}
        
        elif show_mapping_platforms_match:
            object_type = show_mapping_platforms_match.group('object_type')
            try:
                response = self.client.get(f"/objects/{object_type}/platforms")
            except requests.exceptions.HTTPError as e:
                return {"error": "HTTP Error", "exception message": str(e)}

            return {"success": "Platforms retrieved successfully", "response": response}

        elif sync_match: # TODO: support for both sync all or sync just one object
            object_type = sync_match.group('object_type')
            try :
                response = self.client.get(f"/objects/{object_type}/sync")
            except requests.exceptions.HTTPError as e:
                return {"error": "HTTP Error", "exception message": str(e)}

            return self.dump_handler(response)
        elif define_match:
            # FIXME: I have refactored this switch case. I propose we make the rest more neat like this.
            udoType: str = define_match.group('udoType')
            properties: str = define_match.group('properties')

            try:
                # Parse props as JSON - this serves as a basic syntax check on the JSON input. Default to {}
                define_props: dict[str, str] = json.loads(properties) if properties else {}
                # Build Udo and CreateUdoRequest models
                define_request = DefineUdoRequest(udoType=udoType, properties=define_props)
                # This is a basic check, but we should probably add a more advanced one later on
                self.validate_data_type(define_props)
                # Serialize to JSON
                payload = define_request.model_dump()
                # Send Request
                response = self.client.post("/objects", payload)
                return {"success": "Object defined successfully", "response": response}
            except json.JSONDecodeError as e:
                return {
                    "error": "Invalid JSON syntax in properties",
                    "details": {"sql": sqlText, "exception_message": str(e)}
                }
            except ValueError as e:
                return {"error": "Invalid type for DEFINE statement", "exception message": str(e)}
            except requests.exceptions.HTTPError as e:
                return {"error": "HTTP Error", "exception message": str(e)}
            except ValidationError as e:
                return {"error": "Error defining object", "exception message": str(e)}

        # Not sure if we should support dropping a specific object tuple, and not the whole table?
        elif drop_match:
            object_type = drop_match.group('object_type')
            try:
                response = self.client.delete(f"/objects/{object_type}")
            except requests.exceptions.HTTPError as e:
                return {"error": "HTTP Error", "exception message": str(e)}

            return {"success": "Object dropped successfully", "response": response}

        elif add_mapping_match:
            object_type = add_mapping_match.group('object_type')
            platform = add_mapping_match.group('platform')
            syntax = add_mapping_match.group('syntax').strip() # remove outer "" not required in the pydantic model
            properties = add_mapping_match.group('props')

            # Remove outer quotes if present - this is a workaround for the fact that the regex captures the outer quotes (or everyything inside curly braces)
            
            if syntax.startswith('"') and syntax.endswith('"'):
                syntax = syntax[1:-1]
            try:
                # Props is expected to be a JSON-encoded map of maps (e.g., "args": {"propType": "map", ...})
                object_dump_map = json.loads(properties)

                # Build the Pydantic model - validate
                mapping_request = CreatePlatformMappingRequest(
                    platformMapping=PlatformMapping(
                        typeName=object_type,
                        platformName=platform,
                        syntax=syntax.strip(),  # clean up leading/trailing whitespace/newlines
                        objectDumpMap=object_dump_map
                    )
                )
            
                response = self.client.post(
                    f"/objects/{object_type}/platforms/{platform}",
                    mapping_request.model_dump()
                )
            except json.JSONDecodeError as e:
                return {"error": "Invalid JSON syntax in PROPS", "details": str(e)}
            except ValidationError as e:
                return {"error": "Error validating request model (pydantic)", "exception message": str(e)}
            except requests.exceptions.HTTPError as e:
                return {"error": "HTTP Error", "exception message": str(e)}

            return {"success": "Mapping added successfully", "response": response}

        # Fallback to Spark parser
        return self.sparkSession.sql(sqlText)

    # Helper method to extract SQL statements from Polaris response and execute
    def dump_handler(self, json_dump: dict):
        """
        Extracts SQL statements from the Polaris response and executes them using Spark.

        Args:
            json_dump (dict): JSON response from Polaris containing SQL statements.

        Returns:
            list: A list of results from executing the SQL statements.
        """
        statements = json_dump.get("statements", [])  # Extract the list of SQL statements

        if not statements:
            return {"error": "No statements found in response"}

        execution_results = []

        for statement in statements:
            sql_text = statement.get("definition")  # Extract the SQL string
            if sql_text:
                try:
                    result = self.sparkSession.sql(sql_text)  # Execute in Spark
                    execution_results.append({"sql": sql_text, "status": "executed"}) # "result": result
                except Exception as e:
                    execution_results.append({"sql": sql_text, "status": "failed", "error": str(e)})

        return {"success": True, "executions": execution_results}

    def validate_data_type(self, props: dict[str, str]):
        """
        Validate the data type against a predefined set of valid types.

        Args:
            proerties (dict): The properties dictionary to validate.

        Returns:
            dict: A dictionary with the validation result.
        """
        # The same set of valid data types as in the OpenDic API - UserDefinedEntitySchema
        valid_data_types = {"string", "number", "boolean", "float", "date", "array", "list", "map", "object", "variant", "int", "double"}

        for key, value in props.items():
            if value.lower() not in valid_data_types:
                raise ValueError(f"Invalid data type '{value}' for key '{key}'")


        return {"success": "Data types validated successfully"}
