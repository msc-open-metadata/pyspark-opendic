import json
import re
from typing import Any

import pandas as pd
import requests
from pydantic import ValidationError
from pyspark.sql import SparkSession
from pyspark.sql.catalog import Catalog

from pyspark_opendic.client import OpenDicClient
from pyspark_opendic.model.openapi_models import (
    CreatePlatformMappingRequest,
    CreateUdoRequest,
    DefineUdoRequest,
    PlatformMapping,
    Statement,
    Udo,
)
from pyspark_opendic.prettyResponse import PrettyResponse


class OpenDicCatalog(Catalog):
    def __init__(self, sparkSession: SparkSession, api_url: str):
        self.sparkSession = sparkSession

        credentials = sparkSession.conf.get("spark.sql.catalog.polaris.credential")
        if credentials is None:
            raise ValueError("spark.sql.catalog.polaris.credential is not set")
        self.client = OpenDicClient(api_url, credentials)

    def sql(self, sqlText: str):
        query_cleaned = sqlText.strip()

        # TODO: do some systematic syntax union - include alias 'as', etc.
        # TODO: add support for 'or replace' and 'temporary' keywords etc. on catalog-side - not a priority for now, so just ignore
        # TODO: patterns are constants as of now. Should not be defined inside the function itself. They should be moved outside or somewhere else.
        # Syntax: CREATE [OR REPLACE] [TEMPORARY] OPEN <object_type> <name> [IF NOT EXISTS] [AS <alias>] [PROPS { <properties> }]
        opendic_create_pattern: re.Pattern = re.compile(
            r"^create"  # "create" at the start
            r"(?:\s+or\s+replace)?"  # Optional "or replace"
            r"(?:\s+temporary)?"  # Optional "temporary"
            r"\s+open\s+(?P<object_type>\w+)"  # Required object type after "open"
            r"\s+(?P<name>\w+)"  # Required name of the object
            r"(?:\s+if\s+not\s+exists)?"  # Optional "if not exists"
            r"(?:\s+as\s+(?P<alias>\w+))?"  # Optional alias after "as"
            r"(?:\s+props\s*(?P<properties>\{[\s\S]*\}))?",  # Optional "props" keyword, but curly braces are mandatory if present - This is a JSON object
            re.IGNORECASE,
        )

        # Syntax: SHOW OPEN TYPES
        # Example: SHOW OPEN TYPES
        opendic_show_types_pattern: re.Pattern = re.compile(
            r"^show"  # "show" at the start
            r"\s+open\s+types$",  # Required "open types"
            re.IGNORECASE,
        )

        # Syntax: SHOW OPEN <object_type>[s]
        # Example: SHOW OPEN functions
        opendic_show_pattern: re.Pattern = re.compile(
            r"^show"  # "show" at the start
            r"\s+open\s+(?P<object_type>(?!types$)\w+)"  # Required object type after "open" and not "TYPES"
            r"s?$",  # Optionally match a trailing "s"
            re.IGNORECASE,
        )

        # Syntax: SYNC OPEN <object_type>[s]
        # Example: SYNC OPEN functions
        opendic_sync_pattern: re.Pattern = re.compile(
            r"^sync"  # "sync" at the start
            r"\s+open\s+(?P<object_type>\w+)\s+for"  # "open platforms for"
            r"\s+(?P<platform>\w+)"  # Object type (e.g., function)
            r"$",  # End of string
            re.IGNORECASE,
        )

        # Syntax: DEFINE OPEN <udoType> PROPS { <properties> }
        # Example: sql = 'DEFINE OPEN function PROPS { "language": "string", "version": "string", "def":"string"}'
        # TODO: can we somehow add validation for wheter the props are defined with data types? as above, "language": "string".. can we validate that string is a data type etc.?
        opendic_define_pattern: re.Pattern = re.compile(
            r"^define"  # "DEFINE" at the start
            r"\s+open\s+(?P<udoType>\w+)"  # Required UDO type (e.g., "function")
            r"(?:\s+props\s*(?P<properties>\{[\s\S]*\}))?"  # REQUIRED PROPS with JSON inside {}
            r"$",
            re.IGNORECASE,
        )

        # Syntax: DROP OPEN <object_type>
        # Example: DROP OPEN function
        opendic_drop_pattern: re.Pattern = re.compile(
            r"^drop"  # "DROP" at the start
            r"\s+open\s+(?P<object_type>\w+)",  # Required object type after "open"
            re.IGNORECASE,
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
        opendic_add_mapping_pattern: re.Pattern = re.compile(
            r"^add"
            r"\s+open\s+mapping"
            r"\s+(?P<object_type>\w+)"
            r"\s+platform\s+(?P<platform>\w+)"
            r"\s+syntax\s*\{\s*(?P<syntax>[\s\S]*?)\s*\}"
            r"\s+props\s*(?P<props>\{[\s\S]*?\})"
            r"$",
            re.IGNORECASE | re.DOTALL,
        )

        # Syntax: SHOW OPEN MAPPING <object_type> PLATFORM <platform>
        # Example: SHOW OPEN MAPPING function PLATFORM snowflake
        opendic_show_mapping_for_object_and_platform_pattern: re.Pattern = re.compile(
            r"^show"  # "show" at the start
            r"\s+open\s+mapping"  # "open mapping"
            r"\s+(?P<object_type>\w+)"  # Object type (e.g., function) - some defined UDO with a mapping TODO: we should have a check on Polaris side if a type and mapping exists.
            r"\s+platform\s+(?P<platform>\w+)"  # Platform name (e.g., snowflake)
            r"$",  # End of string
            re.IGNORECASE,
        )

        # Syntax: SHOW OPEN PLATFORMS FOR <object_type>
        # Example: SHOW OPEN PLATFORMS FOR function
        opendic_show_platforms_for_object_pattern: re.Pattern = re.compile(
            r"^show"  # "show" at the start
            r"\s+open\s+platforms\s+for"  # "open platforms for"
            r"\s+(?P<object_type>\w+)"  # Object type (e.g., function)
            r"$",  # End of string
            re.IGNORECASE,
        )

        # Syntax: SHOW OPEN PLATFORMS
        # Example: SHOW OPEN PLATFORMS
        opendic_show_platforms_all_pattern: re.Pattern = re.compile(
            r"^show"  # "show" at the start
            r"\s+open\s+platforms$"  # Required "open platforms"
            r"$",  # End of string
            re.IGNORECASE,
        )

        # Syntax: SHOW OPEN MAPPING[S] FOR <platform>
        # Example: SHOW OPEN MAPPING FOR snowflake or SHOW OPEN MAPPINGS FOR snowflake
        opendic_show_mappings_for_platform_pattern: re.Pattern = re.compile(
            r"^show"  # "show" at the start
            r"\s+open\s+mappings?\s+for\s+(?P<platform>\w+)"  # "open mappings" (with optional 's') followed by 'for' and platform name
            r"$",  # End of string
            re.IGNORECASE,
        )

        # Syntax: DROP OPEN MAPPING[S] FOR <platform>
        # Example: DROP OPEN MAPPING FOR snowflake or DROP OPEN MAPPINGS FOR snowflake
        opendic_drop_mapping_for_platform_pattern: re.Pattern = re.compile(
            r"^drop"  # "drop" at the start
            r"\s+open\s+mappings?\s+"  # "open mapping" with optional "s" for plural
            r"for\s+(?P<platform>\w+)"  # "for" followed by platform name
            r"$",  # End of string
            re.IGNORECASE,
        )

        # Check pattern matches
        create_match = re.match(opendic_create_pattern, query_cleaned)
        show_types_match = re.match(opendic_show_types_pattern, query_cleaned)
        show_all_platforms_match = re.match(opendic_show_platforms_all_pattern, query_cleaned)
        show_mappings_for_platform_match = re.match(opendic_show_mappings_for_platform_pattern, query_cleaned)
        show_match = re.match(opendic_show_pattern, query_cleaned)
        sync_match = re.match(opendic_sync_pattern, query_cleaned)
        define_match = re.match(opendic_define_pattern, query_cleaned)
        drop_platform_match = re.match(opendic_drop_mapping_for_platform_pattern, query_cleaned)
        drop_match = re.match(opendic_drop_pattern, query_cleaned)
        show_mapping_for_object_and_platform_match = re.match(opendic_show_mapping_for_object_and_platform_pattern, query_cleaned)
        show_platforms_for_object_match = re.match(opendic_show_platforms_for_object_pattern, query_cleaned)
        add_mapping_match = re.match(opendic_add_mapping_pattern, query_cleaned)

        if create_match:
            object_type = create_match.group("object_type")
            name = create_match.group("name")
            alias = create_match.group("alias")
            properties = create_match.group("properties")

            # Parse props as JSON - this serves as a basic syntax check on the JSON input and default to None so we can catch Pydantic Error
            try:
                create_props: dict[str, str] = json.loads(properties) if properties else None

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
                return self.pretty_print_result(
                    {"error": "HTTP Error", "exception message": str(e), "Catalog Response": e.response.json()}
                )
            except ValidationError as e:
                return self.pretty_print_result({"error": "Error creating object", "exception message": str(e)})
            except json.JSONDecodeError as e:
                return self.pretty_print_result(
                    {"error": "Invalid JSON syntax in properties", "details": {"sql": sqlText, "exception_message": str(e)}}
                )

            return self.pretty_print_result({"success": "Object created successfully", "response": response})
        # , "sync_response": dump_handler_response}

        elif show_types_match:
            try:
                response = self.client.get("/objects")
            except requests.exceptions.HTTPError as e:
                return self.pretty_print_result(
                    {"error": "HTTP Error", "exception message": str(e), "Catalog Response": e.response.json()}
                )

            return self.pretty_print_result({"success": "Object types retrieved successfully", "response": response})

        elif show_all_platforms_match:
            try:
                response = self.client.get("/platforms")
            except requests.exceptions.HTTPError as e:
                return self.pretty_print_result(
                    {"error": "HTTP Error", "exception message": str(e), "Catalog Response": e.response.json()}
                )

            return self.pretty_print_result({"success": "Platforms retrieved successfully", "response": response})

        elif show_mappings_for_platform_match:
            platform = show_mappings_for_platform_match.group("platform")
            try:
                response = self.client.get(f"/platforms/{platform}")
            except requests.exceptions.HTTPError as e:
                return self.pretty_print_result(
                    {"error": "HTTP Error", "exception message": str(e), "Catalog Response": e.response.json()}
                )

            return self.pretty_print_result({"success": "Mappings for platform retrieved successfully", "response": response})

        elif drop_platform_match:
            platform = drop_platform_match.group("platform")

            try:
                response = self.client.delete(f"/platforms/{platform}")
            except requests.exceptions.HTTPError as e:
                return self.pretty_print_result(
                    {"error": "HTTP Error", "exception message": str(e), "Catalog Response": e.response.json()}
                )

            return self.pretty_print_result({"success": "Platform's mappings dropped successfully", "response": response})

        elif show_match:
            object_type = show_match.group("object_type")
            try:
                response = self.client.get(f"/objects/{object_type}")
            except requests.exceptions.HTTPError as e:
                return self.pretty_print_result(
                    {"error": "HTTP Error", "exception message": str(e), "Catalog Response": e.response.json()}
                )

            return self.pretty_print_result({"success": "Objects retrieved successfully", "response": response})

        elif show_mapping_for_object_and_platform_match:
            object_type = show_mapping_for_object_and_platform_match.group("object_type")
            platform = show_mapping_for_object_and_platform_match.group("platform")
            try:
                response = self.client.get(f"/objects/{object_type}/platforms/{platform}")
            except requests.exceptions.HTTPError as e:
                return self.pretty_print_result(
                    {"error": "HTTP Error", "exception message": str(e), "Catalog Response": e.response.json()}
                )

            return self.pretty_print_result({"success": "Mapping retrieved successfully", "response": response})

        elif show_platforms_for_object_match:
            object_type = show_platforms_for_object_match.group("object_type")
            try:
                response = self.client.get(f"/objects/{object_type}/platforms")
            except requests.exceptions.HTTPError as e:
                return self.pretty_print_result(
                    {"error": "HTTP Error", "exception message": str(e), "Catalog Response": e.response.json()}
                )

            return self.pretty_print_result({"success": "Platforms retrieved successfully", "response": response})

        elif sync_match:
            object_type = sync_match.group("object_type")
            platform: str = sync_match.group("platform").lower()
            # TODO: should we force the platform to be lowercase? or should we keep it as is?

            try:
                response = self.client.get(f"/objects/{object_type}/platforms/{platform}/pull")
                statements = [Statement.model_validate(item) for item in response]
            except requests.exceptions.HTTPError as e:
                return self.pretty_print_result(
                    {"error": "HTTP Error", "exception message": str(e), "Catalog Response": e.response.json()}
                )
            except ValidationError as e:
                return self.pretty_print_result(
                    {"error": "Error validating request model (pydantic)", "exception message": str(e)}
                )

            return self.dump_handler(statements)

        elif define_match:
            udoType = define_match.group("udoType")
            properties = define_match.group("properties")
            try:
                # Parse props as JSON - this serves as a basic syntax check on the JSON input.
                define_props: dict[str, str] = json.loads(properties) if properties else None
                # Build Udo and CreateUdoRequest models
                define_request = DefineUdoRequest(udoType=udoType, properties=define_props)
                # This is a basic check, but we should probably add a more advanced one later on
                self.validate_data_type(define_props)
                # Serialize to JSON
                payload = define_request.model_dump()
                # Send Request
                response = self.client.post("/objects", payload)

                return self.pretty_print_result({"success": "Object defined successfully", "response": response})
            except json.JSONDecodeError as e:
                return self.pretty_print_result(
                    {"error": "Invalid JSON syntax in properties", "details": {"sql": sqlText, "exception_message": str(e)}}
                )
            except requests.exceptions.HTTPError as e:
                return self.pretty_print_result(
                    {"error": "HTTP Error", "exception message": str(e), "Catalog Response": e.response.json()}
                )
            except ValidationError as e:
                return self.pretty_print_result({"error": "Error defining object", "exception message": str(e)})
            except ValueError as e:
                return self.pretty_print_result({"error": "Invalid type for DEFINE statement", "exception message": str(e)})

        # Not sure if we should support dropping a specific object tuple, and not the whole table?
        elif drop_match:
            object_type = drop_match.group("object_type")

            try:
                response = self.client.delete(f"/objects/{object_type}")
            except requests.exceptions.HTTPError as e:
                return self.pretty_print_result(
                    {"error": "HTTP Error", "exception message": str(e), "Catalog Response": e.response.json()}
                )

            return self.pretty_print_result({"success": "Object dropped successfully", "response": response})

        elif add_mapping_match:
            object_type = add_mapping_match.group("object_type")
            platform = add_mapping_match.group("platform")
            syntax = add_mapping_match.group("syntax").strip()  # remove outer "" not required in the pydantic model
            properties = add_mapping_match.group("props")

            # Remove outer quotes if present - this is a workaround for the fact that the regex captures the outer quotes (or everyything inside curly braces)

            if syntax.startswith('"') and syntax.endswith('"'):
                syntax = syntax[1:-1]
            try:
                # Props is expected to be a JSON-encoded dict of dicts (e.g., "args": {"propType": "map", ...})
                object_dump_map: dict[str, dict[str, Any]] = json.loads(properties)

                # Build the Pydantic model - validate
                mapping_request = CreatePlatformMappingRequest(
                    platformMapping=PlatformMapping(
                        typeName=object_type,
                        platformName=platform,
                        syntax=syntax.strip(),  # clean up leading/trailing whitespace/newlines
                        objectDumpMap=object_dump_map,
                    )
                )

                response = self.client.post(f"/objects/{object_type}/platforms/{platform}", mapping_request.model_dump())
            except json.JSONDecodeError as e:
                return self.pretty_print_result({"error": "Invalid JSON syntax in PROPS", "details": str(e)})
            except ValidationError as e:
                return self.pretty_print_result(
                    {"error": "Error validating request model (pydantic)", "exception message": str(e)}
                )
            except requests.exceptions.HTTPError as e:
                return self.pretty_print_result(
                    {"error": "HTTP Error", "exception message": str(e), "Catalog Response": e.response.json()}
                )

            return self.pretty_print_result({"success": "Mapping added successfully", "response": response})

        # Fallback to Spark parser
        return self.sparkSession.sql(sqlText)

    # Helper method to extract SQL statements from Polaris response and execute
    def dump_handler(self, response: list[Statement]):
        """
        Extracts SQL statements from the Polaris response and executes them using Spark.

        Args:
            response (list): List of Statement objects.

        Returns:
            dict: Execution result with status.
        """
        if not response:
            return self.pretty_print_result({"error": "No statements found in response"})

        execution_results = []
        for statement in response:
            sql_text = statement.definition.strip()  # Extract SQL statement from the response
            if sql_text:
                try:
                    result = self.sparkSession.sql(sql_text)  # Execute in Spark
                    execution_results.append({"sql": sql_text, "status": "executed"})
                except Exception as e:
                    execution_results.append({"sql": sql_text, "status": "failed", "error": str(e)})

        return self.pretty_print_result({"success": True, "executions": execution_results})

    def validate_data_type(self, props: dict[str, str]) -> dict[str, str]:
        """
        Validate the data type against a predefined set of valid types.

        Args:
            proerties (dict): The properties dictionary to validate.

        Returns:
            dict: A dictionary with the validation result.
        """
        # The same set of valid data types as in the OpenDic API - UserDefinedEntitySchema (+ int and double)
        valid_data_types = {
            "string",
            "number",
            "boolean",
            "float",
            "date",
            "array",
            "list",
            "map",
            "object",
            "variant",
            "int",
            "double",
        }

        for key, value in props.items():
            if value.lower() not in valid_data_types:
                raise ValueError(f"Invalid data type '{value}' for key '{key}'")

        return {"success": "Data types validated successfully"}

    def pretty_print_result(self, result: dict):
        """
        Pretty print the result in a readable format.
        """
        pd.set_option("display.width", None)  # Auto-detect terminal width
        pd.set_option("display.max_colwidth", None)  # Show full content of each cell
        pd.set_option("display.max_rows", None)  # Show all rows
        pd.set_option("display.expand_frame_repr", False)  # Don't wrap to multiple lines

        response = result.get("response")

        # Polaris-spec-compliant "good" responses, so objects or lists of objects
        if isinstance(response, list) and all(isinstance(item, dict) for item in response):
            return pd.DataFrame(response)

        elif isinstance(response, dict):
            return pd.DataFrame([response])

        # Everything else — errors, messages, etc.
        return PrettyResponse(result)
