import pytest
from unittest.mock import MagicMock, patch
from pyspark_opendic.catalog import OpenDicCatalog
import json


MOCK_API_URL = "https://mock-api-url.com"


@pytest.fixture
def mock_spark():
    """Creates a mock SparkSession."""
    return MagicMock()

@pytest.fixture
def catalog(mock_spark):
    """Creates an instance of PolarisXCatalog with mock Spark."""
    return OpenDicCatalog(mock_spark, MOCK_API_URL)


def test_create_with_props(catalog):
    # Test the CREATE SQL with PROPS JSON

    query = """
    CREATE OPEN function my_function 
    props {
        "args": {
            "arg1": "string",
            "arg2": "number"
        },
        "language": "sql"
    }
    """

    expected_payload = {
        "type": "function",
        "name": "my_function",
        "alias": None,
        "props": {
            "args": {
                "arg1": "string",
                "arg2": "number"
            },
            "language": "sql"
        }
    }

    # Call the sql method on the catalog with the query
    payload = catalog.sql(query)

    # Parse the payload to ensure it matches
    assert json.loads(payload) == expected_payload

def test_create_without_props(catalog):
    # Test CREATE without PROPS JSON

    query = """
    CREATE OPEN function my_table_func
    """

    expected_payload = {
        "type": "function",
        "name": "my_table_func",
        "alias": None,
        "props": None
    }

    # Call the sql method on the catalog with the query
    payload = catalog.sql(query)

    # Parse the payload to ensure it matches
    assert json.loads(payload) == expected_payload

def test_create_with_alias(catalog):
    # Test CREATE with an alias

    query = """
    CREATE OPEN function my_function AS my_alias
    """

    expected_payload = {
        "type": "function",
        "name": "my_function",
        "alias": "my_alias",
        "props": None
    }

    # Call the sql method on the catalog with the query
    payload = catalog.sql(query)

    # Parse the payload to ensure it matches
    assert json.loads(payload) == expected_payload

def test_invalid_json_in_props(catalog):
    # Test invalid JSON in PROPS (missing closing brace)

    query = """
    CREATE OPEN function my_function 
    PROPS {
        "args": {
            "arg1": "string",
            "arg2": "number"
        },
        "language": "sql"
    """

    # Call the sql method on the catalog with the query
    response = catalog.sql(query)

    # Ensure the response contains the correct error message
    assert "error" in response
    assert response["error"] == "Invalid JSON syntax in properties"


   