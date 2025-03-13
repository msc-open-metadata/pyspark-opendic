import pytest
from unittest.mock import MagicMock, patch
from pyspark_opendic.catalog import OpenDicCatalog, OpenDicClient
import json

from pyspark_opendic.model.create_object_request import CreateObjectRequest


MOCK_API_URL = "https://mock-api-url.com"


@pytest.fixture
def mock_spark():
    """Creates a mock SparkSession."""
    return MagicMock()

@pytest.fixture
@patch('pyspark_opendic.client.OpenDicClient.get_polaris_oauth_token', return_value="mocked_token")
def catalog(mock_get_token, mock_spark):
    """Creates an instance of OpenDicCatalog with mock Spark and mock credentials."""
    mock_spark.conf.get.return_value = "mock_client_id:mock_client_secret"
    return OpenDicCatalog(mock_spark, MOCK_API_URL)


@patch('pyspark_opendic.client.OpenDicClient.post')
def test_create_with_props(mock_post, catalog):
    # Test the CREATE SQL with PROPS JSON

    mock_post.return_value = {"success": True}

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


    dict_props = {"args": {"arg1": "string", "arg2": "number"}, "language": "sql"}
    expected_payload = CreateObjectRequest("function", "my_function", None, dict_props).to_json()

    # Call the sql method on the catalog with the query
    response = catalog.sql(query)

    # Parse the payload to ensure it matches
    mock_post.assert_called_once_with("/function", expected_payload)
    
    assert response == {"success": "Object created successfully", "response": {"success": True}}
    #assert response == {"success": True}

@patch('pyspark_opendic.client.OpenDicClient.post')
def test_create_without_props(mock_post, catalog):
    # Test CREATE without PROPS JSON

    mock_post.return_value = {"success": True}

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
    response = catalog.sql(query)

    mock_post.assert_called_once_with("/function", json.dumps(expected_payload))
    assert response == {"success": "Object created successfully", "response": {"success": True}}

@patch('pyspark_opendic.client.OpenDicClient.post')
def test_create_with_alias(mock_post, catalog):
    # Test CREATE with an alias

    mock_post.return_value = {"success": True}

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
    response = catalog.sql(query)

    mock_post.assert_called_once_with("/function", json.dumps(expected_payload))
    assert response == {"success": "Object created successfully", "response": {"success": True}}

@patch('pyspark_opendic.client.OpenDicClient.post')
def test_invalid_json_in_props(mock_post, catalog):
    # Test invalid JSON in PROPS (missing closing brace)

    mock_post.return_value = {"success": True}

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