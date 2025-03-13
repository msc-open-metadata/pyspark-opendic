import pytest
from unittest.mock import MagicMock, patch
from pyspark_opendic.catalog import OpenDicCatalog
import json
from pyspark_opendic.model.openapi_models import CreateUdoRequest

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
    expected_payload = CreateUdoRequest(object={"type": "function", "name": "my_function", "props": dict_props}).model_dump_json()

    response = catalog.sql(query)

    mock_post.assert_called_once_with("/objects/function", expected_payload)
    assert response == {"success": "Object created successfully", "response": {"success": True}}

@patch('pyspark_opendic.client.OpenDicClient.post')
def test_create_without_props(mock_post, catalog):
    mock_post.return_value = {"success": True}

    query = """
    CREATE OPEN function my_table_func
    """

    expected_payload = CreateUdoRequest(object={"type": "function", "name": "my_table_func", "props": None}).model_dump_json()

    response = catalog.sql(query)

    mock_post.assert_called_once_with("/objects/function", expected_payload)
    assert response == {"success": "Object created successfully", "response": {"success": True}}

@patch('pyspark_opendic.client.OpenDicClient.post')
def test_create_with_alias(mock_post, catalog):
    mock_post.return_value = {"success": True}

    query = """
    CREATE OPEN function my_function AS my_alias
    """

    expected_payload = CreateUdoRequest(object={"type": "function", "name": "my_function", "props": None}).model_dump_json()

    response = catalog.sql(query)

    mock_post.assert_called_once_with("/objects/function", expected_payload)
    assert response == {"success": "Object created successfully", "response": {"success": True}}

@patch('pyspark_opendic.client.OpenDicClient.post')
def test_invalid_json_in_props(mock_post, catalog):
    query = """
    CREATE OPEN function my_function 
    PROPS {
        "args": {
            "arg1": "string",
            "arg2": "number"
        },
        "language": "sql"
    """

    response = catalog.sql(query)

    assert "error" in response
    assert response["error"] == "Invalid JSON syntax in properties"