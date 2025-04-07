from unittest.mock import MagicMock, patch

import pytest

from pyspark_opendic.catalog import OpenDicCatalog
from pyspark_opendic.model.openapi_models import CreatePlatformMappingRequest, CreateUdoRequest, DefineUdoRequest, PlatformMapping, PlatformMappingObjectDumpMapValue, Udo

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

# ---- Tests for CREATE ----

@patch('pyspark_opendic.client.OpenDicClient.post')
@patch('pyspark_opendic.client.OpenDicClient.get')
def test_create_with_props(mock_get, mock_post, catalog):
    mock_post.return_value = {"success": True}
    mock_get.return_value = {"success": True, "objects": [{"type": "function", "name": "my_function", "language": "sql", "args": {"arg1": "string", "arg2": "number"}, "definition": "SELECT * FROM my_table"}]}

    query = """
    CREATE OPEN function my_function
    props {
        "args": {
            "arg1": "string",
            "arg2": "number"
        },
        "language": "sql",
        "definition": "SELECT * FROM my_table"
    }
    """

    dict_props = {"args": {"arg1": "string", "arg2": "number"}, "language": "sql", "definition": "SELECT * FROM my_table"}
    udo_object = Udo(type = "function", name = "my_function", props = dict_props)
    expected_payload = CreateUdoRequest(udo = udo_object).model_dump()

    response = catalog.sql(query)

    mock_get.assert_called_once_with("/objects/function/sync")
    mock_post.assert_called_once_with("/objects/function", expected_payload)
    #assert response == {"success": "Object created successfully", "sync response": {"success": True, "objects": [{"type": "function", "name": "my_function", "language": "sql", "args": {"arg1": "string", "arg2": "number"}, "definition": "SELECT * FROM my_table"}]}}

@patch('pyspark_opendic.client.OpenDicClient.post')
@patch('pyspark_opendic.client.OpenDicClient.get')
def test_create_without_props(mock_get, mock_post, catalog):
    mock_post.return_value = {"success": True}
    #Empty args and definition
    mock_get.return_value = {"success": True, "statements": [{"definition": "CREATE OR REPLACE FUNCTION my_table_func"}]}

    query = """
    CREATE OPEN function my_table_func
    """

    udo_object = Udo(type = "function", name = "my_table_func")
    expected_payload = CreateUdoRequest(udo = udo_object).model_dump()

    response = catalog.sql(query)

    mock_get.assert_called_once_with("/objects/function/sync")
    mock_post.assert_called_once_with("/objects/function", expected_payload)
    assert response == {'success': 'Object created successfully', 'response': {'success': True}, 'sync_response': {'success': True, 'executions': [{'sql': 'CREATE OR REPLACE FUNCTION my_table_func', 'status': 'executed'}]}}

@patch('pyspark_opendic.client.OpenDicClient.post')
@patch('pyspark_opendic.client.OpenDicClient.get')
def test_create_with_alias(mock_get, mock_post, catalog):
    mock_post.return_value = {"success": True}
    mock_get.return_value = {"success": True, "statements": [{"definition": "CREATE OR REPLACE FUNCTION my_function as my_alias"}]}


    query = """
    CREATE OPEN function my_function AS my_alias
    """

    udo_object = Udo(type = "function", name = "my_function", alias = "my_alias")
    expected_payload = CreateUdoRequest(udo = udo_object).model_dump()

    response = catalog.sql(query)

    mock_get.assert_called_once_with("/objects/function/sync")
    mock_post.assert_called_once_with("/objects/function", expected_payload)
    assert response == {'success': 'Object created successfully', 'response': {'success': True}, 'sync_response': {'success': True, 'executions': [{'sql': 'CREATE OR REPLACE FUNCTION my_function as my_alias', 'status': 'executed'}]}}


# ---- Tests for Pydantic INVALID JSON ----
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


# ---- Tests for SHOW ----
@patch('pyspark_opendic.client.OpenDicClient.get')
def test_show(mock_get, catalog):
    mock_get.return_value = {"success": True,
                                 "objects": [{"type": "function", "name": "my_function", "language": "sql", "args": {"arg1": "string", "arg2": "number"}, "definition": "SELECT * FROM my_table"}]}

    query = "SHOW OPEN function"

    response = catalog.sql(query)

    mock_get.assert_called_once_with("/objects/function")
    assert response == {'success': 'Objects retrieved successfully', 'response': {'success': True, 'objects': [{'type': 'function', 'name': 'my_function', 'language': 'sql', 'args': {'arg1': 'string', 'arg2': 'number'}, 'definition': 'SELECT * FROM my_table'}]}}


# ---- Tests for SYNC ----
@patch('pyspark_opendic.client.OpenDicClient.get')
def test_sync_function(mock_get, catalog):
    mock_get.return_value = {"success":
                             True, "statements": [{"definition": "CREATE OR REPLACE FUNCTION my_function AS 'SELECT 1';"}]}

    query = "SYNC OPEN function"

    response = catalog.sql(query)

    mock_get.assert_called_once_with("/objects/function/sync")
    #mock_spark.sql.assert_called_once_with("CREATE OR REPLACE FUNCTION my_function AS 'SELECT 1';")
    assert response == {'success': True, 'executions': [{'sql': "CREATE OR REPLACE FUNCTION my_function AS 'SELECT 1';", 'status': 'executed'}]}


# ---- Tests for DEFINE ----
@patch('pyspark_opendic.client.OpenDicClient.post')
def test_define(mock_post, catalog):
    mock_post.return_value = {"success": True}

    query = """
    DEFINE OPEN function PROPS { "language": "string", "version": "string", "def":"string"}
    """

    expected_payload = DefineUdoRequest(udoType = "function", properties = {"language": "string", "version": "string", "def":"string"}).model_dump()

    print("EXPECTED PAYLOAD:", expected_payload)
    response = catalog.sql(query)

    mock_post.assert_called_once_with("/objects", expected_payload)
    assert response == {'success': 'Object defined successfully', 'response': {'success': True}}

@patch('pyspark_opendic.client.OpenDicClient.post')
def test_define_invalid_json(mock_post, catalog):
    query = """
    DEFINE OPEN function PROPS {"language" "string", "version": "string, "def":"string"}
    """

    response = catalog.sql(query)

    assert "error" in response
    assert response["error"] == "Invalid JSON syntax in properties"

@patch('pyspark_opendic.client.OpenDicClient.post')
def test_define_pydantic_error(mock_post, catalog):
    query = """
    DEFINE OPEN function
    """

    response = catalog.sql(query)

    #assert response["error"] == "Error defining object"
    #assert "validation error" in response["exception message"]
    assert response["error"] != ""

@patch('pyspark_opendic.client.OpenDicClient.post')
def test_define_invalid_type(mock_post, catalog):
    query = """
    DEFINE OPEN table PROPS { "language": "string", "version": "hashmap", "def":"string"}
    """

    response = catalog.sql(query)

    assert response["error"] == "Invalid type for DEFINE statement"
    assert "Invalid data type 'hashmap' for key 'version'" in response["exception message"]


# ---- Tests for DROP ----
@patch('pyspark_opendic.client.OpenDicClient.delete')
def test_drop_function(mock_delete, catalog):
    mock_delete.return_value = {"success": True}

    query = "DROP OPEN function"

    response = catalog.sql(query)

    mock_delete.assert_called_once_with("/objects/function")
    assert response == {'success': 'Object dropped successfully', 'response': {'success': True}}

# ---- Tests for Show types ----
@patch('pyspark_opendic.client.OpenDicClient.get')
def test_show_types(mock_get, catalog):
    mock_get.return_value = {"success": True,
                                 "objects": [{"type": "function", "schema": "{schema}"}]}

    query = "SHOW OPEN TYPES"

    response = catalog.sql(query)

    mock_get.assert_called_once_with("/objects")
    assert response == {'success': 'Object types retrieved successfully', 'response': {'success': True, 'objects': [{"type": "function", "schema": "{schema}"}]}}


# ---- Tests for ADD OPEN MAPPING ----
@patch('pyspark_opendic.client.OpenDicClient.post')
def test_add_open_mapping_multiline(mock_post, catalog):
    """Test multiline ADD OPEN MAPPING query with full JSON dump map."""
    mock_post.return_value = {"success": True}

    query = """
    ADD OPEN MAPPING function PLATFORM spark
    SYNTAX {
        "CREATE FUNCTION {name} ({params}) RETURNS STRING AS $$ {def} $$"
    }
    PROPS {
        "params": {
            "propType": "list",
            "format": "<item>",
            "delimiter": ", "
        },
        "def": {
            "propType": "string",
            "format": "<value>",
            "delimiter": ""
        }
    }
    """

    # expected request model
    expected_payload = CreatePlatformMappingRequest(
        platformMapping= PlatformMapping(
            typeName="function",
            platformName="spark",
            syntax="CREATE FUNCTION {name} ({params}) RETURNS STRING AS $$ {def} $$",
            objectDumpMap={
                "params": PlatformMappingObjectDumpMapValue(
                    propType="list",
                    format="<item>",
                    delimiter=", "
                ),
                "def": PlatformMappingObjectDumpMapValue(
                    propType="string",
                    format="<value>",
                    delimiter=""
                )
            }
        )
    ).model_dump()

    response = catalog.sql(query)

    mock_post.assert_called_once_with("/objects/function/platforms/spark", expected_payload)
    assert response == {'success': 'Mapping added successfully', 'response': {'success': True}}
