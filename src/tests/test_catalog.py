from unittest.mock import MagicMock, patch

import pytest

from pyspark_opendic.catalog import OpenDicCatalog
from pyspark_opendic.model.openapi_models import CreatePlatformMappingRequest, CreateUdoRequest, DefineUdoRequest, PlatformMapping, PlatformMappingObjectDumpMapValue, Statement, Udo
from pyspark_opendic.prettyResponse import PrettyResponse
import pandas as pd

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

    mock_post.assert_called_once_with("/objects/function", expected_payload)
    
    expected = catalog.pretty_print_result({'success': 'Object created successfully', 'response': {'success': True}})
    assert type(response) == type(expected)
    assert str(response) == str(expected)


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

    mock_post.assert_called_once_with("/objects/function", expected_payload)
    expected = {'success': 'Object created successfully', 'response': {'success': True}}
    assert_catalog_response_equal(response, expected)

@patch('pyspark_opendic.client.OpenDicClient.post')
def test_create_batch_with_props(mock_post, catalog):
    mock_post.return_value = {"success": True}

    query = """
    CREATE OPEN BATCH function
    OBJECTS [
        { "name": "my_func1", "language": "sql", "args": { "arg1": "string" }, "definition": "SELECT 1" },
        { "name": "my_func2", "language": "sql", "args": { "arg2": "int" }, "definition": "SELECT 2" }
    ]
    """

    expected_payload = [
        Udo(
            type="function",
            name="my_func1",
            props={"language": "sql", "args": {"arg1": "string"}, "definition": "SELECT 1"}
        ).model_dump(),
        Udo(
            type="function",
            name="my_func2",
            props={"language": "sql", "args": {"arg2": "int"}, "definition": "SELECT 2"}
        ).model_dump()
    ]

    response = catalog.sql(query)

    mock_post.assert_called_once_with("/objects/function/batch", expected_payload)
    expected = {'success': 'Batch created', 'response': {'success': True}}
    assert_catalog_response_equal(response, expected)



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

    assert isinstance(response, PrettyResponse)
    assert "error" in str(response)
    assert "Invalid JSON syntax in properties" in str(response)

@patch('pyspark_opendic.client.OpenDicClient.post')
def test_define_pydantic_error(mock_post, catalog):
    query = """
    DEFINE OPEN function
    """

    response = catalog.sql(query)

    assert isinstance(response, PrettyResponse)
    assert "error" in str(response)
    assert "validation error" in str(response)

# ---- Tests for SHOW ----
@patch('pyspark_opendic.client.OpenDicClient.get')
def test_show(mock_get, catalog):
    mock_get.return_value = {"success": True,
                                 "objects": [{"type": "function", "name": "my_function", "language": "sql", "args": {"arg1": "string", "arg2": "number"}, "definition": "SELECT * FROM my_table"}]}

    query = "SHOW OPEN function"

    response = catalog.sql(query)
    expected = {'success': 'Objects retrieved successfully',
                'response': {'success': True,
                              'objects': [{'type': 'function', 'name': 'my_function', 'language': 'sql', 'args': {'arg1': 'string', 'arg2': 'number'}, 'definition': 'SELECT * FROM my_table'}]
                            }
                }   
    

    mock_get.assert_called_once_with("/objects/function")
    assert_catalog_response_equal(response, expected)
    

@patch('pyspark_opendic.client.OpenDicClient.get')
def test_show_mapping_platform_type(mock_get, catalog):
    mock_get.return_value = {"success": True, 'response': {'success': True, 'objects': [{'type': 'function', 'name': 'my_function', 'language': 'sql', 'args': {'arg1': 'string', 'arg2': 'number'}, 'definition': 'SELECT * FROM my_table'}]}}

    query = "SHOW OPEN MAPPING function PLATFORM spark"

    response = catalog.sql(query)
    expected = {'success': 'Mapping retrieved successfully', 'response': {"success": True, 'response': {'success': True, 'objects': [{'type': 'function', 'name': 'my_function', 'language': 'sql', 'args': {'arg1': 'string', 'arg2': 'number'}, 'definition': 'SELECT * FROM my_table'}]}}}

    mock_get.assert_called_once_with("/objects/function/platforms/spark")
    assert_catalog_response_equal(response, expected)

@patch('pyspark_opendic.client.OpenDicClient.get')
def test_show_mapping_platform(mock_get, catalog):
    mock_get.return_value = {"success": True, 'response': {'success': True, 'objects': [{'type': 'function', 'name': 'my_function', 'language': 'sql', 'args': {'arg1': 'string', 'arg2': 'number'}, 'definition': 'SELECT * FROM my_table'}]}}

    query = "SHOW OPEN PLATFORMS FOR function"

    response = catalog.sql(query)
    expected = {'success': 'Platforms retrieved successfully', 'response': {"success": True, 'response': {'success': True, 'objects': [{'type': 'function', 'name': 'my_function', 'language': 'sql', 'args': {'arg1': 'string', 'arg2': 'number'}, 'definition': 'SELECT * FROM my_table'}]}}}
    mock_get.assert_called_once_with("/objects/function/platforms")
    assert_catalog_response_equal(response, expected)

@patch('pyspark_opendic.client.OpenDicClient.get')
def test_show_open_platforms(mock_get, catalog):
    mock_get.return_value = {"success": True, 'response': [{"platform": "spark"}, {"platform": "snowflake"}]}

    query = "SHOW OPEN PLATFORMS"

    response = catalog.sql(query)
    expected = {'success': 'Platforms retrieved successfully', 'response': {"success": True, 'response': [{"platform": "spark"}, {"platform": "snowflake"}]}}
    mock_get.assert_called_once_with("/platforms")
    assert_catalog_response_equal(response, expected)

@patch('pyspark_opendic.client.OpenDicClient.get')
def test_show_open_mappings_for_platform(mock_get, catalog):
    mock_get.return_value = {"success": True, 'response': {"mapping": "...."}}

    query = "SHOW OPEN MAPPINGS FOR spark"

    response = catalog.sql(query)
    expected = {'success': "Mappings for platform retrieved successfully", 'response': {"success": True, 'response': {"mapping": "...."}}}

    mock_get.assert_called_once_with("/platforms/spark")
    assert_catalog_response_equal(response, expected)

# ---- Tests for SYNC ----
@patch('pyspark_opendic.client.OpenDicClient.get')
def test_sync_function(mock_get, catalog):
    mock_get.return_value = [{"definition": "CREATE OR REPLACE FUNCTION my_function AS 'SELECT 1';"}]

    query = "SYNC OPEN function for Spark"

    response = catalog.sql(query)
    expected = {
        "executions": [{
            "sql": "CREATE OR REPLACE FUNCTION my_function AS 'SELECT 1';",
            "status": "executed"
        }]
    }   

    mock_get.assert_called_once_with("/objects/function/platforms/spark/pull")
    #mock_spark.sql.assert_called_once_with("CREATE OR REPLACE FUNCTION my_function AS 'SELECT 1';")
    assert_catalog_response_equal(response, expected)

@patch('pyspark_opendic.client.OpenDicClient.get')
def test_sync_all_objects_for_platform(mock_get, catalog):
    mock_get.return_value = [{"definition": "CREATE OR REPLACE FUNCTION my_function AS 'SELECT 1';"}]

    query = "SYNC OPEN OBJECTS FOR Spark"

    response = catalog.sql(query)
    expected = {
        "executions": [{
            "sql": "CREATE OR REPLACE FUNCTION my_function AS 'SELECT 1';",
            "status": "executed"
        }]
    }

    mock_get.assert_called_once_with("/platforms/spark/pull")
    assert_catalog_response_equal(response, expected)


# ---- Tests for DEFINE ----
@patch('pyspark_opendic.client.OpenDicClient.post')
def test_define(mock_post, catalog):
    mock_post.return_value = {"success": True}

    query = """
    DEFINE OPEN function PROPS { "language": "string", "version": "string", "def":"string"}
    """

    expected_payload = DefineUdoRequest(udoType = "function", properties = {"language": "string", "version": "string", "def":"string"}).model_dump()

    response = catalog.sql(query)
    expected = {'success': 'Object defined successfully', 'response': {'success': True}}

    mock_post.assert_called_once_with("/objects", expected_payload)
    assert_catalog_response_equal(response, expected)

@patch('pyspark_opendic.client.OpenDicClient.post')
def test_define_invalid_json(mock_post, catalog):
    query = """
    DEFINE OPEN function PROPS {"language" "string", "version": "string, "def":"string"}
    """

    response = catalog.sql(query)

    assert isinstance(response, PrettyResponse)
    assert "error" in str(response)
    assert "Invalid JSON syntax in properties" in str(response)


@patch('pyspark_opendic.client.OpenDicClient.post')
def test_define_invalid_type(mock_post, catalog):
    query = """
    DEFINE OPEN table PROPS { "language": "string", "version": "hashmap", "def":"string"}
    """

    response = catalog.sql(query)

    assert isinstance(response, PrettyResponse)
    assert "Invalid data type 'hashmap'" in str(response)


# ---- Tests for DROP ----
@patch('pyspark_opendic.client.OpenDicClient.delete')
def test_drop_function(mock_delete, catalog):
    mock_delete.return_value = {"success": True}

    query = "DROP OPEN function"

    response = catalog.sql(query)
    expected = {'success': 'Object dropped successfully', 'response': {'success': True}}
    
    mock_delete.assert_called_once_with("/objects/function")
    assert_catalog_response_equal(response, expected)

@patch('pyspark_opendic.client.OpenDicClient.delete')
def test_drop_mapping(mock_delete, catalog):
    mock_delete.return_value = {"success": True}

    query = "DROP OPEN MAPPING FOR spark"

    response = catalog.sql(query)
    expected = {'success': 'Platform\'s mappings dropped successfully', 'response': {'success': True}}

    mock_delete.assert_called_once_with("/platforms/spark")
    assert_catalog_response_equal(response, expected)

# ---- Tests for Show types ----
@patch('pyspark_opendic.client.OpenDicClient.get')
def test_show_types(mock_get, catalog):
    mock_get.return_value = {"success": True,
                                 "objects": [{"type": "function", "schema": "{schema}"}]}

    query = "SHOW OPEN TYPES"

    response = catalog.sql(query)
    expected = {'success': 'Object types retrieved successfully', 'response': {'success': True, 'objects': [{"type": "function", "schema": "{schema}"}]}}
    
    mock_get.assert_called_once_with("/objects")
    assert_catalog_response_equal(response, expected)


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
    expected = {'success': 'Mapping added successfully', 'response': {'success': True}}

    mock_post.assert_called_once_with("/objects/function/platforms/spark", expected_payload)
    assert_catalog_response_equal(response, expected)

# ---- Tests for ALTER object ----
@patch('pyspark_opendic.client.OpenDicClient.put')
def test_alter_with_props(mock_put, catalog):
    mock_put.return_value = {"success": True}

    query = """
    ALTER OPEN function my_function
    PROPS {
        "language": "sql",
        "version": "2.0",
        "def": "SELECT * FROM other_table"
    }
    """

    dict_props = {
        "language": "sql",
        "version": "2.0",
        "def": "SELECT * FROM other_table"
    }

    udo_object = Udo(type="function", name="my_function", props=dict_props)
    expected_payload = CreateUdoRequest(udo=udo_object).model_dump()

    response = catalog.sql(query)

    mock_put.assert_called_once_with("/objects/function/my_function", expected_payload)
    assert_catalog_response_equal(response, {
        "success": "Object altered successfully",
        "response": {"success": True}
    })

def test_dump_handler_invalid_escaped_sql(catalog):
    # This simulates a Polaris sync returning back a weirdly escaped SQL string
    # (same style as what we saw in the screenshot)
    escaped_sql = "CREATE OR ALTER function foo(arg1 int, arg2 int)\n    RETURNS int\n    LANGUAGE python\n    PACKAGES = ('pandas', 'numpy')\n    RUNTIME = 3.12\n    HANDLER = 'foo'\n    AS $$\n    def foo(arg1, arg2):\n      return arg1 + arg2\n    $$\n"

    response = [
        Statement(definition=escaped_sql)
    ]

    result = catalog.dump_handler(response)

    #print(result)
    assert isinstance(result, PrettyResponse)
    #assert "error" in str(result)



# Helper function to assert catalog response equality (based on our 'Pretty Printing')
def assert_catalog_response_equal(actual, expected_dict):
    # Grab the 'response' part from what we expect (this is what pretty_print_result uses)
    expected = expected_dict.get("response")

    # If it's a list of objects → turn it into a DataFrame
    if isinstance(expected, list) and all(isinstance(item, dict) for item in expected):
        expected_df = pd.DataFrame(expected)
    # If it's a single object → wrap it in a one-row DataFrame
    elif isinstance(expected, dict):
        expected_df = pd.DataFrame([expected])
    # Otherwise, it's probably an error or message → wrap in PrettyResponse
    else:
        expected_df = PrettyResponse(expected_dict)

    # Now compare based on what type it ended up being
    if isinstance(expected_df, pd.DataFrame):
        # Actual result should also be a DataFrame
        assert isinstance(actual, pd.DataFrame)
        # Compare the two DataFrames properly (ignore index differences)
        pd.testing.assert_frame_equal(actual.reset_index(drop=True), expected_df.reset_index(drop=True))
    else:
        # Otherwise, we expect both to be PrettyResponses (like error messages)
        assert isinstance(actual, PrettyResponse)
        # Compare their string output
        assert str(actual) == str(expected_df)

