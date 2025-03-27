import pytest
import requests
from unittest.mock import patch
from pyspark_opendic.client import OpenDicClient
from pyspark_opendic.model.openapi_models import CreateUdoRequest, Udo

# Test client is correctly called with the right URL & data
# TODO: add test on OAuth handling
# TODO: test other methods (get, put, delete) in a similar way, when we have added the models and methods to use them


MOCK_API_URL = "https://mock-api-url.com"

@pytest.fixture
@patch('pyspark_opendic.client.OpenDicClient.get_polaris_oauth_token', return_value="mocked_token")
def client(mock_get_token):
    """Creates an instance of OpenDicClient."""
    return OpenDicClient(MOCK_API_URL, "s:s")


@patch("requests.post")
def test_post_function(mock_post : requests.post, client):
    """Test if the OpenDicClient correctly sends a POST request."""

    # Fake the API response on the mock object (the requests.post function)
    mock_post.return_value.status_code = 200
    mock_post.return_value.json.return_value = {"success": True}

    dict_props = {"args": {"arg1": "string", "arg2": "number"}, "language": "sql", "definition": "SELECT * FROM my_table"}
    udo_object = Udo(type = "function", name = "my_function", props = dict_props)
    payload = CreateUdoRequest(udo = udo_object).model_dump()

    # Call the actual function (this normally calls requests.post - which is replaced with mock_post here)
    response = client.post("/objects/functions", payload)

    # Verify that requests.post was actually called with the right URL & data
    mock_post.assert_called_with(
        f"{MOCK_API_URL}/opendic/v1/objects/functions",
        json = payload,
        headers = {'Authorization': 'Bearer mocked_token', 'Content-Type': 'application/json'}
    )

    # Check if we got the expected response
    assert response == {"success": True}

# TODO: obs. not sure about the return format of SHOW yet, so this test is a placeholder
@patch("requests.get")
def test_get_function(mock_get : requests.get, client):
    """Test if OpenDicClient correctly sends a GET request."""
    
    # Fake the API response on the mock object (the requests.get function)
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {"success": True}

    # Call the actual function
    response = client.get("/objects/functions")

    # Verify that requests.get was actually called with the right URL
    mock_get.assert_called_with(
        f"{MOCK_API_URL}/opendic/v1/objects/functions",
        headers={"Authorization": "Bearer mocked_token"}
    )

    # Check if we got the expected response
    assert response == {"success": True}
