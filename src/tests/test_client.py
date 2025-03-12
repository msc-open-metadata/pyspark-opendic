import pytest
import requests
from unittest.mock import patch
from pyspark_opendic.client import OpenDicClient
from pyspark_opendic.model.create_object_request import CreateObjectRequest

# Test client is correctly called with the right URL & data
# TODO: add test on OAuth handling
# TODO: test other methods (get, put, delete) in a similar way, when we have added the models and methods to use them


MOCK_API_URL = "https://mock-api-url.com"


@patch("requests.post")
def test_post_function(mock_post : requests.post):
    """Test if the OpenDicClient correctly sends a POST request."""
    
    # Create an instance of the clint
    client = OpenDicClient(MOCK_API_URL)

    # Fake the API response on the mock object (the requests.post function)
    mock_post.return_value.status_code = 200
    mock_post.return_value.json.return_value = {"success": True}

    dict_props = {"args": {"arg1": "string", "arg2": "number"}, "language": "sql"}
    payload = CreateObjectRequest("function", "my_function", None, dict_props).to_json()

    # Call the actual function (this normally calls requests.post - which is replaced with mock_post here)
    response = client.post("/functions", payload)

    # Verify that requests.post was actually called with the right URL & data
    mock_post.assert_called_with(
        f"{MOCK_API_URL}/functions",
        json = payload
    )

    # Check if we got the expected response
    assert response == {"success": True}

# TODO: obs. not sure about the return format of SHOW yet, so this test is a placeholder
@patch("requests.get")
def test_get_function(mock_get : requests.get):
    """Test if OpenDicClient correctly sends a GET request."""
    
    # Create an instance of the clint
    client = OpenDicClient(MOCK_API_URL)

    # Fake the API response on the mock object (the requests.get function)
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {"success": True}

    # Call the actual function
    response = client.get("/functions")

    # Verify that requests.get was actually called with the right URL
    mock_get.assert_called_with(
        f"{MOCK_API_URL}/functions"
    )

    # Check if we got the expected response
    assert response == {"success": True}
