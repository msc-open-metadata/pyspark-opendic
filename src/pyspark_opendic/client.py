import requests

#TODO: Add handling for credentials for client OAuth
class OpenDicClient:
    def __init__(self, api_url : str):
        self.api_url = api_url

    def post(self, endpoint, data):
        url = self.api_url + endpoint
        response = requests.post(url, json=data)
        response.raise_for_status # Raise an exception if the response is not successful
        return response.json()
    
    def get(self, endpoint):
        url = self.api_url + endpoint
        response = requests.get(url)
        response.raise_for_status # Raise an exception if the response is not successful
        return response.json()
    
    def put(self, endpoint, data):
        url = self.api_url + endpoint
        response = requests.put(url, json=data)
        response.raise_for_status # Raise an exception if the response is not successful
        return response.json()
    
    def delete(self, endpoint):
        url = self.api_url + endpoint
        response = requests.delete(url)
        response.raise_for_status # Raise an exception if the response is not successful
        return response.json()
    
