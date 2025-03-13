import requests

# TODO: Add handling for credentials for client OAuth
# TODO: Find a better way to handle the url
class OpenDicClient:
    def __init__(self, api_url : str, credentials : str):
        self.api_url = api_url
        self.credentials = credentials
        self.oauth_token = self.get_polaris_oauth_token(credentials)

    def post(self, endpoint, data):
        url = self.api_url+ "/opendic/v1" + endpoint
        response = requests.post(url, json=data, headers={"Authorization": f"Bearer {self.oauth_token}"})
        print("URL:", url)
        response.raise_for_status() # Raise an exception if the response is not successful
        return response.json()
    
    def get(self, endpoint):
        url = self.api_url + "/opendic/v1" + endpoint
        response = requests.get(url, headers={"Authorization": f"Bearer {self.oauth_token}"})
        response.raise_for_status() # Raise an exception if the response is not successful
        return response.json()
    
    def put(self, endpoint, data):
        url = self.api_url + endpoint
        response = requests.put(url, json=data, headers={"Authorization": f"Bearer {self.oauth_token}"})
        response.raise_for_status() # Raise an exception if the response is not successful
        return response.json()
    
    def delete(self, endpoint):
        url = self.api_url + endpoint
        response = requests.delete(url, headers={"Authorization": f"Bearer {self.oauth_token}"})
        response.raise_for_status() # Raise an exception if the response is not successful
        return response.json()
    
    # Helper function to get the OAuth token
    def get_polaris_oauth_token(self, credentials:str):
        client_id = credentials.split(":")[0]
        client_secret= credentials.split(":")[1]

        url = f"{self.api_url}/catalog/v1/oauth/tokens"
        data = {
            "grant_type": "client_credentials",
            "client_id": f"{client_id}",
            "client_secret": f"{client_secret}",
            "scope": "PRINCIPAL_ROLE:ALL"
        }

        return requests.post(url, data=data).json()["access_token"]
