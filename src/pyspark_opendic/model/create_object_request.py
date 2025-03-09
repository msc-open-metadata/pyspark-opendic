import json

class CreateObjectRequest:
    def __init__(self, object_type : str, object_name : str, object_args : dict):
        self.object_type = object_type
        self.object_name = object_name
        self.object_args = object_args

        self.payload = {
            "type": object_type,
            "name": object_name,
            "args": object_args
        }
    
    def to_json(self):
        return json.dumps(self.payload)