import json

class CreateObjectRequest:
    def __init__(self, object_type : str, object_name : str, object_alias, object_props : dict):
        self.object_type = object_type
        self.object_name = object_name
        self.object_alias = object_alias
        self.object_props = object_props

        self.payload = {
            "type": object_type,
            "name": object_name,
            "alias": object_alias,
            "props": object_props
        }
    
    def to_json(self):
        return json.dumps(self.payload)