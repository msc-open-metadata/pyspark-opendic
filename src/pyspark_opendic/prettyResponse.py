import json

class PrettyResponse:
    def __init__(self, data: dict):
        self.data = data

    def _repr_markdown_(self):
        return f"```json\n{json.dumps(self.data, indent=4)}\n```"

    def __repr__(self):
        return json.dumps(self.data, indent=4)
