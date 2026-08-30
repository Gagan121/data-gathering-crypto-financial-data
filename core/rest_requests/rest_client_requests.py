import requests

class RestClient:
    def __init__(self):
        pass

    def get_request(self, full_url, msg) -> dict:
        try:
            response = requests.get(url=full_url, params=msg)
            response.raise_for_status()
            response = response.json()

        except Exception as e:
            print(e)
            return {}

        return response