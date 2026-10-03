import time

import requests
import json

events = []

url = "https://gamma-api.polymarket.com/events/keyset"
params = {
    "closed": "false",
    "limit":500
}

response = requests.get(url, params=params)

data = json.loads(response.text)



while "next_cursor" in data:

    events += data['events']
    time.sleep(1)
    # pagination occurs when we use the next_cursor to fill in the parameter for after_cursor -> it allows you to move on to the next set of pages .....
    new_url = "https://gamma-api.polymarket.com/events/keyset?closed=false"
    params = {
        'after_cursor':data.get("next_cursor"),
        "closed": "false",
        "limit":500
    }

    response = requests.get(url, params=params)
    data = json.loads(response.text)
    print(data)

print("stop")