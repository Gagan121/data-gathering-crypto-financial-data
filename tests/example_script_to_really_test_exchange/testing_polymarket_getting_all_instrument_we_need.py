import requests
import json
import time
import ast
import websockets
import asyncio
"""
# this was used to find all events in a series -> finds events in a list
url = "https://gamma-api.polymarket.com/series"

params = {
    "slug": "btc-up-or-down-5m",
    "limit_per_type": 1,
    "ascending": "true",
    "closed": "false"
}

response = requests.get(url, params=params)

data = json.loads(response.text)

print(data)

"""

current_time = int(time.time())
# 300 is equal to 5 minutes in seconds we are getting the reminder and removing it from the original time
now = current_time - (current_time % 300)

# this was used to find all events in a series -> finds events in a list

# slug = f"btc-up-or-down-5m"
# url = f"https://gamma-api.polymarket.com/series?slug={slug}&closed=false&limit=500&offset=500"
# slug only works here with series otherwise slug needs a timestamp at the end of it
url = f"https://gamma-api.polymarket.com/series"
params = {
    "slug" : f"btc-up-or-down-5m",
    "closed":"false",
    "limit":500,
    "offset":0,
}


response = requests.get(url, params=params)
data = json.loads(response.text)


series_id = "10684"

url = "https://gamma-api.polymarket.com/events/keyset"
params = {
    "limit":200,
    "ascending":"true",
    "closed": "false",
    # this is correlated with the "btc-up-or-down-5m"
    "series_id":"10684"
}

response = requests.get(url, params=params)

data = json.loads(response.text)

events = []

while "next_cursor" in data:

    events += data['events']
    time.sleep(1)
    # pagination occurs when we use the next_cursor to fill in the parameter for after_cursor -> it allows you to move on to the next set of pages .....
    new_url = "https://gamma-api.polymarket.com/events/keyset"
    params = {
        'after_cursor':data.get("next_cursor"),
        "limit": 200,
        "ascending": "true",
        "closed": "false",
        # this is correlated with the "btc-up-or-down-5m"
        "series_id": "10684"
    }

    response = requests.get(url, params=params)
    data = json.loads(response.text)
    print(data)

events += data['events']

print("stop")
# --------------------------------------------------------------------------------------------
# events[0]['markets'][0]["outcomes"] -> '["Up", "Down"]'
# events[0]['markets'][0]["clobTokenIds"] -> '["44594613733704690315394897227759662521183429824282007907633921479634990989260", "89077455060153802984632332231552501115671307713789066937817034009529706763206"]'























clobTokenIds = ast.literal_eval(data['clobTokenIds'])

print(data)



url = "https://clob.polymarket.com/books"

payload = [{ "token_id": clobTokenIds[0] }, { "token_id": clobTokenIds[1] }]
# headers = {"Content-Type": "application/json"}
headers = {}

response = requests.post(url, json=payload, headers=headers)


data = json.loads(response.text)

print(data)


def sort_data(message, dict_of_list_of_messages):
    key = message['event_type']
    if key not in dict_of_list_of_messages:
        dict_of_list_of_messages[key] = []
    else:
        dict_of_list_of_messages[key].append(message)
    print(message)


# -> flow is get the slug and then get the clob token IDs and then put them into a post request get the response, linked to the token ids

async def test(clobTokenIds: list):
    payload = {
        "assets_ids": clobTokenIds,
        "type": "market",
        "level": 2
    }

    websocket_url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    async with websockets.connect(websocket_url) as ws:
        await ws.send(json.dumps(payload))

        dict_of_list_of_messages = dict()

        while True:
            messages = json.loads(await ws.recv())
            if isinstance(messages, list):
                for message in messages:
                    sort_data(message, dict_of_list_of_messages)
            else:
                sort_data(messages, dict_of_list_of_messages)



asyncio.run(test(clobTokenIds=clobTokenIds))

print("")

