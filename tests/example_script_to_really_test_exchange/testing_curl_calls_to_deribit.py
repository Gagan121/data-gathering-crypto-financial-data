import requests

url = "https://test.deribit.com/api/v2/public/get_instruments"

params =  {
    "currency": "BTC",
    "kind": "option",
    "expired":"false"
}

response = requests.get(url=url, params=params)
data = response.json()
print(response.text)