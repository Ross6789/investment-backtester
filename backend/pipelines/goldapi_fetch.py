import requests
import json

def make_gapi_request():
    api_key = "goldapi-4foj4a2smc0shlje-io"
    symbol = "XAU"
    curr = "USD"
    date = "/19700101"

    url = f"https://www.goldapi.io/api/{symbol}/{curr}{date}"
    
    headers = {
        "x-access-token": api_key,
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        # Parse JSON response into a Python dict
        data = response.json()
        return data

    except requests.exceptions.RequestException as e:
        print("Error:", str(e))
        return None

response_data = make_gapi_request()
print(response_data)