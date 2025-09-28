import requests

url = "http://127.0.0.1:8000/api/events/"
data = {
    "name": "TEST",
    "user_id": 23,
    "properties": {
        "source": "python"
    }
}

r = requests.post(url, json=data)
print(r.status_code, r.text)