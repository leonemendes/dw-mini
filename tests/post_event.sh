curl -s -X POST http://127.0.0.1:8000/api/events/ \
    -H "Content-Type: application/json"\
    -d '{
        "name": "TEST",
        "user_id": 23,
        "properties": {"source": "bash"}
    }' | python -m json.tool