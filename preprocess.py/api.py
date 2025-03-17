import requests

DEEPSEEK_API_KEY = "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"

def explain_anomaly(features):
    prompt = f"""
    Explain why this trip was flagged as an anomaly:
    - Pickup Location: {features['PUlocationID']}
    - Dropoff Location: {features['DOlocationID']}
    - Trip Duration: {features['trip_duration']} minutes
    - Pickup Time: {features['pickup_hour']}:00
    """
    response = requests.post(
        "https://api.deepseek.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {DEEPSEEK_API_KEY}"},
        json={"messages": [{"role": "user", "content": prompt}]}
    )
    return response.json()['choices'][0]['message']['content']
