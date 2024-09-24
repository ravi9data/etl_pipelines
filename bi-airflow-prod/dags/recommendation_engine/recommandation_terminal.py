import requests


def indexRecommendationsToCatalog():
    url = 'https://search-indexer.eu-production.internal.grover.com/indexRecommendationsToCatalog'
    headers = {'Accept': '*/*'}
    response = requests.get(url, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        return response.text
    else:
        return f"Error: {response.status_code}"
