import logging
from typing import Any, Generator, List

import requests


def create_collection(name: str, api_domain: str, api_key: str,  destination_id: str,
                      requests_type: str = None) -> str:
    body = {
        "name": name,
        "enabled": True,
        "schedule_type": "manual",
        "priority": "normal",
        # "destination_ids":  [destination_id],
        "notification_email": "dataengsys@grover.com",
        "notification_as_jsonlines": True,
    }
    if requests_type is not None:
        body["requests_type"] = requests_type
    r = requests.post(f'https://{api_domain}/collections?api_key={api_key}', json=body).json()
    check_response_success(r)
    logging.info(f"Collection created: {r}")
    return r["collection"]["id"]


def add_requests(from_df: str, to_collection: str, api_domain: str, api_key: str) -> None:
    url = f'https://{api_domain}/collections/{to_collection}?api_key={api_key}'
    MAX_REQUESTS = 1000
    records = from_df.to_dict(orient="records")
    record_chunks = chunks(records, MAX_REQUESTS)
    for chunk in record_chunks:

        body = {
            "requests": chunk
        }
        r = requests.put(url, json=body).json()
        logging.info(f"Collection {to_collection} Updated: {r}")
        check_response_success(r)


def chunks(lst, n) -> Generator[List[Any], None, None]:
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def start_collection(id: str, api_domain: str, api_key: str) -> None:
    r = requests.get(f"https://{api_domain}/collections/{id}/start?api_key={api_key}").json()
    logging.info(f"Collection {id} started: {r}")
    check_response_success(r)


def check_response_success(response: dict) -> None:
    if isinstance(response, str):
        raise requests.exceptions.RequestException(response)
    if response["request_info"]["success"] is False:
        raise requests.exceptions.RequestException(response)
