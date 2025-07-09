"""Ledger utilities."""
import datetime

from xrpl.clients import JsonRpcClient
from xrpl.models.requests import Ledger


def get_latest_ledger_index(client: JsonRpcClient) -> int:
    """
    Get the latest validated ledger index from the XRPL.
    
    :returns:
        int: The latest validated ledger index.
    """
    # Request the latest ledger information
    ledger_request = Ledger(ledger_index="validated")
    response = client.request(ledger_request).result
    
    # Return the ledger index
    return int(response["ledger"]["ledger_index"])


def get_closest_ledger_index_for_time(client: JsonRpcClient, timestamp: datetime.datetime) -> int:
    unix_time = timestamp.timestamp()
    request = Ledger(ledger_time=unix_time)
    response = client.request(request)
    return int(response.result["ledger"]["ledger_index"])