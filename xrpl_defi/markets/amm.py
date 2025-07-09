"""AMM raw data reading."""

import datetime
import json
import logging
import os
from pathlib import Path
from typing import Iterable

import pandas as pd
from tqdm_loggable.auto import tqdm

from xrpl.models.requests import AMMInfo
from xrpl.clients import JsonRpcClient
from xrpl.models.requests import AccountTx
from xrpl.models.requests import AccountLines
from xrpl.utils import ripple_time_to_datetime
from xrpl.account import get_balance


from xrpl_defi.asset import decode_currency_symbol
from xrpl_defi.ledger import get_closest_ledger_index_for_time, get_latest_ledger_index

logger = logging.getLogger(__name__)


def parse_amm_amount(amount: str | dict) -> tuple[str, float]:
    """
    
    {'amm': {'account': 'rLjUKpwUVmz3vCTmFkXungxwzdoyrWRsFG',
         'amount': '3309114747027',
         'amount2': {'currency': '43525950544F0000000000000000000000000000',
                     'issuer': 'rRbiKwcueo6MchUpMFDce9XpDwHhRLPFo',
                     'value': '661186.9433432882'},    
    """
    if type(amount) is str:
        # Simple amount, no currency
        return "XRP", float(amount) / 10**6
    elif type(amount) is dict:
        currency = decode_currency_symbol(amount["currency"])
        value = float(amount["value"])
        return currency, value


def fetch_amm_historical_payment_and_balances(
    client: JsonRpcClient, 
    account: str, 
    limit=1500, 
    progress=True,
    max_ledger_index=None,
    max_freq=datetime.timedelta(hours=1),
    min_ledger_index=-1,
) -> Iterable[dict]:
    """
    Retrieve AMM supply and price data using trade events for ledger indexes.

    - Payment transactions themeslves do not expose full data to calculate the price,
      so we use this complex approach to parse XRP history
    - Calculate price using AMM supply
    - AMM supply can only change with payment transaction
    - Get all paymnent transactions for AMM account
    - Use ledger index values from payment transactions to scan the AMM historical supply data

    :param client:
        The JsonRpcClient instance to use for making requests.

    :parma account:
        The AMM account address to fetch transactions for.

    :param limit:
        The maximum number of transactions to fetch per request.

    :param progress:
        If True, show a TQDM progress bar for the transaction fetching process.

    :param max_ledger_index: 
        Stop when reaching this ledger index.

        For testing.

    :param max_freq:
        Do not sample too often.
        
    :returns:
        list: A list of Payment transaction with retrofitted AMM data.
    """
    payment_txs = []
    marker = None

    # Get the latest ledger index
    latest_ledger = get_latest_ledger_index(client)
    logger.info(f"Latest validated ledger index: {latest_ledger:,}")

    cap = max_ledger_index if max_ledger_index is not None else latest_ledger

    progress_bar = None
    count = 0
    tx_ledger_index = None
    last_event = None
    timestamp = None

    amm_info = AMMInfo(
        amm_account=account,
    )

    result = client.request(amm_info).result

    asset_1, amount_1 = parse_amm_amount(result["amm"]["amount"])
    asset_2, amount_2 = parse_amm_amount(result["amm"]["amount2"])

    logger.info(f"AMM {account} assets: {asset_1} and {asset_2}, amounts: {amount_1} and {amount_2}")

    # Loop termination handled by RPC market API
    while True:
        # Prepare the account_tx request
        request = AccountTx(
            account=account,
            limit=limit,
            marker=marker,
            ledger_index_min=min_ledger_index,
            ledger_index_max=latest_ledger,
            forward=True,
        )
        
        # Send the request
        response = client.request(request).result
        transactions = response.get("transactions", [])

        logger.info("Fetched %d transactions", len(transactions))

        # Filter for Payment transactions
        for tx in transactions:            
            tx_json = tx.get("tx_json", {})
            ledger_index = int(tx["ledger_index"])
            if tx_json and tx_json["TransactionType"] == "Payment":
                ripple_date = tx_json["date"]
                timestamp = ripple_time_to_datetime(ripple_date)
                
                if last_event and timestamp - last_event < max_freq:
                    # Skip events that are too close to the last event
                    continue

                request = AccountLines(
                    account=account,
                    ledger_index=ledger_index,
                )
            
                account_lines_response = client.request(request).result

                for l in account_lines_response["lines"]:
                    currency = decode_currency_symbol(l["currency"])
                    if currency == asset_1:
                        tx["amm_asset_1_amount"] = float(l["balance"]) 
                    elif currency == asset_2:
                        tx["amm_asset_2_amount"] = float(l["balance"]) 
                        break

                if asset_1 == "XRP":
                    balance = get_balance(account, client=client, ledger_index=ledger_index)
                    tx["amm_asset_1_amount"] = float(balance) / 10**6
                elif asset_2 == "XRP":
                    balance = get_balance(account, client=client, ledger_index=ledger_index)
                    tx["amm_asset_2_amount"] = float(balance) / 10**6
                    
                tx["amm_asset_1"] = asset_1
                tx["amm_asset_2"] = asset_2 
                tx["market"] = account  # Add market field, because it is not reflected back by the node
                count += 1
                last_event = timestamp
                yield tx

            if progress and not progress_bar:
                # We can show progress bar after the first tx
                start_ledger_index = ledger_index
                progress_bar = tqdm(
                    desc=f"Fetching AMM data for {account} = {asset_1}-{asset_2}",
                    unit="ledger",
                    unit_scale=True,
                    unit_divisor=1_000_000,
                    total=latest_ledger - start_ledger_index,  # Total will be set dynamically
                )

            if progress_bar:
                # Update the progress bar with the current ledger index
                progress_bar.set_postfix({
                    "Ledger Index": f"{ledger_index:,}",
                    "Payment txs": f"{count:,}",
                    "Timestamp": f"{timestamp.isoformat() if timestamp else 'N/A'}",
                })
                progress_bar.n = ledger_index - start_ledger_index
                progress_bar.refresh()

        if ledger_index is not None and ledger_index >= cap:
            logger.info(f"Reached max ledger index {cap}, stopping.")
            break
                
        # Check for pagination marker
        marker = response.get("marker")
        if not marker:
            logger.info("No more transactions to fetch.")
            break  # No more transactions to fetch
        
        # logger.info(f"Fetching next batch with marker: {marker}")

        if progress_bar:
            # Update the progress bar with the current ledger index
            progress_bar.update(1)
    
    if progress_bar:
        progress_bar.close()
    
    return payment_txs


def prepare_amm_data(amm_data: Iterable[dict]) -> pd.DataFrame:
    """Prepare AMM data for saving.
    
    - Convert raw AMM and payment event data into a DataFrame

    TODO: Add volume.
    """
    df = pd.DataFrame(amm_data)

    data = []

    for tx in amm_data:

        tx_json = tx["tx_json"]
        ripple_date = tx_json["date"]
        timestamp = ripple_time_to_datetime(ripple_date)

        entry = {
            "timestamp": timestamp,
            "tx_hash": tx["hash"],
            "ledger_index": tx["ledger_index"],
            "market": tx["market"],
            "amm_asset_1": tx["amm_asset_1"],
            "amm_asset_2": tx["amm_asset_2"],
            "amm_asset_1_amount": tx.get("amm_asset_1_amount"),
            "amm_asset_2_amount": tx.get("amm_asset_2_amount"),
            "raw_json": json.dumps(tx),
        }
        data.append(entry)

    assert len(data) > 0, "No AMM data found"

    df = pd.DataFrame(data)
    df = df.sort_values(by="timestamp")
    return df
            

def main():
    """CLI example for inspecting AMM trades."""

    # https://xrpscan.com/tx/58E09AEC42C052060A3E2290C58E82D0EBF0A0EDF0B8F78D68DF197FB077A6B7
    
    # account = "rExvX9VhzYFHa73rY77PBLNtCDNwNM2bB4"

    JSON_RPC_URL = os.environ["JSON_RPC_XRPL"]

    from xrpl_defi.utils.log import setup_console_logging

    setup_console_logging(default_log_level="info")

    client = JsonRpcClient(JSON_RPC_URL)

    AMM_MARKETS = [
        "rLjUKpwUVmz3vCTmFkXungxwzdoyrWRsFG",  # CRYPTO-XRP
        "rhWTXC2m2gGGA9WozUaoMm6kLAVPb1tcS3",  # RLUSD-XRP
    ]

    # Sample 3 days
    data = fetch_amm_historical_payment_and_balances(
        client,
        account="rhWTXC2m2gGGA9WozUaoMm6kLAVPb1tcS3",
        max_freq=datetime.timedelta(hours=1),
    )

    data = list(data)
    df = prepare_amm_data(data)

    path = Path("/tmp/amm_scan.parquet")
    df.to_parquet(path, index=False, compression='zstd')

    size = path.stat().st_size
    logger.info(f"Saved {len(df):,} trades to {path} ({size / 1024:.2f} KB)")    

if __name__ == "__main__":
    main()