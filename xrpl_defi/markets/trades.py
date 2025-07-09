"""Fetch raw AMM trades from the XRPL."""

import json
import logging
from pathlib import Path
from typing import Iterable

from tqdm_loggable.auto import tqdm

import pandas as pd

from xrpl.clients import JsonRpcClient
from xrpl.models.requests import AccountTx
from xrpl.models.transactions import Payment
from xrpl_defi.asset import decode_currency_symbol
from xrpl_defi.ledger import get_latest_ledger_index
from xrpl.utils import ripple_time_to_datetime


logger = logging.getLogger(__name__)



def get_amm_trades(
    client: JsonRpcClient, 
    account: str, 
    limit=200, 
    progress=True,
    max_ledger_index=None,
) -> Iterable[dict]:
    """
    Retrieve all trade transactions for AMM.

    - Only considers AMM payment/offer transactions, does not work with decentralised exchange.

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
        
    :returns:
        list: A list of Payment transaction objects.
    """
    payment_txs = []
    marker = None

    # Get the latest ledger index
    latest_ledger = get_latest_ledger_index(client)
    logger.info(f"Latest validated ledger index: {latest_ledger}")

    cap = max_ledger_index if max_ledger_index is not None else latest_ledger

    progress_bar = None
    count = 0
    tx_ledger_index = None

    # Loop termination handled by RPC market API
    while True:
        # Prepare the account_tx request
        request = AccountTx(
            account=account,
            limit=limit,
            marker=marker,
            ledger_index_min=-1,
            ledger_index_max=latest_ledger,
            forward=True,
        )
        
        # Send the request
        response = client.request(request).result
        transactions = response.get("transactions", [])
        
        # Filter for Payment transactions
        for tx in transactions:            
            if tx.get("tx_json", {}).get("TransactionType") in ("Payment", "OfferCreate"):
                tx["market"] = account  # Add market field, because it is not reflected back by the node
                count += 1
                yield tx

            tx_ledger_index = int(tx["ledger_index"])
    
            if progress and not progress_bar:
                # We can show progress bar after the first tx
                start_ledger_index = tx_ledger_index
                progress_bar = tqdm(
                    desc="Fetching payment txs",
                    unit="ledger",
                    unit_scale=True,
                    unit_divisor=1_000_000,
                    total=latest_ledger - start_ledger_index,  # Total will be set dynamically
                )

            if progress_bar:
                # Update the progress bar with the current ledger index
                progress_bar.set_postfix({
                    "Ledger Index": f"{tx_ledger_index:,}",
                    "Payment txs": f"{count:,}",
                })
                progress_bar.n = tx_ledger_index - start_ledger_index
                progress_bar.refresh()

        if tx_ledger_index is not None and tx_ledger_index >= cap:
            logger.info(f"Reached max ledger index {cap}, stopping.")
            break
                
        # Check for pagination marker
        marker = response.get("marker")
        if not marker:
            break  # No more transactions to fetch
        
        # logger.info(f"Fetching next batch with marker: {marker}")

        if progress_bar:
            # Update the progress bar with the current ledger index
            progress_bar.update(1)
    
    if progress_bar:
        progress_bar.close()
    
    return payment_txs


def prepare_trades_data(payment_transactions: Iterable[dict]) -> pd.DataFrame:
    """
    Prepare trades data from AMM trades.

    - Makes raw XRP data to useseable swap event data
    - Parses out buy/sell, price and other relevant fields

    - In Currency and Amount: These are typically found in the SendMax field of the tx_json, which specifies the maximum amount and currency the sender is willing to send.
    - Out Currency and Amount: These are typically found in the DeliverMax or delivered_amount field, which specifies the amount and currency the sender expects to receive.

    - Exported numbers are float64 and thus useable only for data research and analysis.

    - Example swap (payment) https://xrpscan.com/tx/F007D9DA51F0849A3298B8F73C4E378907FD60406A9888AC90056D8B082E136D
    - Example swap (payment) https://xrpscan.com/tx/6968AF0509D382CBEF10EE02F627DEBD48C02CB091A1B478D49FB43C663DDFD1
    - Example swap (offer) https://xrpscan.com/tx/A8D33649BEC347D5D5AE7614A339246430ED44C09FA5E68951CC14C03200D3C9

    TODO: Add handling fees, such
    
    :param payment_transactions: Iterable of payment transaction dictionaries.
    :returns: DataFrame containing trade data.
    """

    data = []
    
    for tx in payment_transactions:
        tx_json = tx["tx_json"]

        assert tx_json 

        ripple_date = tx_json["date"]
        timestamp = ripple_time_to_datetime(ripple_date)

        # 
        match tx_json["TransactionType"]:
            case "Payment":
                # https://xrpscan.com/tx/F007D9DA51F0849A3298B8F73C4E378907FD60406A9888AC90056D8B082E136D
                # Payment transaction
                amount_out = tx["DeliveredAmount"]["value"]
                currency_out = decode_currency_symbol(tx["DeliveredAmount"]["currency"])            
                amount_in = tx["Amount"]["value"]
                currency_in = decode_currency_symbol(tx["Amount"]["currency"])            
            case "OfferCreate":
                # OfferCreate transaction
                # https://xrpscan.com/tx/A8D33649BEC347D5D5AE7614A339246430ED44C09FA5E68951CC14C03200D3C9
                import ipdb ; ipdb.set_trace()
                amount_in = tx_json["TakerPays"]["value"]
                currency_in = decode_currency_symbol(tx["TakerPays"]["currency"])
                amount_out = tx["TakerGets"]["value"]
                currency_out = decode_currency_symbol(tx["TakerGets"]["currency"])
            case _:
                raise NotImplementedError(
                    f"Unsupported TransactionType: {tx['TransactionType']}"
                )   

        entry = {
            "timestamp": timestamp,        
            "ledger_index": int(tx_json["ledger_index"]),
            "market": tx["market"],  # AMM account injected
            "amount_in": amount_in,
            "amount_out":amount_out,
            "currency_in": currency_in,  # XRP/RLUSD/CRYPTO str
            "currency_out": currency_out,  # XRP/RLUSD/CRYPTO str
            "tx_hash": tx["hash"],  # Transaction hash
            "from_account": tx_json["Account"],  # Sender account
            "to_account": tx_json["Destination"],  # Destination account, usually the same as sender as swaps against the AMM            
            # Store the original data as JSON dump
            "raw_tx": json.dumps(tx_json),
        }
        data.append(entry)

    assert len(data) > 0, "No payment transactions found"

    df = pd.DataFrame(data)
    df = df.sort_values(by="timestamp", ascending=True)
    return df


def main():
    """CLI example for inspecting AMM trades."""

    # https://xrpscan.com/tx/58E09AEC42C052060A3E2290C58E82D0EBF0A0EDF0B8F78D68DF197FB077A6B7
    
    # account = "rExvX9VhzYFHa73rY77PBLNtCDNwNM2bB4"

    JSON_RPC_URL = "https://xrplcluster.com/"

    from xrpl_defi.utils.log import setup_console_logging

    setup_console_logging(default_log_level="info")

    client = JsonRpcClient(JSON_RPC_URL)

    # AMM account CRYPTO/XRP
    # https://xrpscan.com/account/rLjUKpwUVmz3vCTmFkXungxwzdoyrWRsFG
    # AMM account RLUSD/XRP
    # https://xrpscan.com/account/rhWTXC2m2gGGA9WozUaoMm6kLAVPb1tcS3
    account = "rhWTXC2m2gGGA9WozUaoMm6kLAVPb1tcS3"
    
    logger.info(f"Fetching Payment transactions for account: {account}")

    # Fetch Payment transactions
    payment_transactions = get_amm_trades(
        client, 
        account,
        max_ledger_index=87_544_747,  # Sample few trades as start
    )

    payment_transactions = list(payment_transactions)
    
    # Log results
    logger.info(f"Found {len(payment_transactions)} Payment transactions for {account}:")

    df = prepare_trades_data(payment_transactions)
    path = Path("/tmp/amm_trades.parquet")
    df.to_parquet(path, index=False, compression='zstd')

    size = path.stat().st_size
    logger.info(f"Saved {len(df):,} trades to {path} ({size / 1024:.2f} KB)")
    

if __name__ == "__main__":
    main()