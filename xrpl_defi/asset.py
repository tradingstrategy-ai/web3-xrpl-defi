"""XRP ledger token metadata utilities."""

import logging


logger = logging.getLogger(__name__)


def decode_currency_symbol(currency: str) -> str:
    """Decode XRPL currency symbol from its hexadecimal representation.
    
    """
    return bytes.fromhex(currency).decode('utf-8')



if __name__ == "__main__":
    # CRYPTO
    print(decode_currency_symbol("43525950544F0000000000000000000000000000"))