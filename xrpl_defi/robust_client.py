"""Robust client for XRPL JSON-RPC operations.

- xrpl-py client class implementation does not survice real world network conditions
  with timeouts, etc.
"""

from httpx import AsyncClient, ConnectError, ConnectTimeout, ReadTimeout
from typing_extensions import Self
import asyncio
import logging
from json import JSONDecodeError

from xrpl.clients import JsonRpcClient
from xrpl.models.requests.request import Request
from xrpl.models.response import Response
from xrpl.asyncio.clients.client import REQUEST_TIMEOUT
from xrpl.asyncio.clients.utils import json_to_response, request_to_json_rpc
from xrpl.asyncio.clients.exceptions import XRPLRequestFailureException


logger = logging.getLogger(__name__)



class RobustJsonRpcClient(JsonRpcClient):
    """A JSON RPC client with flakiness tolerance"""

    def __init__(self, url: str, max_retries: int = 8, retry_delay: float = 2.5):
        super().__init__(url)
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    async def _request_impl(
        self: Self, request: Request, *, timeout: float = REQUEST_TIMEOUT
    ) -> Response:
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                async with AsyncClient(timeout=timeout) as http_client:
                    response = await http_client.post(
                        self.url,
                        json=request_to_json_rpc(request),
                    )
                    
                    # Check for HTTP errors that should trigger retries
                    if response.status_code >= 500:
                        raise XRPLRequestFailureException(
                            {
                                "error": response.status_code,
                                "error_message": f"Server error: {response.text}",
                            }
                        )
                    
                    try:
                        return json_to_response(response.json())
                    except JSONDecodeError:
                        raise XRPLRequestFailureException(
                            {
                                "error": response.status_code,
                                "error_message": response.text,
                            }
                        )
                        
            except (XRPLRequestFailureException, asyncio.TimeoutError, ConnectionError, ConnectError, ReadTimeout, ConnectTimeout) as e:
                last_exception = e
                
                if attempt < self.max_retries:
                    # Exponential backoff with jitter
                    delay = self.retry_delay * (2 ** attempt)
                    logger.warning(
                        f"Request failed (attempt {attempt + 1}/{self.max_retries + 1}): {e}. "
                        f"Retrying in {delay:.2f} seconds..."
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"All {self.max_retries + 1} attempts failed. Last error: {e}")
                    raise last_exception
