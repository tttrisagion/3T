"""
HyperLiquid client utility library for setting up authenticated connections.
Integrates with the 3T configuration system using secrets.yml.
"""

import eth_account
import mysql.connector
from eth_account.signers.local import LocalAccount
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info
from opentelemetry import trace

from shared.config import config
from shared.exchange_manager import exchange_manager

tracer = trace.get_tracer(__name__)


def setup_hyperliquid_client(base_url=None, skip_ws=False):
    """
    Set up HyperLiquid Info and Exchange clients using credentials from secrets.yml.

    Args:
        base_url: Optional base URL override
        skip_ws: Whether to skip WebSocket connections

    Returns:
        tuple: (address, info_client, exchange_client)

    Raises:
        ValueError: If required secrets are missing
        Exception: If account has no equity
    """
    with tracer.start_as_current_span("setup_hyperliquid_client") as span:
        # Get credentials from secrets.yml
        secret_key = config.get_secret("exchanges.hyperliquid.privateKey")
        account_address = config.get_secret("exchanges.hyperliquid.walletAddress")

        if not secret_key:
            raise ValueError(
                "Missing required secret: exchanges.hyperliquid.privateKey"
            )
        if not account_address:
            raise ValueError(
                "Missing required secret: exchanges.hyperliquid.walletAddress"
            )

        span.set_attribute("account.address", account_address)

        # Create account from private key
        account: LocalAccount = eth_account.Account.from_key(secret_key)

        # Validate address matches private key
        if account_address != account.address:
            span.add_event("Using agent address different from account address")
            print(f"Running with account address: {account_address}")
            print(f"Running with agent address: {account.address}")
        else:
            print(f"Running with account address: {account_address}")

        # Create Info client
        info = Info(base_url, skip_ws)

        # Validate account has equity
        user_state = info.user_state(account_address)
        spot_user_state = info.spot_user_state(account_address)
        margin_summary = user_state["marginSummary"]

        account_value = float(margin_summary["accountValue"])
        spot_balances = len(spot_user_state["balances"])

        span.set_attribute("account.value", account_value)
        span.set_attribute("account.spot_balances", spot_balances)

        if account_value == 0 and spot_balances == 0:
            url = info.base_url.split(".", 1)[1] if info.base_url else "hyperliquid"
            error_msg = (
                f"No accountValue for address {account_address}.\n"
                f"Make sure the account has a balance on {url}.\n"
                f"If the address shown is your API wallet address, update secrets.yml "
                f"to specify your account address, not the API wallet address."
            )
            span.set_attribute("error", True)
            span.add_event("Account has no equity")
            raise Exception(error_msg)

        # Create Exchange client
        exchange = Exchange(account, base_url, account_address=account_address)

        span.set_attribute("setup.success", True)
        return account_address, info, exchange


def get_exchange_instruments():
    """
    Get list of instrument symbols from the database that are supported on HyperLiquid.

    Returns:
        list: List of instrument symbols (e.g., ['BTC', 'ETH', 'SOL'])
    """
    with tracer.start_as_current_span("get_exchange_instruments") as span:
        db_cnx = mysql.connector.connect(
            host=config.get("database.host"),
            user=config.get("database.user"),
            password=config.get_secret("database.password"),
            database=config.get("database.database"),
        )

        try:
            cursor = db_cnx.cursor(dictionary=True)
            cursor.execute("""
                SELECT i.name
                FROM instruments i
                JOIN products p ON i.id = p.instrument_id
                JOIN exchanges e ON p.exchange_id = e.id
                WHERE e.name = 'HyperLiquid'
            """)
            instruments = [row["name"] for row in cursor.fetchall()]
            span.set_attribute("instruments.count", len(instruments))
            return instruments
        finally:
            if db_cnx and db_cnx.is_connected():
                cursor.close()
                db_cnx.close()


def get_resilient_exchange():
    """
    Get a resilient exchange instance with connection pooling and retry logic.

    Returns:
        ccxt.Exchange: Resilient exchange instance
    """
    return exchange_manager.get_exchange()
