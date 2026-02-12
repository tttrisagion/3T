#!/bin/bash
#
# Add a new trading symbol to the 3T database (instruments + products).
#
# Usage:
#   ./scripts/add_symbol.sh <SYMBOL> [MAX_LEVERAGE]
#
# Examples:
#   ./scripts/add_symbol.sh ONDO 5
#   ./scripts/add_symbol.sh PEPE         # defaults to max_leverage=3
#
# This inserts into both `instruments` and `products` tables.
# To activate the symbol for trading, also add it to config.yml
# under reconciliation_engine.symbols and run `make install`.

set -euo pipefail

echo "--- DISCLAIMER ---"
echo "The symbols listed are for informational purposes only."
echo "This project is not financial, investment, or trading advice, nor is it a"
echo "solicitation or recommendation for any asset. The Trisagion project is not a"
echo "licensed financial entity."
echo "All users must conduct their own research and consult a qualified financial"
echo "advisor before making decisions. Engaging with cryptocurrencies involves"
echo "significant risk, including the potential for total loss. By using this"
echo "configuration, you acknowledge full responsibility for your financial choices."
echo "---"
echo ""

DB_HOST="${DB_HOST:-127.0.0.1}"
DB_PORT="${DB_PORT:-3306}"
DB_USER="${DB_USER:-root}"
DB_PASS="${DB_PASS:-secret}"
DB_NAME="${DB_NAME:-3t}"

if [ $# -lt 1 ]; then
    echo "Usage: $0 <SYMBOL> [MAX_LEVERAGE]"
    echo ""
    echo "Examples:"
    echo "  $0 ONDO 5"
    echo "  $0 PEPE        # defaults to max_leverage=3"
    exit 1
fi

SYMBOL="${1^^}"  # uppercase
MAX_LEVERAGE="${2:-3}"
PRODUCT_SYMBOL="${SYMBOL}/USDC:USDC"

MYSQL_CMD="mysql -h ${DB_HOST} -P ${DB_PORT} -u ${DB_USER} -p${DB_PASS} ${DB_NAME}"

# Check if instrument already exists
EXISTING=$(${MYSQL_CMD} -sN -e "SELECT COUNT(*) FROM instruments WHERE name = '${SYMBOL}';")
if [ "$EXISTING" -gt 0 ]; then
    echo "Instrument '${SYMBOL}' already exists."
    # Check if product also exists
    PROD_EXISTS=$(${MYSQL_CMD} -sN -e "SELECT COUNT(*) FROM products WHERE symbol = '${PRODUCT_SYMBOL}';")
    if [ "$PROD_EXISTS" -gt 0 ]; then
        echo "Product '${PRODUCT_SYMBOL}' already exists. Nothing to do."
        exit 0
    else
        echo "Adding missing product entry..."
        ${MYSQL_CMD} -e "
            INSERT INTO products (instrument_id, exchange_id, symbol, product_type, max_leverage)
            VALUES ((SELECT id FROM instruments WHERE name = '${SYMBOL}'), 1, '${PRODUCT_SYMBOL}', 'PERP', ${MAX_LEVERAGE});
        "
        echo "Product '${PRODUCT_SYMBOL}' added (max_leverage=${MAX_LEVERAGE})."
        exit 0
    fi
fi

# Insert instrument and product
${MYSQL_CMD} -e "
    INSERT INTO instruments (name) VALUES ('${SYMBOL}');
    INSERT INTO products (instrument_id, exchange_id, symbol, product_type, max_leverage)
    VALUES ((SELECT id FROM instruments WHERE name = '${SYMBOL}'), 1, '${PRODUCT_SYMBOL}', 'PERP', ${MAX_LEVERAGE});
"

echo "Added '${SYMBOL}' -> '${PRODUCT_SYMBOL}' (max_leverage=${MAX_LEVERAGE})"
echo ""
echo "To activate for trading, add '${PRODUCT_SYMBOL}' to config.yml under"
echo "reconciliation_engine.symbols, then run: make install"
