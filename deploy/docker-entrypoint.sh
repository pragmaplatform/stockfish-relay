#!/bin/sh
set -eu

if [ "$#" -eq 0 ]; then
    set -- "${STOCKFISH_RELAY_MODE:-master}"
fi

exec /usr/local/bin/stockfish-relay "$@"
