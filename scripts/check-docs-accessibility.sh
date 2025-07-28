#!/bin/bash

set -ex

PORT=8080
SERVER="http://localhost:$PORT"
# export PUPPETEER_EXECUTABLE_PATH=$(which chromium-browser)

export DOCS_BASEURL="$SERVER/"
rm -fr dist-doc ; sphinx-build -b html ./docs dist-doc
python -m http.server $PORT --directory dist-doc >/dev/null 2>&1 &
SERVER_PID=$!

./node_modules/.bin/pa11y-ci -s "$SERVER/sitemap.xml"

kill $SERVER_PID || true
