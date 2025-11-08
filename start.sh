#!/bin/bash

# Function to wait for a port using curl
wait_for_port() {
    local host=$1
    local port=$2
    local timeout=${3:-60}
    
    echo "Waiting for $host:$port to be ready..."
    for i in $(seq 1 $timeout); do
        if curl -s --connect-timeout 1 "http://$host:$port" > /dev/null 2>&1; then
            echo "$host:$port is ready!"
            return 0
        fi
        sleep 1
    done
    echo "ERROR: Timeout waiting for $host:$port after ${timeout}s"
    return 1
}

# Make scripts executable
chmod 600 .netrc 2>/dev/null || true
cp .netrc /root/.netrc 2>/dev/null || true
chmod +x aria-nox-nzb.sh 2>/dev/null || true

# Execute aria-nox-nzb.sh
echo "Starting aria2c and qBittorrent services..."
./aria-nox-nzb.sh

# Wait for services
echo "Waiting for services to start..."
wait_for_port localhost 6800 60
wait_for_port localhost 8090 60

# Start web server if needed
if [ -n "${BASE_URL}" ]; then
    PORT=${BASE_URL_PORT:-${PORT:-80}}
    echo "Starting web server on port ${PORT}..."
    gunicorn -k uvicorn.workers.UvicornWorker -w 1 web.wserver:app \
             --bind 0.0.0.0:${PORT} --daemon &
    sleep 2
fi

# Start bot
python3 update.py
python3 -m bot
