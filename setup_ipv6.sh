#!/bin/bash
set -euo pipefail

API_KEY="${1:-${API_KEY:-changeme}}"
GITHUB_REPO="${2:-${GITHUB_REPO:-https://github.com/Stock-Alpha/fastapi-worker.git}}"
APP_DIR="/home/admin/fastapi-worker"

echo ">>> Installing system packages..."
apt update -qq
apt install -y -qq python3 python3-venv python3-pip git sudo curl > /dev/null 2>&1

echo ">>> Creating admin user..."
id admin &>/dev/null || useradd -m -s /bin/bash admin

echo ">>> Cloning repo..."
if [ -d "$APP_DIR" ]; then
    cd "$APP_DIR"
    git pull origin main
else
    git clone "$GITHUB_REPO" "$APP_DIR"
fi
chown -R admin:admin "$APP_DIR"

echo ">>> Setting up Python venv and deps..."
su - admin -c "
    cd $APP_DIR
    python3 -m venv venv
    source venv/bin/activate
    pip install -q -r requirements.txt uvloop
"

echo ">>> Writing .env..."
cat > "$APP_DIR/.env" <<EOF
API_KEY=$API_KEY
BROKER_SNAPSHOT_MIN_INTERVAL_SEC=5
CONTROL_API_BASE_URL=http://127.0.0.1:8000
WORKER_ORDERS_SNAPSHOT=1
EOF
chown admin:admin "$APP_DIR/.env"
chmod 600 "$APP_DIR/.env"

echo ">>> Installing systemd service..."
cat > /etc/systemd/system/fastapi.service <<EOF
[Unit]
Description=FastAPI Worker
After=network.target

[Service]
User=admin
WorkingDirectory=$APP_DIR
Environment="PATH=$APP_DIR/venv/bin"
ExecStart=$APP_DIR/venv/bin/uvicorn app.main:app \\
  --host :: \\
  --port 8000 \\
  --workers 1 \\
  --loop uvloop \\
  --http h11 \\
  --no-access-log
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable fastapi
systemctl restart fastapi

echo ">>> Setting up sudoers for admin..."
cat > /etc/sudoers.d/fastapi <<EOF
admin ALL=(ALL) NOPASSWD: /bin/systemctl restart fastapi
admin ALL=(ALL) NOPASSWD: /bin/systemctl stop fastapi
admin ALL=(ALL) NOPASSWD: /bin/systemctl start fastapi
admin ALL=(ALL) NOPASSWD: /bin/systemctl kill *
EOF
chmod 440 /etc/sudoers.d/fastapi

echo ">>> Detecting IPv6..."
API_IPV6=$(ip -6 addr show scope global | grep inet6 | awk '{print $2}' | cut -d/ -f1 | head -n1)

sleep 2
systemctl is-active fastapi && echo "FastAPI is running!" || echo "ERROR: FastAPI failed to start"

if [ -n "$API_IPV6" ]; then
    echo ">>> API available at:"
    echo "http://[$API_IPV6]:8000/docs"
else
    echo ">>> WARNING: No IPv6 found"
fi
