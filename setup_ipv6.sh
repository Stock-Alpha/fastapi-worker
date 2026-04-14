#!/bin/bash
# ─────────────────────────────────────────────────────────────
# FastAPI Worker — EC2 Bootstrap Script
# Target: Debian 12 (t4g.nano). Service binds 0.0.0.0 (IPv4); use :: in fastapi.service if you need IPv6.
#
# Usage:
#   curl -sSL https://raw.githubusercontent.com/YOUR_USER/fastapi-worker/main/setup.sh | sudo bash -s -- <API_KEY> <GITHUB_REPO_URL>
#
# Or pass as EC2 user-data:
#   #!/bin/bash
#   export API_KEY="your_key_here"
#   export GITHUB_REPO="https://github.com/YOUR_USER/fastapi-worker.git"
#   curl -sSL https://raw.githubusercontent.com/YOUR_USER/fastapi-worker/main/setup.sh | bash
# ─────────────────────────────────────────────────────────────

set -euo pipefail

API_KEY="${1:-${API_KEY:-changeme}}"
GITHUB_REPO="${2:-${GITHUB_REPO:-https://github.com/YOUR_USER/fastapi-worker.git}}"
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
    pip install -q -r requirements.txt
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
cp "$APP_DIR/fastapi.service" /etc/systemd/system/fastapi.service
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

echo ">>> Verifying..."
sleep 2
systemctl is-active fastapi && echo "FastAPI is running!" || echo "ERROR: FastAPI failed to start"

API_IP="$(hostname -I 2>/dev/null | awk '{print $1}')"
echo ">>> Done. API available at http://${API_IP:-<this-host-ip>}:8000"
