#!/usr/bin/env bash
set -euo pipefail

APP_NAME="pexip-event-sink"
INSTALL_DIR="/opt/${APP_NAME}"
APP_DIR="${INSTALL_DIR}/app"
ENV_DIR="/etc/${APP_NAME}"
ENV_FILE="${ENV_DIR}/${APP_NAME}.env"
DATA_DIR="/var/lib/${APP_NAME}"
LOG_FILE="/var/log/${APP_NAME}.log"
SERVICE_FILE="/etc/systemd/system/${APP_NAME}.service"
PORT="5050"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ "${EUID}" -ne 0 ]]; then
  echo "Please run as root: sudo ./install.sh"
  exit 1
fi

prompt_default() {
  local prompt="$1" default="$2" value
  read -r -p "${prompt} [${default}]: " value
  echo "${value:-$default}"
}

prompt_required() {
  local prompt="$1" value=""
  while [[ -z "$value" ]]; do
    read -r -p "${prompt}: " value
  done
  echo "$value"
}

prompt_secret() {
  local prompt="$1" value=""
  while [[ -z "$value" ]]; do
    read -r -s -p "${prompt}: " value
    echo >&2
  done
  echo "$value"
}

yes_no() {
  local prompt="$1" default="$2" value
  read -r -p "${prompt} [${default}]: " value
  value="${value:-$default}"
  [[ "$value" =~ ^[Yy] ]]
}

escape_sed() { printf '%s' "$1" | sed 's/[&/]/\\&/g'; }

install_packages_apt() {
  apt-get update
  DEBIAN_FRONTEND=noninteractive apt-get install -y \
    python3 python3-venv python3-pip apache2 openssl ca-certificates curl
  a2enmod proxy proxy_http headers rewrite ssl >/dev/null
}

install_packages_dnf() {
  dnf install -y python3 python3-pip python3-virtualenv httpd mod_ssl openssl ca-certificates curl || \
  yum install -y python3 python3-pip python3-virtualenv httpd mod_ssl openssl ca-certificates curl
}

configure_apache_conf() {
  local https_mode="$1" server_name="$2" cert_email="$3"

  if command -v apt-get >/dev/null 2>&1; then
    APACHE_SERVICE="apache2"
    APACHE_CONF_DIR="/etc/apache2/conf-available"
    APACHE_SITE_DIR="/etc/apache2/sites-available"
    APACHE_CONF_TARGET="${APACHE_CONF_DIR}/${APP_NAME}.conf"
    APACHE_SITE_TARGET="${APACHE_SITE_DIR}/${APP_NAME}.conf"
  else
    APACHE_SERVICE="httpd"
    APACHE_CONF_DIR="/etc/httpd/conf.d"
    APACHE_CONF_TARGET="${APACHE_CONF_DIR}/${APP_NAME}.conf"
    APACHE_SITE_TARGET="${APACHE_CONF_TARGET}"
  fi

  rm -f /etc/apache2/conf-enabled/${APP_NAME}.conf /etc/apache2/conf-available/${APP_NAME}.conf \
        /etc/apache2/sites-enabled/${APP_NAME}.conf /etc/apache2/sites-available/${APP_NAME}.conf \
        /etc/httpd/conf.d/${APP_NAME}.conf 2>/dev/null || true

  if [[ "$https_mode" == "letsencrypt" ]]; then
    if [[ -z "$server_name" ]]; then
      echo "Let's Encrypt requires a public DNS name/FQDN."
      exit 1
    fi
    sed "s/__SERVER_NAME__/$(escape_sed "$server_name")/g" \
      "${SCRIPT_DIR}/${APP_NAME}-apache-vhost.conf.template" > "$APACHE_SITE_TARGET"
    if command -v a2ensite >/dev/null 2>&1; then
      a2ensite "${APP_NAME}.conf" >/dev/null
    fi
    systemctl restart "$APACHE_SERVICE"

    if command -v apt-get >/dev/null 2>&1; then
      DEBIAN_FRONTEND=noninteractive apt-get install -y certbot python3-certbot-apache
    else
      (dnf install -y certbot python3-certbot-apache || yum install -y certbot python3-certbot-apache) || true
    fi

    echo "Requesting Let's Encrypt certificate for ${server_name}..."
    if [[ -n "$cert_email" ]]; then
      certbot --apache -d "$server_name" --non-interactive --agree-tos -m "$cert_email" --redirect
    else
      certbot --apache -d "$server_name" --non-interactive --agree-tos --register-unsafely-without-email --redirect
    fi
  elif [[ "$https_mode" == "selfsigned" ]]; then
    local cert_dir="/etc/ssl/${APP_NAME}"
    local cert_file="${cert_dir}/${APP_NAME}.crt"
    local key_file="${cert_dir}/${APP_NAME}.key"
    mkdir -p "$cert_dir"
    chmod 700 "$cert_dir"
    if [[ -z "$server_name" ]]; then
      server_name="$(hostname -f 2>/dev/null || hostname)"
    fi
    openssl req -x509 -nodes -days 825 -newkey rsa:2048 \
      -keyout "$key_file" -out "$cert_file" -subj "/CN=${server_name}" >/dev/null 2>&1
    chmod 600 "$key_file"
    sed -e "s/__SERVER_NAME__/$(escape_sed "$server_name")/g" \
        -e "s/__CERT_FILE__/$(escape_sed "$cert_file")/g" \
        -e "s/__KEY_FILE__/$(escape_sed "$key_file")/g" \
      "${SCRIPT_DIR}/${APP_NAME}-apache-ssl-selfsigned.conf.template" > "$APACHE_SITE_TARGET"
    if command -v a2ensite >/dev/null 2>&1; then
      a2ensite "${APP_NAME}.conf" >/dev/null
    fi
  else
    cp "${SCRIPT_DIR}/${APP_NAME}-apache-http.conf" "$APACHE_CONF_TARGET"
    if command -v a2enconf >/dev/null 2>&1; then
      a2enconf "$APP_NAME" >/dev/null
    fi
  fi
}

echo "=== Pexip Event Sink installer ==="
echo
PEXIP_MGMT_HOST=$(prompt_required "Pexip Management Node FQDN/IP")
PEXIP_MGMT_USER=$(prompt_default "Pexip Management username" "admin")
PEXIP_MGMT_PASS=$(prompt_secret "Pexip Management password")
VERIFY_TLS=$(prompt_default "Verify Pexip Management TLS certificate? true/false" "false")

if command -v apt-get >/dev/null 2>&1; then
  install_packages_apt
elif command -v dnf >/dev/null 2>&1 || command -v yum >/dev/null 2>&1; then
  install_packages_dnf
else
  echo "Unsupported Linux package manager. Install python3, venv/pip, Apache, and OpenSSL first."
  exit 1
fi

PEXIP_MGMT_BASE_URL="https://${PEXIP_MGMT_HOST}"

discover_system_locations() {
  local curl_tls_args=()
  if [[ "${VERIFY_TLS,,}" != "true" && "${VERIFY_TLS,,}" != "yes" && "${VERIFY_TLS}" != "1" ]]; then
    curl_tls_args=(-k)
  fi

  local api_url="${PEXIP_MGMT_BASE_URL}/api/admin/configuration/v1/system_location/"
  local tmp_json
  tmp_json=$(mktemp)

  echo
  echo "Discovering Pexip system locations from ${api_url} ..."
  if ! curl -sS "${curl_tls_args[@]}" -u "${PEXIP_MGMT_USER}:${PEXIP_MGMT_PASS}" "$api_url" -o "$tmp_json"; then
    rm -f "$tmp_json"
    echo "Unable to connect to Pexip Management API for location discovery."
    return 1
  fi

  python3 - "$tmp_json" <<'PYDISCOVER'
import json, re, sys
path = sys.argv[1]
try:
    data = json.load(open(path))
except Exception as e:
    print(f"Unable to parse API response as JSON: {e}", file=sys.stderr)
    sys.exit(2)
objs = data.get('objects', data if isinstance(data, list) else [])
if not isinstance(objs, list) or not objs:
    print("No system locations returned by the API.", file=sys.stderr)
    sys.exit(3)
for loc in objs:
    name = loc.get('name') or '(unnamed)'
    rid = loc.get('id')
    if rid is None:
        uri = loc.get('resource_uri', '')
        m = re.search(r'/system_location/(\d+)/', uri)
        rid = m.group(1) if m else '?'
    print(f"[{rid}] {name}")
PYDISCOVER
  local rc=$?
  rm -f "$tmp_json"
  return $rc
}

if ! discover_system_locations; then
  echo
  echo "Location discovery failed. You can still enter the location names manually."
fi

POLICY_LOCATION_NAMES=$(prompt_default "System Location names to manage for Teams DR policy toggle, comma-separated" "NC-LOC,NC-EDGE-LOC")

echo
HTTPS_MODE="http"
SERVER_NAME=""
CERT_EMAIL=""
if yes_no "Enable HTTPS for the dashboard?" "yes"; then
  SERVER_NAME=$(prompt_required "Public DNS name/FQDN for this server, for example eventsink.company.com")
  if yes_no "Use Let's Encrypt trusted certificate? DNS must already point to this server and ports 80/443 must be open" "yes"; then
    HTTPS_MODE="letsencrypt"
    CERT_EMAIL=$(prompt_default "Let's Encrypt notification email, blank allowed" "")
  else
    HTTPS_MODE="selfsigned"
    echo "Using a self-signed certificate. Browsers and Pexip may not trust this until the cert is trusted/imported."
  fi
fi

# Location IDs are intentionally not stored. The app resolves location names to
# resource URIs dynamically at runtime so this installer is portable between
# Pexip Infinity environments where numeric IDs may differ.

mkdir -p "$APP_DIR" "$ENV_DIR" "$DATA_DIR"
rsync -a --delete "${SCRIPT_DIR}/app/" "$APP_DIR/" 2>/dev/null || { rm -rf "$APP_DIR"/*; cp -a "${SCRIPT_DIR}/app/." "$APP_DIR/"; }

# Flask resolves templates/static relative to app.py, so copy these into the app directory.
mkdir -p "$APP_DIR/templates" "$APP_DIR/static"
rsync -a --delete "${SCRIPT_DIR}/templates/" "$APP_DIR/templates/" 2>/dev/null || cp -a "${SCRIPT_DIR}/templates/." "$APP_DIR/templates/"
rsync -a --delete "${SCRIPT_DIR}/static/" "$APP_DIR/static/" 2>/dev/null || cp -a "${SCRIPT_DIR}/static/." "$APP_DIR/static/"

cp "${SCRIPT_DIR}/requirements.txt" "$INSTALL_DIR/requirements.txt"

python3 -m venv "$INSTALL_DIR/venv"
"$INSTALL_DIR/venv/bin/pip" install --upgrade pip
"$INSTALL_DIR/venv/bin/pip" install -r "$INSTALL_DIR/requirements.txt"

cat > "$ENV_FILE" <<ENVEOF
# Pexip Event Sink environment
PEXIP_MGMT_BASE_URL=${PEXIP_MGMT_BASE_URL}
PEXIP_MGMT_URL=${PEXIP_MGMT_BASE_URL}/api/admin/status/v1/worker_vm/
PEXIP_MGMT_LICENSE_URL=${PEXIP_MGMT_BASE_URL}/api/admin/status/v1/licensing/
PEXIP_MGMT_USER=${PEXIP_MGMT_USER}
PEXIP_MGMT_PASS=${PEXIP_MGMT_PASS}
PEXIP_MGMT_VERIFY_TLS=${VERIFY_TLS}
PEXIP_POLICY_LOCATION_NAMES=${POLICY_LOCATION_NAMES}
PEXIP_DATA_DIR=${DATA_DIR}
PEXIP_LOG_PATH=${LOG_FILE}
PEXIP_ENDED_LIMIT=20
PEXIP_VMLOAD_RETENTION_HOURS=4
PEXIP_VMLOAD_BUCKET_MINUTES=15
ENVEOF
chmod 640 "$ENV_FILE"
chown root:www-data "$ENV_FILE" 2>/dev/null || chown root:apache "$ENV_FILE" 2>/dev/null || true

touch "$LOG_FILE"
chown -R www-data:www-data "$DATA_DIR" "$LOG_FILE" "$INSTALL_DIR" 2>/dev/null || chown -R apache:apache "$DATA_DIR" "$LOG_FILE" "$INSTALL_DIR" || true
chmod 750 "$DATA_DIR"

cp "${SCRIPT_DIR}/${APP_NAME}.service" "$SERVICE_FILE"
if ! id www-data >/dev/null 2>&1 && id apache >/dev/null 2>&1; then
  sed -i 's/^User=www-data/User=apache/; s/^Group=www-data/Group=apache/' "$SERVICE_FILE"
fi

configure_apache_conf "$HTTPS_MODE" "$SERVER_NAME" "$CERT_EMAIL"

systemctl daemon-reload
systemctl enable --now "$APP_NAME"
systemctl restart "${APACHE_SERVICE:-apache2}"

SERVER_IP=$(hostname -I 2>/dev/null | awk '{print $1}')
if [[ "$HTTPS_MODE" == "http" ]]; then
  BASE_URL="http://${SERVER_IP:-SERVER_IP}"
else
  BASE_URL="https://${SERVER_NAME:-${SERVER_IP:-SERVER_IP}}"
fi

echo
echo "Install complete."
echo "Dashboard: ${BASE_URL}/pexip-sink/"
echo "Health:    ${BASE_URL}/pexip-sink/health"
echo "Pexip Event Sink POST URL: ${BASE_URL}/pexip-sink/event_sink"
echo
echo "Useful commands:"
echo "  sudo systemctl status ${APP_NAME}"
echo "  sudo journalctl -u ${APP_NAME} -f"
echo "  sudo tail -f ${LOG_FILE}"
