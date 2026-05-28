# Pexip Event Sink Dashboard Installer

<img width="1061" height="1182" alt="image" src="https://github.com/user-attachments/assets/a1e7c614-b753-4ddb-beb9-b364e6f4628c" />

<img width="1062" height="1340" alt="image" src="https://github.com/user-attachments/assets/415aef59-f0b0-4cc4-9ce8-0564f9b9285c" />

This package installs the Flask-based Pexip Event Sink dashboard on a Linux server using:

- Apache as the public web server
- Gunicorn as the local Python application service
- SQLite for event storage
- Optional HTTPS using Let's Encrypt or a self-signed certificate

The package is distributed as a `.tar.gz` so Linux execute permissions are preserved.

## Recommended Azure / firewall ports

Allow inbound:

- TCP 22 for SSH, preferably from your source IP only
- TCP 80 for HTTP and Let's Encrypt validation
- TCP 443 for HTTPS

Do **not** expose port `5050`. Gunicorn listens only on `127.0.0.1:5050`.

## Extract the installer

Copy the `.tar.gz` file to the Ubuntu server, then run:

```bash
tar -xzf pexip-event-sink-installer.tar.gz
cd pexip-event-sink-installer
```

## Clean removal of a previous install

From the extracted installer directory, run:

```bash
sudo ./uninstall.sh --purge
```

If the previous installer directory is not available, run this manual cleanup:

```bash
sudo systemctl disable --now pexip-event-sink 2>/dev/null || true
sudo rm -f /etc/systemd/system/pexip-event-sink.service
sudo systemctl daemon-reload

sudo rm -f /etc/apache2/conf-enabled/pexip-event-sink.conf
sudo rm -f /etc/apache2/conf-available/pexip-event-sink.conf
sudo rm -f /etc/apache2/sites-enabled/pexip-event-sink.conf
sudo rm -f /etc/apache2/sites-available/pexip-event-sink.conf
sudo rm -f /etc/httpd/conf.d/pexip-event-sink.conf

sudo rm -rf /opt/pexip-event-sink
sudo rm -rf /etc/pexip-event-sink
sudo rm -rf /var/lib/pexip-event-sink
sudo rm -f /var/log/pexip-event-sink.log
sudo rm -rf /etc/ssl/pexip-event-sink

sudo systemctl restart apache2 2>/dev/null || sudo systemctl restart httpd 2>/dev/null || true
```

This removes the service, Apache config, app files, env file, SQLite database, logs, and self-signed certificates created by this installer.

## Install

```bash
sudo ./install.sh
```

Because this is a `.tar.gz` package, `install.sh` and `uninstall.sh` should already be executable. If your transfer method strips permissions, use:

```bash
sudo bash install.sh
```

## Installer prompts

The installer will ask for:

- Pexip Management Node FQDN/IP
- Pexip Management username
- Pexip Management password
- Whether to verify the Pexip Management TLS certificate
- The installer discovers available Pexip System Locations from the Management API
- System Location names to manage for the Teams DR toggle, default `NC-LOC,NC-EDGE-LOC`
- Whether to enable HTTPS
- Public FQDN for the dashboard
- Whether to use Let's Encrypt or self-signed HTTPS

## Policy control behavior

The Teams DR policy control uses **location names**, not location IDs. During install, it queries the Pexip Management API and displays the discovered System Locations, for example:

```text
[1] NC-LOC
[2] NC-EDGE-LOC
[5] TX-EDGE-LOC
[7] DR-LOC
```

When prompted, enter the location names you want the dashboard to manage, comma-separated:

```text
NC-LOC,NC-EDGE-LOC
```

You can enter more than two locations. The dashboard resolves the names to the correct Pexip resource URIs at runtime, so the package is portable across environments where numeric IDs may be different.

Only configured names are evaluated or changed. Other locations, such as `TX-EDGE-LOC`, are ignored unless you include them. This prevents unrelated locations from forcing the dashboard into a false `Mixed` state.

The installer writes this value to:

```text
/etc/pexip-event-sink/pexip-event-sink.env
```

You can update them later:

```bash
sudo nano /etc/pexip-event-sink/pexip-event-sink.env
```

Example with two locations:

```bash
PEXIP_POLICY_LOCATION_NAMES=NC-LOC,NC-EDGE-LOC
```

Example with more than two locations:

```bash
PEXIP_POLICY_LOCATION_NAMES=NC-LOC,NC-EDGE-LOC,DR-LOC
```

Do not add numeric IDs. Then restart:

```bash
sudo systemctl restart pexip-event-sink
```

## HTTPS with Let's Encrypt

Before running the installer:

1. Create a DNS A record pointing your FQDN to the server public IP.
2. Open TCP 80 and TCP 443 inbound.
3. Make sure Apache can answer on port 80 for certificate validation.

Example final URLs:

```text
https://eventsink.company.com/pexip-sink/
https://eventsink.company.com/pexip-sink/event_sink
```

## Self-signed HTTPS

The installer can create a self-signed certificate under:

```text
/etc/ssl/pexip-event-sink/
```

Browsers will warn until the certificate is trusted. Pexip may reject the event sink endpoint unless the certificate is trusted by the Pexip deployment.

## Useful commands

```bash
sudo systemctl status pexip-event-sink
sudo journalctl -u pexip-event-sink -f
sudo tail -f /var/log/pexip-event-sink.log
curl http://127.0.0.1:5050/pexip-sink/health
```

## Installed paths

```text
/opt/pexip-event-sink/                       Application files and Python venv
/etc/pexip-event-sink/pexip-event-sink.env   Environment file with Pexip settings
/var/lib/pexip-event-sink/                   SQLite database
/var/log/pexip-event-sink.log                Application log
```

## Pexip Event Sink URL

Use this URL in Pexip Infinity:

```text
https://YOUR-FQDN/pexip-sink/event_sink
```

For HTTP-only lab installs:

```text
http://YOUR-SERVER/pexip-sink/event_sink
```
