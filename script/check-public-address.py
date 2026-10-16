"""Read-only, signed dual-stack diagnosis using the host's existing agent state."""
import hashlib
import hmac
import http.client
import ipaddress
import json
import secrets
import socket
import ssl
import time
from urllib.parse import urlsplit

with open('/etc/.buncloud-agent/state.json', encoding='utf-8') as source:
    state = json.load(source)
panel = urlsplit(state['panel_url'])
if panel.scheme != 'https' or panel.username or panel.query or panel.fragment:
    raise ValueError('An HTTPS panel URL without credentials is required')
path = panel.path.rstrip('/') + '/api/v1/server/machine/address'
result = {}
for family, af in [('4', socket.AF_INET), ('6', socket.AF_INET6)]:
    connection = http.client.HTTPSConnection(panel.hostname, panel.port or 443, timeout=6)
    try:
        # Pin the socket family; do not use a proxy or fall back to the other family.
        addresses = socket.getaddrinfo(panel.hostname, panel.port or 443, af, socket.SOCK_STREAM)
        sock = socket.socket(af, socket.SOCK_STREAM)
        sock.settimeout(6)
        try:
            sock.connect(addresses[0][4])
            connection.sock = ssl.create_default_context().wrap_socket(sock, server_hostname=panel.hostname)
        except Exception:
            sock.close()
            raise
        timestamp, nonce = str(int(time.time())), secrets.token_hex(16)
        payload = '\n'.join(['GET', path, '', timestamp, nonce, hashlib.sha256(b'').hexdigest()])
        signature = hmac.new(state['machine_token'].encode(), payload.encode(), hashlib.sha256).hexdigest()
        connection.request('GET', path, headers={
            'X-Ravel-Machine-Id': str(state['machine_id']),
            'X-Ravel-Timestamp': timestamp, 'X-Ravel-Nonce': nonce,
            'X-Ravel-Signature': signature, 'Accept': 'application/json',
        })
        response = connection.getresponse()
        if response.status != 200:
            raise ValueError('Panel returned HTTP ' + str(response.status))
        address = json.loads(response.read(8192))['data']['ip']
        if ipaddress.ip_address(address).version != int(family):
            raise ValueError('Panel returned a different address family')
        result['ipv' + family] = address
    except Exception as error:
        result['ipv' + family + '_error'] = str(error)
    finally:
        connection.close()
print(json.dumps(result, ensure_ascii=False, indent=2))
