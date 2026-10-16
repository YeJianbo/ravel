#!/usr/bin/env bash
set -euo pipefail

binary=/usr/local/ravel/ravel
case "${1:-help}" in
    version|config|server)
        exec "$binary" "$@"
        ;;
    start|stop|restart|status|enable|disable)
        action=$1
        if command -v rc-service >/dev/null && [[ -d /run/openrc ]]; then
            case "$action" in
                enable) exec rc-update add ravel default ;;
                disable) exec rc-update del ravel default ;;
                *) exec rc-service ravel "$action" ;;
            esac
        fi
        exec systemctl "$action" ravel.service
        ;;
    log)
        if [[ -d /run/openrc ]]; then exec tail -n 100 -f /var/log/ravel.log; fi
        exec journalctl -u ravel.service -n 100 -f
        ;;
    *)
        printf '%s\n' 'Ravel: start | stop | restart | status | enable | disable | log | version | config | server'
        printf '%s\n' 'Managed runtime: /usr/local/ravel/ravel run'
        ;;
esac
