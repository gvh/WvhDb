#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'USAGE'
    macOS launchd helper for WvHDb

    Commands:

            install  Copy plist to /Library/LaunchDaemons and load it (sudo)
            uninstall Unload and remove plist from /Library/LaunchDaemons (sudo)
            start    sudo launchctl start com.example.wvhdb
            stop     sudo launchctl stop com.example.wvhdb

    Notes:
        - Ensure binary path and directories exist and have correct permissions.

USAGE
}

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LABEL="com.gardnervh.wvhdb"
DAEMON_PLIST="$ROOT_DIR/Scripts/$LABEL.plist"

install_daemon() {
    sudo cp "$DAEMON_PLIST" "/Library/LaunchDaemons/$LABEL.plist"
    sudo chown root:wheel "/Library/LaunchDaemons/$LABEL.plist"
    sudo chmod 644 "/Library/LaunchDaemons/$LABEL.plist"
    sudo launchctl unload "/Library/LaunchDaemons/$LABEL.plist" 2>/dev/null || true
    sudo launchctl load -w "/Library/LaunchDaemons/$LABEL.plist"
    echo "Installed and loaded LaunchDaemon: $LABEL"
}

uninstall_daemon() {
    sudo launchctl unload "/Library/LaunchDaemons/$LABEL.plist" 2>/dev/null || true
    sudo rm -f "/Library/LaunchDaemons/$LABEL.plist"
    echo "Uninstalled LaunchDaemon: $LABEL"
}

start_daemon() {
    sudo launchctl start "$LABEL"
}

stop_daemon() {
    sudo launchctl stop "$LABEL"
}

case "${1:-}" in
install) install_daemon ;;
uninstall) uninstall_daemon ;;
start) start_daemon ;;
stop) stop_daemon ;;
*) usage; exit 1 ;;
esac
