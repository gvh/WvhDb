#!/usr/bin/env bash
set -euo pipefail

# Create group record
sudo dscl . -create /Groups/wvhdb
sudo dscl . -create /Groups/wvhdb RealName "WvHDb Group"
sudo dscl . -create /Groups/wvhdb PrimaryGroupID 510

# Create user record (no password, no login shell)
sudo dscl . -create /Users/wvhdb

sudo dscl . -create /Users/wvhdb UserShell /usr/bin/false
sudo dscl . -create /Users/wvhdb RealName "WvHDb Service"
sudo dscl . -create /Users/wvhdb UniqueID 510
sudo dscl . -create /Users/wvhdb PrimaryGroupID 510
sudo dscl . -create /Users/wvhdb NFSHomeDirectory /usr/local/var/wvhdb

# Create home and log directories and assign ownership
sudo mkdir -p /usr/local/var/wvhdb /usr/local/var/log/wvhdb
sudo chown -R wvhdb:wvhdb /usr/local/var/wvhdb /usr/local/var/log/wvhdb

# Install and configure the daemon
sudo cp com.gardnervh.wvhdb.plist /Library/LaunchDaemons/
sudo chown root:wheel /Library/LaunchDaemons/com.gardnervh.wvhdb.plist
sudo chmod 644 /Library/LaunchDaemons/com.gardnervh.wvhdb.plist
sudo launchctl unload /Library/LaunchDaemons/com.gardnervh.wvhdb.plist 2>/dev/null || true
sudo launchctl load -w /Library/LaunchDaemons/com.gardnervh.wvhdb.plist
sudo launchctl start com.gardnervh.wvhdb
