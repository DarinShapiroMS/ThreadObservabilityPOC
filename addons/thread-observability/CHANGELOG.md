# Changelog

## 0.2.0

- Switched base image from `ghcr.io/home-assistant/{arch}-base:3.20` to `ghcr.io/hassio-addons/base:20.1.1`
- Fixes persistent `s6-overlay-suexec: fatal: can only run as pid 1` crash loop
- Root cause: the low-level HA base image ships a buggy legacy-services compatibility shim; the community addon base (used by ~200 official community addons) is purpose-built for s6-rc.d native services
- Removed `legacy-services` bundle override and empty `services.d/` placeholder (no longer needed)
- Removed `rm -rf /etc/cont-init.d` workaround from Dockerfile
- Dropped `bash` from apk install (provided by base image)

## 0.1.9

- Override base image's buggy legacy-services bundle with a noop s6-rc.d bundle (empty contents.d)
- Prevents HA's s6-overlay from invoking suexec on legacy-services, eliminating the PID 1 crash
- Allows native s6-rc.d core and mcp services to run cleanly without cascade restarts

## 0.1.8

- Keep empty /etc/services.d directory (only delete cont-init.d) so HA legacy-services shim finds it, scans, finds nothing, and exits cleanly
- Prevents suexec fatal crash that cascades into service restarts
- Allows s6-rc.d native services to run uninterrupted after legacy shim completes

## 0.1.7

- Added explicit `rm -rf /etc/cont-init.d /etc/services.d` in Dockerfile to eliminate Docker layer cache issues
- Forces removal of legacy HA s6-overlay compatibility layer directories that cause cascade crashes

## 0.1.6

- Added rotating file logger to /data/thread-observability/addon.log (2 MB, 2 backups)
- Both core and MCP services now log to stdout + file on startup
- get_recent_logs MCP tool now has live data to read
- Log level controlled via THREAD_OBS_LOG_LEVEL env var (default: info)

## 0.1.5

- Implemented MCP JSON-RPC 2.0 protocol endpoint at POST /mcp (VS Code MCP client compatible)
- Added get_recent_logs tool for live log access from IDE
- Added .vscode/mcp.json wired to HA instance at 192.168.68.90:8100
- Reads from /data/thread-observability/addon.log with /run/uncaught-logs/current fallback

## 0.1.4

- Removed cont-init.d entirely to eliminate legacy-cont-init and legacy-services shims
- Moved runtime directory creation to Dockerfile RUN step
- Both legacy s6-overlay shims now have nothing to process, eliminating suexec PID 1 crash

## 0.1.3

- Migrated from legacy services.d to native s6-overlay v3 s6-rc.d service format
- Eliminates s6-overlay-suexec PID 1 fatal crash on service startup

## 0.1.2

- Fixed s6-overlay v3 compatibility by replacing with-contenv shebang with plain bash in all service scripts

## 0.1.1

- Fixed container startup by ensuring s6 scripts are LF-normalized and executable
- Fixed CI build behavior for Home Assistant base image pip install restrictions
- Removed deprecated architecture and cleaned add-on metadata defaults for linting

## 0.1.0

- Initial scaffold for Home Assistant add-on structure
- Added two-process skeleton (core + MCP)
- Added configuration schema and build metadata
