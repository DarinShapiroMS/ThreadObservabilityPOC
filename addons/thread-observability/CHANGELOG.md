# Changelog

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
