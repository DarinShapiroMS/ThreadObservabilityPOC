# ThreadPOC

ThreadPOC is a Home Assistant-focused Thread observability platform.

The active product lives in the Home Assistant add-on at [addons/thread-observability](addons/thread-observability). It currently ships a live ingress dashboard, MCP/API surface, direct chat integration, background assessment plumbing, and the supporting storage/pipeline code used by the add-on.

## Deployment Strategy

This repository is primarily a Home Assistant add-on repository.

### Recommended (v1): Home Assistant Add-on Repository

Use this when you want a packaged service with its own container/runtime.

1. In Home Assistant, go to Settings -> Add-ons -> Add-on Store.
2. Open the three-dot menu and choose Repositories.
3. Add this repository URL.
4. Install the Thread Observability add-on.

### HACS Considerations

HACS is excellent for custom integrations, frontend cards, and themes.
It is not the primary mechanism for installing Docker-based add-ons.

Recommended split:
- Add-on repository: backend services (ingestion, enrichment, MCP/API, storage orchestration).
- HACS (optional later): companion UI card/integration for richer Lovelace experience.

## Repository Layout

- addons/thread-observability: Home Assistant add-on package
- documentation: architecture and product documentation
- samples: captured fixtures and example payloads for development/debugging; see [samples/README.md](samples/README.md)
- scripts: local developer helpers and repo automation; see [scripts/README.md](scripts/README.md)

## Current Status

- Active add-on implementation with a shipping Home Assistant ingress UI.
- Current runtime/version details live in [addons/thread-observability/README.md](addons/thread-observability/README.md) and [documentation/README.md](documentation/README.md).
- Deterministic issue definitions are intentionally paused pending the redesign tracked in GitHub issue #5; the current runtime still exposes health, topology, assessment, and chat surfaces.
- The remaining backlog is grouped in [documentation/08-work-buckets.md](documentation/08-work-buckets.md).

## Releases and Updates

Home Assistant detects add-on updates from the add-on version in addons/thread-observability/config.yaml.

For each add-on update:
1. Bump version in addons/thread-observability/config.yaml.
2. Add a matching entry in addons/thread-observability/CHANGELOG.md.
3. Commit and push to your repository.

Version guard CI:
- Pull requests that modify addons/thread-observability require a version bump.
- Workflow: .github/workflows/addon-version-guard.yml

Add-on CI:
- Lints add-on metadata and builds amd64 image.
- Workflow: .github/workflows/addon-ci.yml

Copilot cloud agent setup:
- Provisions the Copilot cloud agent environment and validates setup workflow changes.
- Workflow: .github/workflows/copilot-setup-steps.yml
