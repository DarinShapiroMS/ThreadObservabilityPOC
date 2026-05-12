$base = "C:\Users\darin_jwxgczt\Documents\ThreadPOC\samples"
$mcp  = Join-Path $base "mcp"
$ha   = Join-Path $base "ha"
$ms   = Join-Path $base "matter_server"
$otbr = Join-Path $base "otbr"
New-Item -ItemType Directory -Force -Path $mcp,$ha,$ms,$otbr | Out-Null

function Save-Mcp {
    param(
        [string]$tool,
        [hashtable]$callArgs = @{},
        [string]$folder = $mcp,
        [string]$fileBase = $null
    )
    $ts = (Get-Date).ToString("o")
    $body = @{
        jsonrpc = "2.0"
        id      = 1
        method  = "tools/call"
        params  = @{ name = $tool; arguments = $callArgs }
    } | ConvertTo-Json -Depth 6 -Compress
    try {
        $resp = Invoke-RestMethod -Method Post -Uri "http://192.168.68.90:8100/mcp" -ContentType "application/json" -Body $body -TimeoutSec 25
        $text = $resp.result.content[0].text
        $argsJson = ($callArgs | ConvertTo-Json -Compress)
        $out = "{`"_captured_at`":`"$ts`",`"_tool`":`"$tool`",`"_args`":$argsJson,`"response`":$text}"
        $name = if ($fileBase) { $fileBase } else { $tool }
        $path = Join-Path $folder ($name + ".json")
        Set-Content -Path $path -Value $out -Encoding utf8
        Write-Host "saved $name"
    } catch {
        Write-Host "FAIL  $tool : $($_.Exception.Message)"
    }
}

# Thread Observability addon MCP tools
Save-Mcp "list_all_nodes"
Save-Mcp "query_events" @{ limit = 20 }
Save-Mcp "get_health_snapshot"
Save-Mcp "get_network_topology"
Save-Mcp "get_storage_stats"
Save-Mcp "get_ingest_state"
Save-Mcp "list_otbr_candidates"
Save-Mcp "get_config"
Save-Mcp "discover_thread_devices"
Save-Mcp "get_recent_logs" @{ limit = 50 }
Save-Mcp "list_active_issues"
Save-Mcp "get_timeseries_health"

# HA Supervisor data (via MCP HA-facing tools), written to samples/ha
Save-Mcp "ha_get_addon_state" -folder $ha
Save-Mcp "ha_get_addon_logs" @{ slug = "9e5048e8_thread-observability"; lines = 200 } -folder $ha -fileBase "ha_get_addon_logs__thread-observability"
Save-Mcp "ha_get_addon_logs" @{ slug = "core_matter_server"; lines = 200 } -folder $ha -fileBase "ha_get_addon_logs__matter_server"
Save-Mcp "ha_get_addon_logs" @{ slug = "core_openthread_border_router"; lines = 200 } -folder $ha -fileBase "ha_get_addon_logs__otbr"

Get-ChildItem -Recurse $base -Filter "*.json" | Select-Object FullName, Length
