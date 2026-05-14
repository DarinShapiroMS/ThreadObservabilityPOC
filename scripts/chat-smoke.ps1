param(
    [Parameter(Mandatory = $true)]
    [string]$BaseUrl,

    [string]$MatrixPath,

    [string]$AgentId,

    [string]$Token,

    [string]$ConversationPrefix = "chat-smoke",

    [switch]$DryRun,

    [switch]$SkipStats
)

$ErrorActionPreference = 'Stop'

$RepoRoot = Split-Path -Parent $PSScriptRoot
if (-not $MatrixPath) {
    $MatrixPath = Join-Path $RepoRoot 'samples/chat/live_smoke_matrix.json'
}

if (-not (Test-Path $MatrixPath)) {
    throw "Matrix file not found: $MatrixPath"
}

function Merge-Hashtable {
    param(
        [hashtable]$Base,
        [hashtable]$Override
    )

    $merged = @{}
    foreach ($key in $Base.Keys) {
        $merged[$key] = $Base[$key]
    }
    foreach ($key in $Override.Keys) {
        $baseValue = $merged[$key]
        $overrideValue = $Override[$key]
        if ($baseValue -is [hashtable] -and $overrideValue -is [hashtable]) {
            $merged[$key] = Merge-Hashtable -Base $baseValue -Override $overrideValue
        }
        else {
            $merged[$key] = $overrideValue
        }
    }
    return $merged
}

function ConvertTo-Hashtable {
    param([object]$Value)

    if ($null -eq $Value) {
        return $null
    }
    if ($Value -is [hashtable]) {
        return $Value
    }
    if ($Value -is [psobject] -and $Value.PSObject.Properties.Count -gt 0 -and -not ($Value -is [string])) {
        $table = @{}
        foreach ($property in $Value.PSObject.Properties) {
            $table[$property.Name] = ConvertTo-Hashtable $property.Value
        }
        return $table
    }
    if ($Value -is [System.Collections.IDictionary]) {
        $table = @{}
        foreach ($key in $Value.Keys) {
            $table[$key] = ConvertTo-Hashtable $Value[$key]
        }
        return $table
    }
    if ($Value -is [System.Collections.IEnumerable] -and -not ($Value -is [string])) {
        $items = @()
        foreach ($item in $Value) {
            $items += ,(ConvertTo-Hashtable $item)
        }
        return $items
    }
    return $Value
}

function Get-ToolNames {
    param([object[]]$ToolCalls)

    $names = @()
    foreach ($toolCall in ($ToolCalls | Where-Object { $_ })) {
        if ($toolCall.name) {
            $names += [string]$toolCall.name
        }
    }
    return $names
}

function Test-ContainsAny {
    param(
        [string]$Text,
        [object[]]$Needles
    )

    foreach ($needle in ($Needles | Where-Object { $_ })) {
        if ($Text.IndexOf([string]$needle, [System.StringComparison]::OrdinalIgnoreCase) -ge 0) {
            return $true
        }
    }
    return $false
}

function Test-ContainsAll {
    param(
        [string]$Text,
        [object[]]$Needles
    )

    foreach ($needle in ($Needles | Where-Object { $_ })) {
        if ($Text.IndexOf([string]$needle, [System.StringComparison]::OrdinalIgnoreCase) -lt 0) {
            return $false
        }
    }
    return $true
}

function Test-ContainsNone {
    param(
        [string]$Text,
        [object[]]$Needles
    )

    foreach ($needle in ($Needles | Where-Object { $_ })) {
        if ($Text.IndexOf([string]$needle, [System.StringComparison]::OrdinalIgnoreCase) -ge 0) {
            return $false
        }
    }
    return $true
}

$matrix = Get-Content -Raw -Path $MatrixPath | ConvertFrom-Json
$defaults = ConvertTo-Hashtable $matrix.defaults
$cases = @($matrix.cases)

if (-not $cases.Count) {
    throw "Matrix does not contain any cases: $MatrixPath"
}

$trimmedBaseUrl = $BaseUrl.TrimEnd('/')
$chatTurnUrl = "$trimmedBaseUrl/v1/chat/turn"
$chatStatsUrl = "$trimmedBaseUrl/v1/chat/stats"

$headers = @{ Accept = 'application/json' }
if ($Token) {
    $headers['Authorization'] = "Bearer $Token"
}

$beforeStats = $null
if (-not $DryRun -and -not $SkipStats) {
    try {
        $beforeStats = Invoke-RestMethod -Method Get -Uri $chatStatsUrl -Headers $headers
    }
    catch {
        Write-Warning "Unable to fetch pre-run chat stats: $($_.Exception.Message)"
    }
}

$results = @()

for ($index = 0; $index -lt $cases.Count; $index += 1) {
    $case = ConvertTo-Hashtable $cases[$index]
    $caseName = if ($case.ContainsKey('name') -and $case['name']) { [string]$case['name'] } else { "case-$index" }
    $message = if ($case.ContainsKey('message') -and $case['message']) { [string]$case['message'] } else { '' }
    if (-not $message) {
        throw "Case '$caseName' is missing message"
    }

    $payload = Merge-Hashtable -Base $defaults -Override $case
    $payload.Remove('name')
    $payload.Remove('expect_all_contains')
    $payload.Remove('expect_any_contains')
    $payload.Remove('forbid_contains')
    $payload.Remove('require_tool_names')
    $payload.Remove('require_any_tool_names')
    $payload.Remove('forbid_tool_names')
    $payload['message'] = $message
    $payload['streaming'] = $false
    if ($AgentId) {
        $payload['agent_id'] = $AgentId
    }
    $payload['conversation_id'] = "$ConversationPrefix-$($index + 1)-$(Get-Date -Format 'yyyyMMddHHmmss')"

    if ($DryRun) {
        $results += [pscustomobject]@{
            Name = $caseName
            Status = 'DRY-RUN'
            ToolCalls = ''
            Response = $message
        }
        continue
    }

    $jsonBody = $payload | ConvertTo-Json -Depth 100
    $requestHeaders = @{}
    foreach ($key in $headers.Keys) {
        $requestHeaders[$key] = $headers[$key]
    }
    $requestHeaders['Content-Type'] = 'application/json'
    $response = Invoke-RestMethod -Method Post -Uri $chatTurnUrl -Headers $requestHeaders -Body $jsonBody
    $responseText = ''
    if ($response -and $response.response -and $response.response.text) {
        $responseText = [string]$response.response.text
    }
    $toolNames = @(Get-ToolNames -ToolCalls $response.tool_calls)

    $failures = @()
    if (-not $responseText.Trim()) {
        $failures += 'empty response text'
    }
    if ($case.expect_all_contains -and -not (Test-ContainsAll -Text $responseText -Needles $case.expect_all_contains)) {
        $failures += 'missing one or more required phrases'
    }
    if ($case.expect_any_contains -and -not (Test-ContainsAny -Text $responseText -Needles $case.expect_any_contains)) {
        $failures += 'missing any expected phrase'
    }
    if ($case.forbid_contains -and -not (Test-ContainsNone -Text $responseText -Needles $case.forbid_contains)) {
        $failures += 'response contained a forbidden phrase'
    }
    if ($case.require_tool_names) {
        foreach ($requiredTool in @($case.require_tool_names)) {
            if ($toolNames -notcontains [string]$requiredTool) {
                $failures += "missing required tool call: $requiredTool"
            }
        }
    }
    if ($case.require_any_tool_names) {
        $matched = $false
        foreach ($requiredTool in @($case.require_any_tool_names)) {
            if ($toolNames -contains [string]$requiredTool) {
                $matched = $true
                break
            }
        }
        if (-not $matched) {
            $failures += "missing any accepted tool call"
        }
    }
    if ($case.forbid_tool_names) {
        foreach ($forbiddenTool in @($case.forbid_tool_names)) {
            if ($toolNames -contains [string]$forbiddenTool) {
                $failures += "forbidden tool call observed: $forbiddenTool"
            }
        }
    }

    $results += [pscustomobject]@{
        Name = $caseName
        Status = $(if ($failures.Count) { 'FAIL' } else { 'PASS' })
        ToolCalls = ($toolNames -join ', ')
        Response = $responseText
        Failure = ($failures -join '; ')
    }
}

$results | Format-Table -AutoSize | Out-String | Write-Host

if (-not $DryRun -and -not $SkipStats) {
    try {
        $afterStats = Invoke-RestMethod -Method Get -Uri $chatStatsUrl -Headers $headers
        if ($beforeStats -and $afterStats.total_turns -lt ($beforeStats.total_turns + $cases.Count)) {
            throw "chat stats did not advance by at least $($cases.Count) turns"
        }
    }
    catch {
        throw "Smoke run completed, but stats verification failed: $($_.Exception.Message)"
    }
}

$failures = @($results | Where-Object { $_.Status -eq 'FAIL' })
if ($failures.Count) {
    throw "Chat smoke run failed for $($failures.Count) case(s)."
}