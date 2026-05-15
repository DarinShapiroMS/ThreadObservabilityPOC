param(
    [Parameter(Mandatory = $true)]
    [string]$BaseUrl,

    [string]$MatrixPath,

    [string]$AgentId,

    [string]$Token,

    [string]$ConversationPrefix = "chat-smoke",

    [switch]$DryRun,

    [switch]$SkipStats,

    [switch]$SkipTranscript
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

function Get-NestedValue {
    param(
        [object]$Object,
        [string[]]$Path
    )

    $current = $Object
    foreach ($segment in $Path) {
        if ($null -eq $current) {
            return $null
        }
        if ($current -is [System.Collections.IList] -and $segment -match '^\d+$') {
            $index = [int]$segment
            if ($index -lt 0 -or $index -ge $current.Count) {
                return $null
            }
            $current = $current[$index]
            continue
        }
        if ($current -is [hashtable]) {
            if (-not $current.ContainsKey($segment)) {
                return $null
            }
            $current = $current[$segment]
            continue
        }
        if ($current -is [psobject]) {
            $property = $current.PSObject.Properties[$segment]
            if ($null -eq $property) {
                return $null
            }
            $current = $property.Value
            continue
        }
        return $null
    }
    return $current
}

function Get-TranscriptEventKinds {
    param([object]$TranscriptTurn)

    $events = @(Get-NestedValue -Object $TranscriptTurn -Path @('transcript', 'events'))
    $kinds = @()
    foreach ($event in $events) {
        $kind = Get-NestedValue -Object $event -Path @('kind')
        if ($kind) {
            $kinds += [string]$kind
        }
    }
    return $kinds
}

function Get-TranscriptAnswerTexts {
    param([object]$TranscriptTurn)

    $events = @(Get-NestedValue -Object $TranscriptTurn -Path @('transcript', 'events'))
    $texts = @()
    foreach ($event in $events) {
        $kind = [string](Get-NestedValue -Object $event -Path @('kind'))
        if ($kind -ne 'assistant_completion') {
            continue
        }
        $content = Get-NestedValue -Object $event -Path @('response', 'choices', '0', 'message', 'content')
        if ($content -is [string] -and $content.Trim()) {
            $texts += $content.Trim()
        }
    }
    return $texts
}

function Get-TranscriptReviewerText {
    param([object]$TranscriptTurn)

    $events = @(Get-NestedValue -Object $TranscriptTurn -Path @('transcript', 'events'))
    $blocks = @()
    foreach ($event in $events) {
        $kind = [string](Get-NestedValue -Object $event -Path @('kind'))
        if ($kind -notin @('audit_review', 'answer_review')) {
            continue
        }
        $messages = @(Get-NestedValue -Object $event -Path @('request', 'messages'))
        foreach ($message in $messages) {
            $content = Get-NestedValue -Object $message -Path @('content')
            if ($content -is [string] -and $content.Trim()) {
                $blocks += $content.Trim()
            }
        }
    }
    return ($blocks -join "`n`n")
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
$chatTranscriptBaseUrl = "$trimmedBaseUrl/v1/chat/transcript"

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
    $payload.Remove('require_transcript_event_kinds')
    $payload.Remove('expect_initial_any_contains')
    $payload.Remove('forbid_initial_contains')
    $payload.Remove('expect_reviewer_any_contains')
    $payload.Remove('forbid_reviewer_contains')
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
    $transcriptTurn = $null
    $transcriptEventKinds = @()
    $initialAnswers = @()
    $reviewerText = ''

    $failures = @()
    if (-not $SkipTranscript) {
        try {
            $transcriptUrl = "$chatTranscriptBaseUrl/$($payload['conversation_id'])"
            $transcriptResponse = Invoke-RestMethod -Method Get -Uri $transcriptUrl -Headers $headers
            $turns = @(Get-NestedValue -Object $transcriptResponse -Path @('transcript_turns'))
            if ($turns.Count -gt 0) {
                $transcriptTurn = $turns[0]
                $transcriptEventKinds = @(Get-TranscriptEventKinds -TranscriptTurn $transcriptTurn)
                $initialAnswers = @(Get-TranscriptAnswerTexts -TranscriptTurn $transcriptTurn)
                $reviewerText = [string](Get-TranscriptReviewerText -TranscriptTurn $transcriptTurn)
            }
        }
        catch {
            $failures += "transcript fetch failed: $($_.Exception.Message)"
        }
    }
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
    if (-not $SkipTranscript) {
        if (-not $transcriptTurn) {
            $failures += 'missing persisted transcript turn'
        }
        if ($case.require_transcript_event_kinds) {
            foreach ($requiredEventKind in @($case.require_transcript_event_kinds)) {
                if ($transcriptEventKinds -notcontains [string]$requiredEventKind) {
                    $failures += "missing required transcript event: $requiredEventKind"
                }
            }
        }
        $initialAnswerText = if ($initialAnswers.Count) { [string]$initialAnswers[0] } else { '' }
        if ($case.expect_initial_any_contains -and -not (Test-ContainsAny -Text $initialAnswerText -Needles $case.expect_initial_any_contains)) {
            $failures += 'initial answer missing any expected phrase'
        }
        if ($case.forbid_initial_contains -and -not (Test-ContainsNone -Text $initialAnswerText -Needles $case.forbid_initial_contains)) {
            $failures += 'initial answer contained a forbidden phrase'
        }
        if ($case.expect_reviewer_any_contains -and -not (Test-ContainsAny -Text $reviewerText -Needles $case.expect_reviewer_any_contains)) {
            $failures += 'reviewer prompt missing any expected phrase'
        }
        if ($case.forbid_reviewer_contains -and -not (Test-ContainsNone -Text $reviewerText -Needles $case.forbid_reviewer_contains)) {
            $failures += 'reviewer prompt contained a forbidden phrase'
        }
    }

    $results += [pscustomobject]@{
        Name = $caseName
        Status = $(if ($failures.Count) { 'FAIL' } else { 'PASS' })
        ToolCalls = ($toolNames -join ', ')
        TranscriptEvents = ($transcriptEventKinds -join ', ')
        InitialAnswer = $(if ($initialAnswers.Count) { [string]$initialAnswers[0] } else { '' })
        FinalAnswer = $responseText
        ReviewerPrompt = $reviewerText
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