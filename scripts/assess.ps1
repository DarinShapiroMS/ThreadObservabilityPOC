param([string]$Tool, [string]$JsonArgs = "{}")
$payload = '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"' + $Tool + '","arguments":' + $JsonArgs + '}}'
$r = Invoke-RestMethod -Method Post -Uri http://192.168.68.90:8100/mcp -ContentType "application/json" -Body $payload
$r.result.content[0].text
