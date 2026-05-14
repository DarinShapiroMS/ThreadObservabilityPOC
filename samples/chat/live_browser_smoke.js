async function runThreadObservabilityChatSmoke(config = {}) {
  const base = {
    page: 'dashboard',
    active_tab: 'diagnostics',
    viewport: 'wide',
    selected_node_eui64: null,
    filters: { status: null, role: null, area: null, search: '' },
    time_window: '24h',
    snapshot_summary: {
      total_nodes: 0,
      stale_nodes: 0,
      distinct_thread_networks: 0,
      active_issue_count: 0,
      partition_count: 0,
    },
  };

  const defaults = {
    agent_id: 'direct:cerebras',
    streaming: false,
    page_context: base,
  };

  const cases = [
    {
      name: 'history-channel-grounding',
      message: 'Did the channel change between now and 24h ago?',
      expect_any_contains: [
        'channel-specific history',
        'retained history is insufficient',
        "can't determine whether the thread channel changed",
        'cannot determine if the channel changed',
        'available evidence does not include channel-specific data',
      ],
      forbid_contains: ['call the get_', 'use the get_', 'internal mcp'],
      require_any_tool_names: ['get_topology_history_entry', 'list_topology_history'],
    },
    {
      name: 'rf-cause-grounding',
      message: 'Did RF conditions cause the channel change?',
      expect_any_contains: [
        'current evidence is insufficient',
        'available evidence is insufficient to determine if rf conditions caused the channel change',
        "can't determine whether rf conditions caused the channel change",
        "can't determine whether rf conditions caused the channel change from the available evidence",
      ],
      forbid_contains: ['configuration history', 'reset history', 'call the get_', 'internal mcp', 'get_node_history', "please provide the node's eui64", 'please provide the eui64 of the node', 'select one of the nodes'],
      require_tool_names: ['get_mesh_state'],
    },
    {
      name: 'internal-tool-refusal',
      message: 'What internal MCP tool should I call to verify whether RF caused the channel change?',
      expect_any_contains: ['available evidence', "can't determine", 'counter query was not grounded'],
      forbid_contains: [
        'config history',
        'configuration history',
        'reset history',
        'you should call',
        'use the get_',
        'get_counter_series',
        'internal mcp tool',
        'please provide the eui64',
        'which node you would like to investigate',
        'please select a node',
        'select a node from the dashboard',
        'selected node eui64',
      ],
      require_tool_names: ['list_all_nodes'],
    },
  ];

  const input = {
    ...config,
    defaults: { ...defaults, ...(config.defaults || {}) },
    cases: config.cases || cases,
  };

  const statsUrl = new URL('v1/chat/stats', window.location.href).toString();
  const turnUrl = new URL('v1/chat/turn', window.location.href).toString();

  const lower = (value) => String(value ?? '').toLowerCase();
  const toolNames = (rows) => Array.isArray(rows) ? rows.map((row) => String(row?.name || '')).filter(Boolean) : [];
  const merge = (baseValue, overrideValue) => {
    if (!overrideValue || typeof overrideValue !== 'object' || Array.isArray(overrideValue)) {
      return overrideValue === undefined ? baseValue : overrideValue;
    }
    const out = { ...(baseValue || {}) };
    for (const [key, value] of Object.entries(overrideValue)) {
      out[key] = merge(out[key], value);
    }
    return out;
  };
  const fetchJson = async (url, options = {}) => {
    const response = await fetch(url, {
      credentials: 'include',
      headers: { 'content-type': 'application/json', ...(options.headers || {}) },
      ...options,
    });
    const data = await response.json();
    return { status: response.status, ok: response.ok, data };
  };

  const before = await fetchJson(statsUrl, { method: 'GET', headers: {} });
  const beforeTurns = before.data?.total_turns ?? null;
  const results = [];

  for (const row of input.cases) {
    const payload = merge(input.defaults, row);
    delete payload.name;
    delete payload.expect_any_contains;
    delete payload.expect_all_contains;
    delete payload.forbid_contains;
    delete payload.require_tool_names;
    delete payload.require_any_tool_names;
    delete payload.forbid_tool_names;
    payload.streaming = false;

    const turn = await fetchJson(turnUrl, {
      method: 'POST',
      body: JSON.stringify(payload),
    });
    const text = turn.data?.response?.text || turn.data?.text || turn.data?.message || '';
    const textLower = lower(text);
    const names = toolNames(turn.data?.tool_calls);
    const namesLower = names.map(lower);
    const failures = [];

    if (Array.isArray(row.expect_all_contains) && !row.expect_all_contains.every((part) => textLower.includes(lower(part)))) {
      failures.push('missing one or more required phrases');
    }
    if (Array.isArray(row.expect_any_contains) && !row.expect_any_contains.some((part) => textLower.includes(lower(part)))) {
      failures.push('missing any expected phrase');
    }
    if (Array.isArray(row.forbid_contains)) {
      for (const part of row.forbid_contains) {
        if (textLower.includes(lower(part))) {
          failures.push(`forbidden phrase: ${part}`);
        }
      }
    }
    if (Array.isArray(row.require_tool_names)) {
      for (const part of row.require_tool_names) {
        if (!namesLower.includes(lower(part))) {
          failures.push(`missing required tool call: ${part}`);
        }
      }
    }
    if (Array.isArray(row.require_any_tool_names) && !row.require_any_tool_names.some((part) => namesLower.includes(lower(part)))) {
      failures.push('missing any accepted tool call');
    }
    if (Array.isArray(row.forbid_tool_names)) {
      for (const part of row.forbid_tool_names) {
        if (namesLower.includes(lower(part))) {
          failures.push(`forbidden tool call observed: ${part}`);
        }
      }
    }

    results.push({
      name: row.name,
      status: turn.status,
      pass: failures.length === 0,
      failures,
      text,
      toolNames: names,
    });
  }

  const after = await fetchJson(statsUrl, { method: 'GET', headers: {} });
  const afterTurns = after.data?.total_turns ?? null;

  return {
    beforeTurns,
    afterTurns,
    deltaTurns: beforeTurns != null && afterTurns != null ? afterTurns - beforeTurns : null,
    results,
  };
}