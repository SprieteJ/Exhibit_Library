async function fetchCategoryOverview(category) {
  const _v = _navVersion;
  spinOn();
  try {
    if (!_ccDataCache) {
      _ccDataCache = await fetch('/api/control-center', {signal: _navAbort?.signal}).then(r=>r.json());
    }
    if (_v !== _navVersion) return;
    const data = _ccDataCache;
    if (!data.charts?.length) throw new Error('no data');

    if (chart) { chart.destroy(); chart = null; }
    const canvas = document.getElementById('main-chart');
    canvas.style.display = 'none';

    let existing = document.getElementById('cc-matrix-container');
    if (existing) existing.remove();

    const container = document.createElement('div');
    container.id = 'cc-matrix-container';
    container.style.cssText = 'position:absolute;inset:0;overflow-y:auto;padding:16px 20px;';

    // Filter to this category
    const charts = data.charts.filter(c => c.category === category);

    if (!charts.length) {
      container.innerHTML = '<div style="display:flex;flex-direction:column;align-items:center;justify-content:center;height:60%;gap:12px"><div style="font-size:48px;opacity:0.2">&#10003;</div><div style="font-size:15px;font-weight:500;color:var(--graphite)">All quiet</div><div style="font-size:13px;color:var(--muted)">No rules configured for ' + category + ' yet.</div></div>';
      canvas.parentElement.appendChild(container);
      setTitle(category + ' — Signal Overview', 'Control center rules for ' + category);
      document.getElementById('chart-spin').classList.remove('on');
      return;
    }

    // Group charts by their chart_name prefix to infer groups
    // Build a group column by detecting chart name patterns
    const GROUP_MAP = {
      'Gap': 'Moving Averages', 'moving average': 'Moving Averages', 'Moving': 'Moving Averages',
      'Deviation': 'Moving Averages', 'deviation': 'Moving Averages',
      'Drawdown': 'Price Performance', 'drawdown': 'Price Performance',
      'Volatility': 'Volatility', 'volatility': 'Volatility', 'DVOL': 'Volatility', 'RV vs': 'Volatility',
      'Funding': 'Derivatives', 'funding': 'Derivatives',
      'Dominance': 'Market Cap', 'dominance': 'Market Cap', 'Market Cap': 'Market Cap', 'market cap': 'Market Cap',
      'ETH/BTC': 'Relative', 'Ratio': 'Relative',
    };
    function inferGroup(chartName) {
      for (const [pattern, group] of Object.entries(GROUP_MAP)) {
        if (chartName.includes(pattern)) return group;
      }
      return 'Other';
    }

    // Sort charts by group
    const groupOrder = ['Price Performance', 'Moving Averages', 'Market Cap', 'Volatility', 'Derivatives', 'Relative', 'Other'];
    charts.sort((a, b) => {
      const ga = groupOrder.indexOf(inferGroup(a.chart_name));
      const gb = groupOrder.indexOf(inferGroup(b.chart_name));
      return (ga === -1 ? 99 : ga) - (gb === -1 ? 99 : gb);
    });

    const thStyle = 'text-align:left;font-size:11px;font-weight:600;color:var(--muted);letter-spacing:.06em;text-transform:uppercase;padding:8px 10px';
    let tableHTML = '<div style="margin-bottom:12px"><span style="font-family:var(--mono);font-size:11px;color:var(--muted)">Last updated: ' + data.updated + '</span></div>';
    tableHTML += '<table style="width:100%;border-collapse:collapse;font-size:13px">';
    tableHTML += '<thead><tr style="border-bottom:1px solid var(--border)">';
    tableHTML += '<th style="' + thStyle + ';width:18%">Chart</th>';
    tableHTML += '<th style="' + thStyle + ';width:18%">What to watch</th>';
    tableHTML += '<th style="' + thStyle + ';width:8%">Type</th>';
    tableHTML += '<th style="' + thStyle + ';width:7%">Weight</th>';
    tableHTML += '<th style="text-align:center;' + thStyle + ';width:5%">Status</th>';
    tableHTML += '<th style="' + thStyle + ';width:22%">Detail</th>';
    tableHTML += '<th style="' + thStyle + ';width:22%">Context</th>';
    tableHTML += '</tr></thead><tbody>';

    let lastGroup = null;
    for (const c of charts) {
      const group = inferGroup(c.chart_name);
      const chartLink = c.chart_key && c.chart_tab
        ? '<a href="#" onclick="selectView(\'' + c.chart_tab + '\',\'' + c.chart_key + '\');return false;" style="color:var(--graphite);text-decoration:none;border-bottom:1px dotted var(--muted);font-weight:500">' + c.chart_name + ' &#8594;</a>'
        : '<span style="color:var(--muted)">' + c.chart_name + '</span>';

      // Group band row
      if (group !== lastGroup) {
        lastGroup = group;
        tableHTML += '<tr><td colspan="7" style="padding:16px 10px 8px;font-size:13px;font-weight:700;color:var(--graphite);letter-spacing:.08em;text-transform:uppercase;text-align:center;border-bottom:1px solid var(--border);background:' + (darkMode ? 'rgba(255,255,255,0.03)' : 'rgba(0,0,0,0.03)') + '">' + group + '</td></tr>';
      }

      for (let ri = 0; ri < c.rules.length; ri++) {
        const r = c.rules[ri];
        const isActive = r.active;
        const bgColor = isActive ? (darkMode ? 'rgba(0,214,74,0.06)' : 'rgba(0,180,60,0.06)') : 'transparent';
        const opacity = isActive ? '1' : '0.4';
        const dot = isActive
          ? '<span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:#00D64A"></span>'
          : '<span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:var(--border)"></span>';
        const typeTag = r.type ? '<span style="font-size:10px;padding:1px 5px;border-radius:3px;background:' + (darkMode ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.05)') + ';color:var(--muted)">' + r.type + '</span>' : '';
        const weightTag = r.weight === 'major'
          ? '<span style="font-size:10px;font-weight:600;color:var(--graphite)">major</span>'
          : '<span style="font-size:10px;color:var(--muted)">minor</span>';

        tableHTML += '<tr style="border-bottom:0.5px solid var(--border);background:' + bgColor + ';opacity:' + opacity + ';transition:opacity 0.15s" onmouseenter="this.style.opacity=\'1\'" onmouseleave="this.style.opacity=\'' + opacity + '\'">';
        tableHTML += '<td style="padding:8px 10px">' + (ri === 0 ? chartLink : '') + '</td>';
        tableHTML += '<td style="padding:8px 10px;font-weight:' + (isActive ? '600' : '400') + '">' + r.name + '</td>';
        tableHTML += '<td style="padding:8px 10px">' + typeTag + '</td>';
        tableHTML += '<td style="padding:8px 10px">' + weightTag + '</td>';
        tableHTML += '<td style="padding:8px 10px;text-align:center">' + dot + '</td>';
        tableHTML += '<td style="padding:8px 10px;font-family:var(--mono);font-size:12px;color:' + (isActive ? 'var(--graphite)' : 'var(--muted)') + '">' + r.detail + '</td>';
        tableHTML += '<td style="padding:8px 10px;font-size:11px;color:var(--muted);font-style:italic">' + (r.context || '') + '</td>';
        tableHTML += '</tr>';
      }
    }

    tableHTML += '</tbody></table>';
    container.innerHTML = tableHTML;
    canvas.parentElement.appendChild(container);
    setTitle(category + ' — Signal Overview', 'Control center rules for ' + category);
    document.getElementById('chart-spin').classList.remove('on');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}





// ── ETF: Total AuM (cumulative flows) ────────────────────────────────────────
