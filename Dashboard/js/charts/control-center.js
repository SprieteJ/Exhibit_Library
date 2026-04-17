async function fetchControlCenter(mode) {
  const _v = _navVersion;
  spinOn();
  try {
    const data = await fetch('/api/control-center', {signal: _navAbort?.signal}).then(r=>r.json());
    if (_v !== _navVersion) return; // navigated away
    if (!data.charts?.length) throw new Error('no data');

    if (chart) { chart.destroy(); chart = null; }
    const canvas = document.getElementById('main-chart');
    canvas.style.display = 'none';

    let existing = document.getElementById('cc-matrix-container');
    if (existing) existing.remove();

    const container = document.createElement('div');
    container.id = 'cc-matrix-container';
    container.style.cssText = 'position:absolute;inset:0;overflow-y:auto;padding:16px 20px;';

    // Filter based on mode
    let charts = data.charts;
    let title = 'Control Center';
    let subtitle = 'Signal monitor — attention routing layer';

    if (mode === 'flagged') {
      charts = charts.map(c => ({...c, rules: c.rules.filter(r => r.active)})).filter(c => c.rules.length > 0);
      title = 'Flagged Signals';
      subtitle = charts.length === 0 ? 'All quiet — no rules triggered right now' : charts.reduce((n, c) => n + c.rules.length, 0) + ' rules active';
    } else if (mode === 'bitcoin') {
      charts = charts.filter(c => c.category === 'Bitcoin');
      title = 'Bitcoin — All Rules'; subtitle = 'Full BTC rule matrix';
    } else if (mode === 'ethereum') {
      charts = charts.filter(c => c.category === 'Ethereum');
      title = 'Ethereum — All Rules'; subtitle = 'Full ETH rule matrix';
    } else if (mode === 'altcoins') {
      charts = charts.filter(c => c.category === 'Altcoins');
      title = 'Altcoins — All Rules'; subtitle = 'Full altcoin rule matrix';
    }

    // Empty state for flagged
    if (mode === 'flagged' && charts.length === 0) {
      container.innerHTML = '<div style="display:flex;flex-direction:column;align-items:center;justify-content:center;height:60%;gap:12px"><div style="font-size:48px;opacity:0.2">&#10003;</div><div style="font-size:15px;font-weight:500;color:var(--graphite)">All quiet</div><div style="font-size:13px;color:var(--muted)">No rules triggered. Check the deep dive views for the full picture.</div></div>';
      canvas.parentElement.appendChild(container);
      setTitle(title, subtitle);
      document.getElementById('chart-spin').classList.remove('on');
      return;
    }

    // ── Chart of the Day ──
    let cotdHTML = '';
    if (mode === 'flagged' || !mode) {
      // Score each chart: major active = 3, minor active = 1
      let scored = [];
      for (const c of data.charts) {
        let score = 0;
        let activeRules = [];
        for (const r of c.rules) {
          if (r.active) {
            const pts = (r.weight === 'major') ? 3 : 1;
            score += pts;
            activeRules.push(r.name);
          }
        }
        if (score > 0) {
          scored.push({ chart: c, score, activeRules });
        }
      }
      scored.sort((a, b) => b.score - a.score);

      if (scored.length > 0) {
        const top = scored[0];
        const c = top.chart;
        const ruleList = top.activeRules.slice(0, 3).join(' · ');
        const dotCount = top.activeRules.length;

        // Build detailed context for each active rule
        const ruleDetails = top.activeRules.map(rName => {
          for (const r of top.chart.rules) {
            if (r.name === rName && r.active) return r;
          }
          return null;
        }).filter(Boolean);

        const contextRows = ruleDetails.map(r => {
          const wt = r.weight === 'major' ? '<span style="font-size:9px;font-weight:600;padding:1px 5px;border-radius:3px;background:rgba(0,214,74,0.15);color:#059669">MAJOR</span>' : '<span style="font-size:9px;padding:1px 5px;border-radius:3px;background:' + (darkMode ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.05)') + ';color:var(--muted)">minor</span>';
          return '<div style="display:flex;align-items:flex-start;gap:8px;padding:5px 0;border-bottom:0.5px solid ' + (darkMode ? 'rgba(255,255,255,0.04)' : 'rgba(0,0,0,0.04)') + '">' +
            '<span style="display:inline-block;width:6px;height:6px;border-radius:50%;background:#00D64A;margin-top:5px;flex-shrink:0"></span>' +
            '<div style="flex:1"><div style="font-size:12px;font-weight:500;color:var(--graphite)">' + r.name + ' ' + wt + '</div>' +
            '<div style="font-size:11px;font-family:var(--mono);color:' + (darkMode ? '#B0B3B0' : '#555') + ';margin-top:2px">' + r.detail + '</div>' +
            (r.context ? '<div style="font-size:11px;color:var(--muted);font-style:italic;margin-top:2px">' + r.context + '</div>' : '') +
            '</div></div>';
        }).join('');

        cotdHTML = `
          <div style="margin-bottom:20px;padding:20px 24px;border-radius:8px;background:${darkMode ? 'rgba(0,214,74,0.04)' : 'rgba(0,180,60,0.04)'};border:1px solid ${darkMode ? 'rgba(0,214,74,0.12)' : 'rgba(0,180,60,0.12)'}">
            <div style="display:flex;align-items:center;gap:8px;margin-bottom:12px">
              <span style="font-size:11px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:${darkMode ? '#00D64A' : '#059669'}">Chart of the Day</span>
              <span style="font-size:10px;color:var(--muted)">${dotCount} signal${dotCount > 1 ? 's' : ''} active · score ${top.score}</span>
            </div>
            <div style="margin-bottom:14px">
              <a href="#" onclick="selectView('${c.chart_tab}','${c.chart_key}');return false;" style="font-size:18px;font-weight:600;color:var(--graphite);text-decoration:none;border-bottom:2px solid ${darkMode ? 'rgba(0,214,74,0.3)' : 'rgba(0,180,60,0.3)'}">${c.chart_name} →</a>
              <div style="margin-top:4px;font-size:12px;color:var(--muted)">${c.category}</div>
            </div>
            <div style="margin-top:8px">${contextRows}</div>
          </div>`;
      }
    }

    let tableHTML = cotdHTML + '<div style="margin-bottom:12px"><span style="font-family:var(--mono);font-size:11px;color:var(--muted)">Last updated: ' + data.updated + '</span></div>';
    tableHTML += '<table style="width:100%;border-collapse:collapse;font-size:13px">';
    tableHTML += '<thead><tr style="border-bottom:1px solid var(--border)">';
    tableHTML += '<th style="text-align:left;font-size:11px;font-weight:600;color:var(--muted);letter-spacing:.06em;text-transform:uppercase;padding:8px 10px;width:22%">Chart</th>';
    tableHTML += '<th style="text-align:left;font-size:11px;font-weight:600;color:var(--muted);letter-spacing:.06em;text-transform:uppercase;padding:8px 10px;width:20%">Rule</th>';
    tableHTML += '<th style="text-align:center;font-size:11px;font-weight:600;color:var(--muted);letter-spacing:.06em;text-transform:uppercase;padding:8px 10px;width:6%">Status</th>';
    tableHTML += '<th style="text-align:left;font-size:11px;font-weight:600;color:var(--muted);letter-spacing:.06em;text-transform:uppercase;padding:8px 10px;width:25%">Detail</th>';
    tableHTML += '<th style="text-align:left;font-size:11px;font-weight:600;color:var(--muted);letter-spacing:.06em;text-transform:uppercase;padding:8px 10px;width:27%">Context</th>';
    tableHTML += '</tr></thead><tbody>';

    let lastCat = null;
    for (const c of charts) {
      // Category band
      if (c.category !== lastCat) {
        lastCat = c.category;
        tableHTML += '<tr><td colspan="7" style="padding:18px 0 8px;text-align:center;font-size:13px;font-weight:700;color:var(--graphite);letter-spacing:.08em;text-transform:uppercase;background:' + (darkMode ? 'rgba(255,255,255,0.03)' : 'rgba(0,0,0,0.03)') + ';border-top:2px solid var(--border);border-bottom:1px solid var(--border)">' + c.category + '</td></tr>';
      }

      // Chart rows
      const chartLink = c.chart_key && c.chart_tab
        ? '<a href="#" onclick="selectView(\'' + c.chart_tab + '\',\'' + c.chart_key + '\');return false;" style="color:var(--graphite);text-decoration:none;border-bottom:1px dotted var(--muted);font-weight:600">' + c.chart_name + ' &#8594;</a>'
        : '<span style="color:var(--muted)">' + c.chart_name + '</span>';

      for (let ri = 0; ri < c.rules.length; ri++) {
        const r = c.rules[ri];
        const opacity = (mode === 'flagged' || r.active) ? '1' : '0.35';
        const rowBg = r.active ? (darkMode ? 'rgba(0,214,74,0.06)' : 'rgba(0,180,60,0.06)') : 'transparent';
        const dot = r.active
          ? '<span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:#00D64A"></span>'
          : '<span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:var(--border)"></span>';

        tableHTML += '<tr style="border-bottom:0.5px solid var(--border);background:' + rowBg + ';opacity:' + opacity + ';transition:opacity 0.15s" onmouseenter="this.style.opacity=\'1\'" onmouseleave="this.style.opacity=\'' + opacity + '\'">';
        // Only show chart link on first rule row for this chart
        tableHTML += '<td style="padding:8px 10px">' + (ri === 0 ? chartLink : '') + '</td>';
        tableHTML += '<td style="padding:8px 10px;font-weight:' + (r.active ? '600' : '400') + '">' + r.name + '</td>';
        tableHTML += '<td style="padding:8px 10px;text-align:center">' + dot + '</td>';
        tableHTML += '<td style="padding:8px 10px;font-family:var(--mono);font-size:12px;color:' + (r.active ? 'var(--graphite)' : 'var(--muted)') + '">' + r.detail + '</td>';
        tableHTML += '<td style="padding:8px 10px;font-size:11px;color:var(--muted);font-style:italic">' + (r.context || '') + '</td>';
        tableHTML += '</tr>';
      }
    }

    tableHTML += '</tbody></table>';
    container.innerHTML = tableHTML;
    canvas.parentElement.appendChild(container);
    setTitle(title, subtitle);
    document.getElementById('chart-spin').classList.remove('on');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}
