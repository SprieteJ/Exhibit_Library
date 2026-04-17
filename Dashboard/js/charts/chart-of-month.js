async function fetchChartOfMonth() {
  spinOn();
  try {
    // Fetch CC data to score charts
    const ccData = await fetch('/api/control-center', {signal: _navAbort?.signal}).then(r=>r.json());
    if (!ccData.charts?.length) throw new Error('no data');

    // Score each chart
    let scored = [];
    for (const c of ccData.charts) {
      let score = 0;
      let activeRules = [];
      for (const r of c.rules) {
        if (r.active) {
          score += (r.weight === 'major') ? 3 : 1;
          activeRules.push(r);
        }
      }
      if (score > 0 && c.chart_tab && c.chart_key) {
        scored.push({ chart: c, score, activeRules });
      }
    }
    scored.sort((a, b) => b.score - a.score);

    if (!scored.length) {
      if (chart) { chart.destroy(); chart = null; }
      const canvas = document.getElementById('main-chart');
      canvas.style.display = 'none';
      let existing = document.getElementById('cc-matrix-container');
      if (existing) existing.remove();
      const container = document.createElement('div');
      container.id = 'cc-matrix-container';
      container.style.cssText = 'position:absolute;inset:0;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:12px';
      container.innerHTML = '<div style="font-size:48px;opacity:0.2">&#10003;</div><div style="font-size:15px;font-weight:500;color:var(--graphite)">All quiet</div><div style="font-size:13px;color:var(--muted)">No active signals right now.</div>';
      canvas.parentElement.appendChild(container);
      setTitle('Chart of the Month', 'No active signals');
      document.getElementById('chart-spin').classList.remove('on');
      return;
    }

    const top = scored[0];
    const c = top.chart;

    // Build info card above the chart
    const rulesSummary = top.activeRules.map(r => {
      const dot = '<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:#00D64A;margin-right:4px"></span>';
      const weight = r.weight === 'major' ? '<span style="font-size:10px;font-weight:600;color:var(--graphite)">major</span>' : '<span style="font-size:10px;color:var(--muted)">minor</span>';
      return '<div style="display:flex;align-items:center;gap:8px;padding:4px 0">' + dot + '<span style="font-weight:500">' + r.name + '</span> ' + weight + ' <span style="font-family:var(--mono);font-size:11px;color:var(--muted)">' + r.detail + '</span></div>';
    }).join('');

    const perfRow = document.getElementById('perf-row');
    if (perfRow) {
      perfRow.innerHTML = `
        <div style="display:flex;align-items:flex-start;gap:20px;width:100%;padding:4px 0">
          <div style="flex:1">
            <div style="font-size:11px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:${darkMode ? '#00D64A' : '#059669'};margin-bottom:6px">Chart of the Month — ${c.category}</div>
            <div style="font-size:11px;color:var(--muted);margin-bottom:8px">${top.activeRules.length} active signal${top.activeRules.length > 1 ? 's' : ''} (score: ${top.score})</div>
            ${rulesSummary}
          </div>
        </div>`;
    }

    // Now load the actual chart by calling its fetch function
    const from = document.getElementById('rp-from')?.value || '2020-01-01';
    const to = document.getElementById('rp-to')?.value || new Date().toISOString().split('T')[0];

    // Map chart_key to its fetch function
    const fetchMap = {
      'btc-ma-gap': () => fetchBtcMAGap(from, to),
      'btc-ma': () => fetchBtcMA(from, to),
      'btc-200w-floor': () => fetchBtc200wFloor(from, to),
      'btc-200d-dev': () => fetchBtc200dDev(from, to),
      'btc-drawdown': () => fetchBtcDrawdownATH(from, to),
      'btc-realvol': () => fetchBtcRealvol(from, to),
      'btc-rv-iv': () => fetchBtcRvIv(from, to),
      'btc-funding': () => fetchBtcFunding(from, to),
      'btc-dominance': () => fetchBtcDominance(from, to),
      'btc-dom-ma': () => fetchBtcDominanceMa(from, to),
      'btc-mcap': () => fetchBtcMcap(from, to),
      'btc-pi-cycle': () => fetchBtcPiCycle(from, to),
      'eth-ma-gap': () => fetchEthMAGap(from, to),
      'eth-ma': () => fetchEthMA(from, to),
      'eth-200d-dev': () => fetchEth200dDev(from, to),
      'eth-drawdown': () => fetchEthDrawdown(from, to),
      'eth-btc-ratio': () => fetchEthBtcRatio(from, to),
      'am-dominance': () => fetchDominanceShares(from, to),
    };

    const loader = fetchMap[c.chart_key];
    if (loader) {
      await loader();
      // Override the title to include COTM branding
      setTitle('Chart of the Month: ' + c.chart_name, c.category + ' · ' + top.activeRules.length + ' active signals');
    } else {
      // Fallback: navigate to the chart
      selectView(c.chart_tab, c.chart_key);
    }

  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}



// ── Control Center: Regime & Cycle Panel ──────────────────────────────────────

