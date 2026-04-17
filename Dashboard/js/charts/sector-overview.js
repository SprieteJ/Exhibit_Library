async function fetchSectorOverview() {
  const _v = _navVersion;
  spinOn();
  try {
    const data = await fetch('/api/sector-overview', {signal: _navAbort?.signal}).then(r=>r.json());
    if (_v !== _navVersion) return;
    if (!data.sectors?.length) throw new Error('no data');

    // Destroy chart, show table
    if (chart) { chart.destroy(); chart = null; }
    const canvas = document.getElementById('main-chart');
    canvas.style.display = 'none';

    let existing = document.getElementById('cc-matrix-container');
    if (existing) existing.remove();

    const container = document.createElement('div');
    container.id = 'cc-matrix-container';
    container.style.cssText = 'position:absolute;inset:0;overflow-y:auto;padding:16px 20px;';

    const fmtM = v => {
      if (v == null) return '—';
      if (v >= 1e12) return '$' + (v/1e12).toFixed(2) + 'T';
      if (v >= 1e9)  return '$' + (v/1e9).toFixed(1) + 'B';
      if (v >= 1e6)  return '$' + (v/1e6).toFixed(0) + 'M';
      return '$' + v.toFixed(0);
    };

    let tableHTML = `
      <table style="width:100%;border-collapse:collapse;font-size:13px">
        <thead>
          <tr style="border-bottom:1px solid var(--border)">
            <th style="text-align:left;font-size:11px;font-weight:600;color:var(--muted);letter-spacing:.06em;text-transform:uppercase;padding:8px 10px">Sector</th>
            <th style="text-align:right;font-size:11px;font-weight:600;color:var(--muted);letter-spacing:.06em;text-transform:uppercase;padding:8px 10px">Total Mcap</th>
            <th style="text-align:right;font-size:11px;font-weight:600;color:var(--muted);letter-spacing:.06em;text-transform:uppercase;padding:8px 10px">Median Mcap</th>
            <th style="text-align:right;font-size:11px;font-weight:600;color:var(--muted);letter-spacing:.06em;text-transform:uppercase;padding:8px 10px">1M Perf (EW)</th>
            <th style="text-align:center;font-size:11px;font-weight:600;color:var(--muted);letter-spacing:.06em;text-transform:uppercase;padding:8px 10px"># Assets</th>
            <th style="text-align:left;font-size:11px;font-weight:600;color:var(--muted);letter-spacing:.06em;text-transform:uppercase;padding:8px 10px">Constituents</th>
          </tr>
        </thead>
        <tbody>`;

    for (const s of data.sectors) {
      const perfColor = s.perf_1m > 0 ? '#00D64A' : (s.perf_1m < 0 ? '#EC5B5B' : 'var(--muted)');
      const perfStr = s.perf_1m != null ? (s.perf_1m > 0 ? '+' : '') + s.perf_1m.toFixed(1) + '%' : '—';
      const constStr = s.constituents?.join(', ');

      tableHTML += `
        <tr style="border-bottom:0.5px solid var(--border)">
          <td style="padding:9px 10px;font-weight:600"><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:${s.color};margin-right:8px"></span>${s.name}</td>
          <td style="padding:9px 10px;text-align:right;font-family:var(--mono);font-size:12px">${fmtM(s.total_mcap)}</td>
          <td style="padding:9px 10px;text-align:right;font-family:var(--mono);font-size:12px">${fmtM(s.median_mcap)}</td>
          <td style="padding:9px 10px;text-align:right;font-family:var(--mono);font-size:12px;color:${perfColor}">${perfStr}</td>
          <td style="padding:9px 10px;text-align:center;font-family:var(--mono);font-size:12px">${s.count}</td>
          <td style="padding:9px 10px;font-size:11px;color:var(--muted)">${constStr}</td>
        </tr>`;
    }

    tableHTML += '</tbody></table>';
    container.innerHTML = tableHTML;
    canvas.parentElement.appendChild(container);
    setTitle('Sector Overview', 'Source: CoinGecko Pro — Wintermute OTC');
    document.getElementById('chart-spin').classList.remove('on');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}



// ── Themes: Sector Overview Matrix ────────────────────────────────────────────

