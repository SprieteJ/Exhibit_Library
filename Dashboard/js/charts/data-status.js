async function fetchDataStatus() {
  spinOn();
  try {
    var data = await fetch('/api/data-status', {signal: _navAbort?.signal}).then(function(r){return r.json()});

    if (chart) { chart.destroy(); chart = null; }
    var canvas = document.getElementById('main-chart');
    canvas.style.display = 'none';
    var existing = document.getElementById('cc-matrix-container');
    if (existing) existing.remove();

    var container = document.createElement('div');
    container.id = 'cc-matrix-container';
    container.style.cssText = 'width:100%;height:100%;overflow-y:auto;padding:20px 24px;position:absolute;top:0;left:0;right:0;bottom:0;background:var(--bg);z-index:5;';

    var categories = {
      'market_data': {label: 'Market Data', icon: '📊'},
      'perps': {label: 'Perpetuals', icon: '📈'},
      'derivatives': {label: 'Derivatives', icon: '🎯'},
      'etf': {label: 'ETFs', icon: '🏦'},
      'misc': {label: 'Miscellaneous', icon: '📁'}
    };

    var datasets = Array.isArray(data) ? data : (data.datasets || []);
    var borderCol = 'var(--border)';
    var h = '';

    for (var catKey in categories) {
      var cat = categories[catKey];
      var catData = datasets.filter(function(d) { return d.category === catKey; });
      if (!catData.length) continue;

      h += '<div style="margin-bottom:24px">';
      h += '<div style="font-size:13px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:var(--muted);margin-bottom:12px">' + cat.icon + ' ' + cat.label + '</div>';

      h += '<table style="width:100%;border-collapse:collapse;font-size:12px">';
      h += '<thead><tr style="border-bottom:1px solid ' + borderCol + '">';
      h += '<th style="text-align:left;padding:6px 8px;color:var(--muted);font-weight:600">Dataset</th>';
      h += '<th style="text-align:left;padding:6px 8px;color:var(--muted);font-weight:600">Granularity</th>';
      h += '<th style="text-align:left;padding:6px 8px;color:var(--muted);font-weight:600">Source</th>';
      h += '<th style="text-align:right;padding:6px 8px;color:var(--muted);font-weight:600">From</th>';
      h += '<th style="text-align:right;padding:6px 8px;color:var(--muted);font-weight:600">To</th>';
      h += '<th style="text-align:right;padding:6px 8px;color:var(--muted);font-weight:600">Assets</th>';
      h += '<th style="text-align:right;padding:6px 8px;color:var(--muted);font-weight:600">Rows</th>';
      h += '<th style="text-align:center;padding:6px 8px;color:var(--muted);font-weight:600">Status</th>';
      h += '</tr></thead><tbody>';

      for (var di = 0; di < catData.length; di++) {
        var d = catData[di];
        var statusColor = d.status === 'live' ? (darkMode ? '#5DCAA5' : '#059669') : d.status === 'stale' ? (darkMode ? '#F09595' : '#DC2626') : d.status === 'static' ? 'var(--muted)' : '#E1C87E';
        var statusLabel = d.status === 'live' ? 'Live' : d.status === 'stale' ? 'Stale' : d.status === 'static' ? 'Static' : d.status === 'empty' ? 'Empty' : 'Error';
        var rowsStr = d.rows > 1000000 ? (d.rows / 1000000).toFixed(2) + 'M' : d.rows > 1000 ? (d.rows / 1000).toFixed(0) + 'k' : d.rows.toString();

        h += '<tr style="border-bottom:0.5px solid ' + (darkMode ? 'rgba(255,255,255,0.03)' : 'rgba(0,0,0,0.03)') + '">';
        h += '<td style="padding:6px 8px;font-weight:500;color:var(--graphite)">' + d.name + '</td>';
        h += '<td style="padding:6px 8px;color:var(--muted)">' + d.granularity + '</td>';
        h += '<td style="padding:6px 8px;color:var(--muted)">' + d.source + '</td>';
        h += '<td style="padding:6px 8px;text-align:right;font-family:var(--mono);color:var(--muted)">' + (d.date_from || '—') + '</td>';
        h += '<td style="padding:6px 8px;text-align:right;font-family:var(--mono);color:var(--muted)">' + (d.date_to || '—') + '</td>';
        h += '<td style="padding:6px 8px;text-align:right;font-family:var(--mono);color:var(--graphite)">' + d.assets + '</td>';
        h += '<td style="padding:6px 8px;text-align:right;font-family:var(--mono);color:var(--graphite)">' + rowsStr + '</td>';
        h += '<td style="padding:6px 8px;text-align:center"><span style="font-size:10px;font-weight:600;padding:2px 8px;border-radius:4px;background:' + statusColor + '20;color:' + statusColor + '">' + statusLabel + '</span></td>';
        h += '</tr>';
      }

      h += '</tbody></table></div>';
    }

    // Summary
    var totalRows = datasets.reduce(function(s, d) { return s + d.rows; }, 0);
    var liveCount = datasets.filter(function(d) { return d.status === 'live'; }).length;
    var staleCount = datasets.filter(function(d) { return d.status === 'stale'; }).length;
    h += '<div style="margin-top:16px;font-size:11px;color:var(--muted)">';
    h += datasets.length + ' datasets · ' + (totalRows > 1000000 ? (totalRows/1000000).toFixed(1) + 'M' : (totalRows/1000).toFixed(0) + 'k') + ' total rows · ';
    h += liveCount + ' live · ' + staleCount + ' stale';
    h += ' · Updated ' + (data.updated || '?');
    h += '</div>';

    container.innerHTML = h;
    var chartArea = document.getElementById('chart-area') || canvas.parentElement;
    chartArea.appendChild(container);
    var emptyEl = document.getElementById('chart-empty');
    if (emptyEl) emptyEl.style.display = 'none';
    var perfRow = document.getElementById('perf-row');
    if (perfRow) perfRow.innerHTML = '';
    setTitle('Database Status', liveCount + ' live · ' + staleCount + ' stale · ' + datasets.length + ' datasets');
    document.getElementById('chart-spin').classList.remove('on');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

function navigateToChart(chartKey) {
  for (var t in TABS) {
    var tab = TABS[t];
    if (tab.groups) {
      for (var gi = 0; gi < tab.groups.length; gi++) {
        var grp = tab.groups[gi];
        if (grp.children) {
          for (var ci = 0; ci < grp.children.length; ci++) {
            if (grp.children[ci].key === chartKey) {
              selectView(grp.children[ci]);
              return;
            }
          }
        }
      }
    }
  }
}

