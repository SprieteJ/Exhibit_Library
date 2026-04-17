async function fetchRegimePanel() {
  spinOn();
  try {
    var ccData = await fetch('/api/control-center', {signal: _navAbort?.signal}).then(function(r){return r.json()});

    if (chart) { chart.destroy(); chart = null; }
    var canvas = document.getElementById('main-chart');
    canvas.style.display = 'none';
    var existing = document.getElementById('cc-matrix-container');
    if (existing) existing.remove();

    var container = document.createElement('div');
    container.id = 'cc-matrix-container';
    container.style.cssText = 'width:100%;height:100%;overflow-y:auto;padding:20px 24px;position:absolute;top:0;left:0;right:0;bottom:0;background:var(--bg);z-index:5;';

    var red = darkMode ? '#F09595' : '#A32D2D';
    var green = darkMode ? '#5DCAA5' : '#0F6E56';
    var redBg = darkMode ? 'rgba(240,149,149,0.06)' : 'rgba(163,45,45,0.04)';
    var greenBg = darkMode ? 'rgba(93,202,165,0.06)' : 'rgba(15,110,86,0.04)';
    var borderCol = 'var(--border)';

    // Build lookup
    var ccLookup = {};
    if (ccData && ccData.charts) {
      for (var ci = 0; ci < ccData.charts.length; ci++) {
        var ch = ccData.charts[ci];
        if (!ccLookup[ch.chart_key]) ccLookup[ch.chart_key] = [];
        for (var ri = 0; ri < ch.rules.length; ri++) {
          var ru = ch.rules[ri];
          ccLookup[ch.chart_key].push({name: ru.name, active: ru.active, detail: ru.detail || '', context: ru.context || '', weight: ru.weight || '', type: ru.type || ''});
        }
      }
    }

    function findRule(chartKey, sub, onlyActive) {
      var arr = ccLookup[chartKey] || [];
      for (var i = 0; i < arr.length; i++) {
        if (arr[i].name.toLowerCase().indexOf(sub.toLowerCase()) >= 0) {
          if (onlyActive && !arr[i].active) continue;
          return arr[i];
        }
      }
      return null;
    }

    var regimes = [
      { name: 'Risk-on expansion', tag: 'bullish alts', color: '#3B6D11', bgColor: '#EAF3DE',
        indicators: [
          { label: 'BTC dominance falling', ck: 'btc-dom-ma', rm: 'dominance falling', fb: '30d z < -1.0' },
          { label: 'Altseason elevated', ck: 'alt-altseason', rm: 'altseason > 75', fb: '> 75% alts outperforming BTC' },
          { label: 'Funding rate positive', ck: 'btc-funding', rm: 'funding positive', fb: '> 0.01% avg' },
          { label: 'ETH/BTC ratio rising', ck: 'eth-btc-ratio', rm: 'eth/btc rising', fb: '30d > +5%' },
          { label: 'Alt intracorrelation rising', ck: 'am-intracorr', rm: 'correlation rising', fb: 'Rising > +0.10' },
        ]},
      { name: 'BTC flight-to-quality', tag: 'btc dominance', color: '#185FA5', bgColor: '#E6F1FB',
        indicators: [
          { label: 'BTC dominance rising', ck: 'btc-dom-ma', rm: 'dominance rising', fb: 'z > +1.0' },
          { label: 'ETH/BTC ratio falling', ck: 'eth-btc-ratio', rm: 'falling sharply', fb: '30d < -10%' },
          { label: 'Alt share declining', ck: 'am-dominance', rm: 'alt share', fb: 'Alt mcap vs total' },
          { label: 'BTC ETF inflows, ETH flat', ck: null, rm: null, fb: 'ETF flow divergence' },
          { label: 'Alt correlated + falling', ck: 'am-intracorr', rm: 'correlated and alts falling', fb: 'Corr > 0.4 AND alts down' },
        ]},
      { name: 'Deleveraging', tag: 'crisis', color: '#A32D2D', bgColor: '#FCEBEB',
        indicators: [
          { label: 'Funding flips negative', ck: 'btc-funding', rm: 'negative streak', fb: '5+ consecutive negative days' },
          { label: 'Open interest dropping', ck: null, rm: null, fb: '> 15% drop in 7 days' },
          { label: 'Realized vol spiking', ck: null, rm: null, fb: '> 80% annualized' },
          { label: 'BTC-SPY correlation spiking', ck: null, rm: null, fb: '> 0.6' },
          { label: 'BTC drawdown velocity', ck: null, rm: null, fb: '> 10% drop in 7 days' },
        ]},
      { name: 'Consolidation', tag: 'sideways', color: '#5F5E5A', bgColor: '#F1EFE8',
        indicators: [
          { label: 'Realized vol low', ck: null, rm: null, fb: '< 35% annualized' },
          { label: 'BTC drawdown mild', ck: null, rm: null, fb: 'Between -5% and -20%' },
          { label: 'Funding near zero', ck: null, rm: null, fb: '< 0.005%' },
          { label: 'Stablecoin supply growing', ck: null, rm: null, fb: 'Or stable' },
          { label: 'DVOL low', ck: null, rm: null, fb: '< 50' },
        ]},
      { name: 'Macro risk-off', tag: 'external shock', color: '#854F0B', bgColor: '#FAEEDA',
        indicators: [
          { label: 'SPY/QQQ falling', ck: null, rm: null, fb: '30d return < -5%' },
          { label: 'DXY strengthening', ck: null, rm: null, fb: 'Rising trend' },
          { label: 'BTC-SPY correlation high', ck: null, rm: null, fb: '> 0.5 sustained' },
          { label: 'ETF outflows', ck: null, rm: null, fb: 'Net negative' },
          { label: 'VIX elevated', ck: null, rm: null, fb: '> 25' },
        ]},
    ];

    var h = '';
    h += '<div style="margin-bottom:32px">';
    h += '<div style="font-size:13px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:var(--muted);margin-bottom:16px">Market regime</div>';
    h += '<div style="font-size:12px;color:var(--muted);margin-bottom:16px">Each regime confirmed when 3 of 5 indicators align. Green = wired + active. Grey = wired but inactive. Red = not yet wired.</div>';

    for (var ri2 = 0; ri2 < regimes.length; ri2++) {
      var reg = regimes[ri2];
      var liveCount = 0;
      var indHtml = '';

      for (var ii = 0; ii < reg.indicators.length; ii++) {
        var ind = reg.indicators[ii];
        var isLive = false;
        var det = ind.fb;
        var isWired = !!(ind.ck && ind.rm);

        if (ind.ck && ind.rm) {
          var activeRule = findRule(ind.ck, ind.rm, true);
          if (activeRule) {
            isLive = true;
            det = activeRule.detail;
            liveCount++;
          } else {
            var anyRule = findRule(ind.ck, ind.rm, false);
            if (anyRule) det = anyRule.detail;
          }
        }

        var ic = isLive ? green : (isWired ? '#888' : red);
        var dot = isLive ? '&#10003;' : (isWired ? '&#9679;' : '&#10007;');
        indHtml += '<div style="display:flex;align-items:center;gap:8px;padding:5px 0;border-bottom:0.5px solid ' + (darkMode ? 'rgba(255,255,255,0.03)' : 'rgba(0,0,0,0.03)') + '">';
        indHtml += '<span style="font-size:12px;color:' + ic + ';width:16px;text-align:center">' + dot + '</span>';
        indHtml += '<span style="font-size:12px;color:' + (isLive ? green : 'var(--graphite)') + ';font-weight:' + (isLive ? '600' : '400') + '">' + ind.label + '</span>';
        if (ind.ck) { indHtml += '<a href="javascript:void(0)" data-chartkey="' + ind.ck + '" style="font-size:10px;color:var(--muted);margin-left:6px;text-decoration:none;padding:1px 6px;border:1px solid var(--border);border-radius:4px">view →</a>'; }
        indHtml += '<span style="font-size:11px;font-family:var(--mono);color:var(--muted);margin-left:auto;max-width:50%;text-align:right;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' + det + '</span>';
        indHtml += '</div>';
      }

      var confirmed = liveCount >= 3;
      h += '<div style="margin-bottom:16px;border:0.5px solid ' + borderCol + ';border-radius:8px;overflow:hidden' + (confirmed ? ';border-color:' + green : '') + '">';
      h += '<div style="display:flex;align-items:center;gap:10px;padding:12px 16px;background:' + (confirmed ? greenBg : (darkMode ? 'rgba(255,255,255,0.02)' : 'rgba(0,0,0,0.015)')) + '">';
      h += '<span style="font-size:13px;font-weight:600;color:var(--graphite)">' + reg.name + '</span>';
      h += '<span style="font-size:10px;padding:2px 8px;border-radius:4px;background:' + reg.bgColor + ';color:' + reg.color + '">' + reg.tag + '</span>';
      h += '<span style="margin-left:auto;font-size:11px;font-weight:600;color:' + (liveCount >= 3 ? green : (liveCount > 0 ? '#E1C87E' : red)) + '">' + liveCount + '/5</span>';
      if (confirmed) h += '<span style="font-size:10px;padding:2px 6px;border-radius:3px;background:' + greenBg + ';color:' + green + ';font-weight:600">CONFIRMED</span>';
      h += '</div>';
      h += '<div style="padding:8px 16px 12px">' + indHtml + '</div></div>';
    }
    h += '</div>';

    // Cycle blueprint
    var steps = [
      [1,'Liquidity conditions improve','Monetary policy easing. DXY weakening, real yields falling.'],
      [2,'VCs raise capital','New fund raises, deployment into crypto startups accelerates.'],
      [3,'Speculation demand increases','DEX activity, DeFi loans, stablecoin supply expanding.'],
      [4,'Media covers crypto','Mainstream financial media running crypto stories regularly.'],
      [5,'FOMO builds','New speculators. More builders. More VC capital. More DeFi demand.'],
      [6,'DeFi boosts yields','Projects compete for TVL by boosting yields.'],
      [7,'Rotation into alts','Asset prices rise, investors chase beta.'],
      [8,'New products & capital markets','ETFs, IPOs, ICOs, VC investments, M&A surges.'],
      [9,'Leverage builds','Funding elevated, OI/mcap high, vol compressed. Everyone is long.'],
      [10,'Liquidity rolls over','Markets sell off. DXY reverses. Next crypto winter begins.'],
    ];
    h += '<div>';
    h += '<div style="font-size:13px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:var(--muted);margin-bottom:16px">Crypto cycle blueprint</div>';
    for (var si = 0; si < steps.length; si++) {
      h += '<div style="display:flex;align-items:flex-start;gap:12px;padding:10px 0;border-bottom:0.5px solid ' + borderCol + '">';
      h += '<div style="width:28px;height:28px;border-radius:50%;background:' + redBg + ';display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:600;color:' + red + ';flex-shrink:0">' + steps[si][0] + '</div>';
      h += '<div style="flex:1"><div style="font-size:13px;font-weight:500;color:' + red + '">' + steps[si][1] + '</div><div style="font-size:11px;color:var(--muted);margin-top:2px">' + steps[si][2] + '</div></div>';
      h += '</div>';
    }
    h += '</div>';

    container.innerHTML = h;
    var chartArea = document.getElementById('chart-area') || canvas.parentElement;
    chartArea.appendChild(container);
    var emptyEl = document.getElementById('chart-empty');
    if (emptyEl) emptyEl.style.display = 'none';
    var perfRow = document.getElementById('perf-row');
    if (perfRow) perfRow.innerHTML = '';
    // Add click handlers for regime links
    chartArea.addEventListener('click', function(e) {
      var el = e.target.closest('.regime-link');
      if (el && el.dataset.ck) {
        navigateToChart(el.dataset.ck);
      }
    });
    // Bind chart navigation links
    var links = container.querySelectorAll('a[data-chartkey]');
    for (var li = 0; li < links.length; li++) {
      (function(link) {
        link.addEventListener('click', function(e) {
          e.preventDefault();
          navigateToChart(link.getAttribute('data-chartkey'));
        });
      })(links[li]);
    }
    setTitle('Market Regime & Cycle Position', 'Live scoring from ' + (ccData.charts ? ccData.charts.length : 0) + ' indicators');
    document.getElementById('chart-spin').classList.remove('on');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

