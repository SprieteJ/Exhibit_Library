async function fetchBtcLanding() {
  spinOn();
  try {
    if (chart) { chart.destroy(); chart = null; }
    var canvas = document.getElementById('main-chart');
    canvas.style.display = 'none';
    var existing = document.getElementById('cc-matrix-container');
    if (existing) existing.remove();

    var container = document.createElement('div');
    container.id = 'cc-matrix-container';
    container.style.cssText = 'width:100%;height:100%;overflow-y:auto;padding:20px 24px;position:absolute;top:0;left:0;right:0;bottom:0;background:var(--bg);z-index:5;';

    var borderCol = darkMode ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.08)';
    var cardBg = darkMode ? 'rgba(255,255,255,0.02)' : 'rgba(0,0,0,0.015)';
    var hoverBg = darkMode ? 'rgba(255,255,255,0.05)' : 'rgba(0,0,0,0.03)';

    var categories = [
      { title: 'Price Performance', icon: '\u{1F4C8}', color: '#00D64A',
        charts: [
          { key: 'btc-rolling', name: 'Rolling return', desc: 'N-day rolling return (%)' },
          { key: 'btc-drawdown', name: 'Drawdown from ATH', desc: 'Continuous drawdown from peak' },
        ]},
      { title: 'Moving Averages', icon: '\u301C\uFE0F', color: '#2471CC',
        charts: [
          { key: 'btc-ma', name: '50d & 200d MA', desc: 'Price with moving averages (log)' },
          { key: 'btc-ma-gap', name: '50d/200d gap', desc: 'Golden/death cross proximity' },
          { key: 'btc-200w-floor', name: '200-week MA', desc: 'The macro floor' },
          { key: 'btc-200d-dev', name: '200-week deviation', desc: '% deviation from 200w MA' },
          { key: 'btc-pi-cycle', name: 'Pi cycle top', desc: '111d vs 2x350d MA' },
        ]},
      { title: 'Market Cap', icon: '\u{1F4B0}', color: '#F7931A',
        charts: [
          { key: 'btc-mcap', name: 'Total market cap', desc: 'BTC mcap with milestones' },
          { key: 'btc-dominance', name: 'Market dominance', desc: 'BTC as % of total crypto' },
        ]},
      { title: 'BTC Dominance', icon: '\u{1F451}', color: '#FFB800',
        charts: [
          { key: 'btc-dom-ma', name: '50d & 200d MA', desc: 'Dominance with moving averages' },
        ]},
      { title: 'Volatility', icon: '\u26A1', color: '#746BE6',
        charts: [
          { key: 'btc-realvol', name: 'Realised volatility', desc: '30d, 90d, 180d rolling vol' },
          { key: 'btc-rv-iv', name: 'RV vs IV (DVOL)', desc: 'Realized vs implied vol' },
        ]},
      { title: 'Bitcoin vs Gold', icon: '\u{1F947}', color: '#DB33CB',
        charts: [
          { key: 'btc-gold', name: 'BTC vs Gold', desc: 'Price overlay comparison' },
          { key: 'btc-gold-ratio', name: 'BTC/Gold ratio', desc: 'BTC priced in gold' },
        ]},
      { title: 'Derivatives', icon: '\u{1F4CA}', color: '#EC5B5B',
        charts: [
          { key: 'btc-funding', name: 'Funding rate', desc: '8h perpetual funding' },
          { key: 'btc-oi', name: 'Open interest', desc: 'Total OI with price overlay' },
          { key: 'btc-funding-delta', name: 'Funding delta', desc: '30d funding change vs price' },
        ]},
      { title: 'Cycle Analysis', icon: '\u{1F504}', color: '#26A17B',
        charts: [
          { key: 'btc-epochs', name: 'Halving epochs', desc: 'x-fold from halving price' },
          { key: 'btc-cycles', name: 'Bear market cycles', desc: 'Indexed to peak' },
          { key: 'btc-bull', name: 'Bull market cycles', desc: 'Indexed to trough' },
        ]},
    ];

    var h = '';
    h += '<div style="display:grid;grid-template-columns:repeat(auto-fill, minmax(280px, 1fr));gap:16px">';

    for (var ci = 0; ci < categories.length; ci++) {
      var cat = categories[ci];
      h += '<div style="border:1px solid ' + borderCol + ';border-radius:10px;overflow:hidden;background:' + cardBg + '">';
      h += '<div style="padding:14px 16px 10px;border-bottom:1px solid ' + borderCol + ';display:flex;align-items:center;gap:10px">';
      h += '<span style="font-size:18px">' + cat.icon + '</span>';
      h += '<span style="font-size:14px;font-weight:600;color:var(--graphite)">' + cat.title + '</span>';
      h += '<span style="margin-left:auto;font-size:11px;color:var(--muted)">' + cat.charts.length + '</span>';
      h += '</div>';
      h += '<div style="padding:6px 0">';
      for (var chi = 0; chi < cat.charts.length; chi++) {
        var ch = cat.charts[chi];
        h += '<div class="btc-landing-item" data-key="' + ch.key + '" style="padding:8px 16px;cursor:pointer;display:flex;align-items:center;gap:10px;transition:background 0.1s">';
        h += '<div style="width:3px;height:24px;border-radius:2px;background:' + cat.color + ';opacity:0.4;flex-shrink:0"></div>';
        h += '<div style="flex:1;min-width:0">';
        h += '<div style="font-size:13px;font-weight:500;color:var(--graphite);white-space:nowrap;overflow:hidden;text-overflow:ellipsis">' + ch.name + '</div>';
        h += '<div style="font-size:11px;color:var(--muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis">' + ch.desc + '</div>';
        h += '</div>';
        h += '<svg width="14" height="14" viewBox="0 0 14 14" style="flex-shrink:0;opacity:0.3"><path d="M5 3l4 4-4 4" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/></svg>';
        h += '</div>';
      }
      h += '</div></div>';
    }
    h += '</div>';

    container.innerHTML = h;
    var items = container.querySelectorAll('.btc-landing-item');
    for (var ii = 0; ii < items.length; ii++) {
      (function(item) {
        item.addEventListener('mouseenter', function() { item.style.background = hoverBg; });
        item.addEventListener('mouseleave', function() { item.style.background = ''; });
        item.addEventListener('click', function() {
          var chartKey = item.getAttribute('data-key');
          selectView('bitcoin', chartKey);
        });
      })(items[ii]);
    }

    var chartArea = document.getElementById('chart-area') || canvas.parentElement;
    chartArea.appendChild(container);
    var emptyEl = document.getElementById('chart-empty');
    if (emptyEl) emptyEl.style.display = 'none';
    var perfRow = document.getElementById('perf-row');
    if (perfRow) perfRow.innerHTML = '';
    setTitle('Bitcoin', '8 categories \u00b7 ' + categories.reduce(function(s,c){return s+c.charts.length},0) + ' charts');
    document.getElementById('chart-spin').classList.remove('on');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

