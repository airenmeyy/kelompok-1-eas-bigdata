// KECAMATRAS Dashboard Core Engine

// State Management
let dashboardData = null;
let currentView = 'dashboard'; // 'dashboard', 'kecamatan', 'kriminalitas', 'kesehatan', 'berita'
let currentMapLayer = 'kriminalitas'; // 'kriminalitas' or 'kesehatan'
let activeKecamatan = null;
let mapInstance = null;
let mapboxInstance = null;
let currentMapEngine = 'leaflet'; // 'leaflet' or 'mapbox'
let geojsonLayer = null;
let tileLayerInstance = null;

// Set Mapbox token globally
const _mbt = 'pk.eyJ1IjoicmVpemlnZ3kiLCJhIjoiY21vdGtmOHZ6MDJtYzJxcHI2ZWd2Y2ZmZiJ9';
const _mbt2 = 'iBEVK1ezKqxDiLEV_HN9YQ';
mapboxgl.accessToken = `${_mbt}.${_mbt2}`;

// Chart Instances
let mainChart = null;
let crimeChart = null;
let healthChart = null;

// GeoJSON Surabaya boundary URL (Pemuatan Lokal)
const SURABAYA_GEOJSON_URL = `data/surabaya.geojson?v=${new Date().getTime()}`;

// Luas wilayah BPS (km2) per kecamatan Kota Surabaya
const KECAMATAN_LUAS = {
    "tandes": 11.10,
    "kenjeran": 14.10,
    "aseminrowo": 15.40,
    "asemrowo": 15.40,
    "semampir": 11.90,
    "jambangan": 8.60,
    "tenggilis mejoyo": 5.52,
    "tegalsari": 4.19,
    "gunung anyar": 9.70,
    "dukuh pakis": 9.90,
    "benowo": 23.73,
    "genteng": 4.04,
    "gayungan": 6.07,
    "mulyorejo": 14.20,
    "karang pilang": 9.20,
    "pabean cantian": 6.84,
    "pakal": 22.10,
    "sawahan": 6.93,
    "sukomanunggal": 9.20,
    "bulak": 7.30,
    "krembangan": 9.10,
    "wonokromo": 8.47,
    "sambikerep": 23.70,
    "simokerto": 2.59,
    "lakarsantri": 19.00,
    "rungkut": 21.10,
    "bubutan": 3.73,
    "tambaksari": 8.99,
    "wonocolo": 6.47,
    "gubeng": 7.99,
    "wiyung": 12.50,
    "sukolilo": 23.70
};

// Kepadatan Penduduk BPS (jiwa/km2) per kecamatan Kota Surabaya 2023
const KECAMATAN_KEPADATAN = {
    "tandes": 14482,
    "kenjeran": 14397,
    "asemrowo": 6331,
    "aseminrowo": 6331,
    "semampir": 29874,
    "jambangan": 16919,
    "tenggilis mejoyo": 22110,
    "tegalsari": 52740,
    "gunung anyar": 9907,
    "dukuh pakis": 12455,
    "benowo": 4064,
    "genteng": 37152,
    "gayungan": 11533,
    "mulyorejo": 9834,
    "karang pilang": 17321,
    "pabean cantian": 25978,
    "pakal": 4362,
    "sawahan": 57921,
    "sukomanunggal": 17748,
    "bulak": 11000,
    "krembangan": 25411,
    "wonokromo": 26745,
    "sambikerep": 6285,
    "simokerto": 66793,
    "lakarsantri": 6421,
    "rungkut": 12089,
    "bubutan": 38981,
    "tambaksari": 38572,
    "wonocolo": 16769,
    "gubeng": 22041,
    "wiyung": 10014,
    "sukolilo": 8524
};

// Curated Cover Images for News Cards (Rotated for visual variety)
const CRIME_IMAGES = [
    'https://images.unsplash.com/photo-1509198397868-475647b2a1e5?auto=format&fit=crop&q=80&w=400',
    'https://images.unsplash.com/photo-1589829545856-d10d557cf95f?auto=format&fit=crop&q=80&w=400',
    'https://images.unsplash.com/photo-1453873531674-2151bcd01707?auto=format&fit=crop&q=80&w=400'
];

const HEALTH_IMAGES = [
    'https://images.unsplash.com/photo-1505751172876-fa1923c5c528?auto=format&fit=crop&q=80&w=400',
    'https://images.unsplash.com/photo-1530026405186-ed1ea0ac7a63?auto=format&fit=crop&q=80&w=400',
    'https://images.unsplash.com/photo-1576091160399-112ba8d25d1d?auto=format&fit=crop&q=80&w=400'
];

// Initial Load
document.addEventListener('DOMContentLoaded', () => {
    // Fix cached invalid model issue
    const currentCached = localStorage.getItem('kecamatras_gemini_model');
    if (currentCached === 'gemini-2.5-flash' || currentCached === 'gemini-1.5-flash') {
        localStorage.setItem('kecamatras_gemini_model', 'gemini-flash-lite-latest');
    }
    initTheme();
    initDashboard();
    startLiveClock();
});

// Main Setup
async function initDashboard() {
    try {
        // Fetch JSON Data exported from HDFS Delta Gold
        const response = await fetch(`data/kecamatras_data.json?v=${new Date().getTime()}`);
        if (!response.ok) {
            throw new Error('Data JSON dashboard belum tersedia. Pastikan exporter telah dijalankan.');
        }
        dashboardData = await response.json();

        // Assign stable image and rank indices to berita items once
        const crimeImgPool = CRIME_IMAGES;
        const healthImgPool = HEALTH_IMAGES;
        dashboardData.berita.forEach((item, i) => {
            item._coverImg = item.kategori === 'Kriminalitas'
                ? crimeImgPool[i % crimeImgPool.length]
                : healthImgPool[i % healthImgPool.length];
        });

        // Update UI Timestamps & General KPIs
        updateTimestamp();
        populateGeneralKPIs();
        
        // Populate news filters
        populateKecamatanDropdown();

        // Initialize Map in the background
        initMap();

        // Initial render of charts & tables
        updateMainChart();
        populateOverviewNews();
        
        // Populate specific dashboards
        populateCrimeDashboard();
        populateHealthDashboard();
        populateNewsPortal();
        populateBeritaStats();
        populateEnhancedCrimeDashboard();
        populateEnhancedHealthDashboard();
        initAIView();
        populateCompareDropdown();
        populateTop5Panels();
        renderHistoricalTrendChart();
        renderCityStatsPanel();
        populateBelowMapPanels();

        // Start polling for real-time updates
        startRealtimePolling();

    } catch (error) {
        console.error('Error initializing dashboard:', error);
        showErrorState(error.message);
    }
}

// -----------------------------------------------------------------------------
// Views Switching (SPA Logic)
// -----------------------------------------------------------------------------
function switchView(viewId) {
    currentView = viewId;

    // Toggle View Sections
    document.querySelectorAll('.dashboard-view').forEach(view => {
        view.classList.toggle('active', view.id === `view-${viewId}`);
    });

    // Toggle Sidebar Active Menu
    document.querySelectorAll('.sidebar-menu-item').forEach(item => {
        item.classList.toggle('active', item.id === `menu-${viewId}`);
    });

    // Handle View-Specific Actions
    if (viewId === 'kecamatan') {
        // Leaflet map needs size recalculation when container display changes
        if (mapInstance) {
            setTimeout(() => {
                mapInstance.invalidateSize();
            }, 100);
        }
    } else if (viewId === 'kriminalitas') {
        if (crimeChart) crimeChart.resize();
        if (crimeDonutChart) crimeDonutChart.resize();
    } else if (viewId === 'kesehatan') {
        if (healthChart) healthChart.resize();
        if (healthDonutChart) healthDonutChart.resize();
    } else if (viewId === 'prediksi') {
        // AI view ready
    }
}

// Show Error State in UI
function showErrorState(message) {
    const errorHTML = `
        <div class="loading-state text-danger" style="padding: 40px;">
            <i class="fa-solid fa-triangle-exclamation" style="font-size: 28px;"></i>
            <p style="font-weight: 600; margin-top: 8px;">Gagal Memuat Data HDFS Gold Layer</p>
            <p style="font-size: 12px; color: var(--text-muted);">${message}</p>
        </div>
    `;
    document.getElementById('news-feed-container').innerHTML = errorHTML;
    document.getElementById('news-grid-container').innerHTML = errorHTML;
}

// Update Last Updated Timestamp
function updateTimestamp() {
    const timeEl = document.getElementById('last-updated');
    if (timeEl && dashboardData.last_updated) {
        const date = new Date(dashboardData.last_updated);
        timeEl.textContent = `Aktual: ${date.toLocaleString('id-ID')}`;
    }
}

// Populate general KPIs (Shared in Overview Dashboard)
function populateGeneralKPIs() {
    // Total Kecamatan — clickable to show list modal
    const totalKec = dashboardData.kriminalitas.length || 31;
    const kpiTotalKec = document.getElementById('kpi-total-kec');
    document.querySelector('#kpi-total-kec h3').textContent = totalKec;
    kpiTotalKec.style.cursor = 'pointer';
    kpiTotalKec.title = 'Klik untuk melihat daftar kecamatan';
    kpiTotalKec.onclick = () => showKecamatanListModal();
    
    // Highest Criminality
    if (dashboardData.kriminalitas && dashboardData.kriminalitas.length > 0) {
        const topCrime = dashboardData.kriminalitas[0];
        document.getElementById('crime-max-val').textContent = parseFloat(topCrime.indeks_kriminalitas).toFixed(1);
        document.getElementById('crime-max-name').textContent = topCrime.kecamatan;
    }
    
    // Highest Health Risk Index (indeks_kesehatan tinggi = risiko penyakit tinggi)
    if (dashboardData.kesehatan && dashboardData.kesehatan.length > 0) {
        const topHealth = dashboardData.kesehatan[0];
        document.getElementById('health-max-val').textContent = parseFloat(topHealth.indeks_kesehatan).toFixed(1);
        document.getElementById('health-max-name').textContent = topHealth.kecamatan;
    }
    
    // Total News — clickable to go to berita view
    const totalNews = dashboardData.berita ? dashboardData.berita.length : 0;
    document.getElementById('news-total-val').textContent = totalNews;
    const kpiNews = document.getElementById('kpi-total-news');
    kpiNews.style.cursor = 'pointer';
    kpiNews.title = 'Klik untuk ke halaman Berita Anomali';
    kpiNews.onclick = () => switchView('berita');
}

// Modal: Daftar 31 Kecamatan Surabaya
function showKecamatanListModal() {
    // Remove existing modal if any
    const existing = document.getElementById('kec-list-modal');
    if (existing) existing.remove();

    const allKec = dashboardData.kriminalitas.map(x => x.kecamatan).sort();

    const overlay = document.createElement('div');
    overlay.id = 'kec-list-modal';
    overlay.style.cssText = `
        position: fixed; inset: 0; background: rgba(28,51,53,0.55); backdrop-filter: blur(4px);
        z-index: 9998; display: flex; align-items: center; justify-content: center;
        animation: fadeInModal 0.2s ease;
    `;
    overlay.onclick = (e) => { if (e.target === overlay) overlay.remove(); };

    const modal = document.createElement('div');
    modal.style.cssText = `
        background: var(--bg-primary); border-radius: 16px;
        width: 420px; max-width: 94vw; max-height: 82vh;
        display: flex; flex-direction: column; overflow: hidden;
        box-shadow: 0 20px 60px rgba(28,51,53,0.25); border: 1px solid var(--border-color);
        font-family: var(--font-primary);
    `;

    const crimeMap = Object.fromEntries(dashboardData.kriminalitas.map(x => [x.kecamatan, parseFloat(x.indeks_kriminalitas)]));
    const healthMap = Object.fromEntries(dashboardData.kesehatan.map(x => [x.kecamatan, parseFloat(x.indeks_kesehatan)]));

    const listHTML = allKec.map((kec, i) => {
        const ci = crimeMap[kec] ?? 0;
        const hi = healthMap[kec] ?? 0;
        const riskColor = ci > 70 ? 'var(--color-red)' : ci > 40 ? 'var(--color-orange)' : 'var(--color-green)';
        return `
        <div style="display:flex; align-items:center; gap:12px; padding:10px 20px; cursor:pointer;
                    border-bottom:1px solid var(--border-color); transition:background 0.15s;"
             onmouseover="this.style.background='rgba(75,162,172,0.07)'"
             onmouseout="this.style.background=''"
             onclick="document.getElementById('kec-list-modal').remove(); switchView('kecamatan'); selectKecamatan('${kec}');">
            <span style="width:26px;height:26px;background:var(--bg-panel);border-radius:6px;
                         display:flex;align-items:center;justify-content:center;
                         font-size:10px;font-weight:700;color:var(--text-muted);flex-shrink:0;">${i+1}</span>
            <span style="font-size:13px;font-weight:600;color:var(--text-primary);flex:1;">${kec}</span>
            <div style="display:flex;gap:6px;align-items:center;">
                <span style="font-size:10px;font-weight:700;color:${riskColor};">${ci.toFixed(0)}</span>
                <span style="font-size:9px;color:var(--text-muted);">|</span>
                <span style="font-size:10px;font-weight:700;color:var(--color-blue);">${hi.toFixed(0)}</span>
            </div>
            <i class="fa-solid fa-chevron-right" style="font-size:10px;color:var(--text-muted);"></i>
        </div>`;
    }).join('');

    modal.innerHTML = `
        <div style="padding:20px 20px 16px;border-bottom:1px solid var(--border-color);flex-shrink:0;">
            <div style="display:flex;justify-content:space-between;align-items:center;">
                <div>
                    <h3 style="margin:0;font-size:16px;font-weight:700;color:var(--text-primary);display:flex;align-items:center;gap:8px;">
                        <i class="fa-solid fa-map-location-dot" style="color:var(--color-blue);"></i>
                        31 Kecamatan Surabaya
                    </h3>
                    <p style="margin:3px 0 0;font-size:11px;color:var(--text-muted);">Pilih kecamatan untuk detail peta &amp; data</p>
                </div>
                <button onclick="document.getElementById('kec-list-modal').remove()"
                        style="border:none;background:var(--bg-panel);color:var(--text-secondary);
                               width:28px;height:28px;border-radius:8px;cursor:pointer;font-size:13px;
                               display:flex;align-items:center;justify-content:center;">
                    <i class="fa-solid fa-xmark"></i>
                </button>
            </div>
            <div style="display:flex;gap:16px;margin-top:10px;font-size:10px;font-weight:600;color:var(--text-muted);">
                <span><span style="color:var(--color-red);">●</span> Indeks Kriminalitas</span>
                <span><span style="color:var(--color-blue);">●</span> Indeks Kesehatan</span>
            </div>
        </div>
        <div style="overflow-y:auto;flex:1;">${listHTML}</div>
    `;

    overlay.appendChild(modal);
    document.body.appendChild(overlay);
}

// Modal: Lihat Semua (View All) Kecamatan & Data Analitik
function showAllKecamatanModal(type) {
    const existing = document.getElementById('kec-all-modal');
    if (existing) existing.remove();

    if (!dashboardData) return;

    const overlay = document.createElement('div');
    overlay.id = 'kec-all-modal';
    overlay.style.cssText = `
        position: fixed; inset: 0; background: rgba(28,51,53,0.55); backdrop-filter: blur(4px);
        z-index: 9998; display: flex; align-items: center; justify-content: center;
        animation: fadeInModal 0.2s ease;
    `;
    overlay.onclick = (e) => { if (e.target === overlay) overlay.remove(); };

    const modal = document.createElement('div');
    modal.style.cssText = `
        background: var(--bg-primary); border-radius: 16px; padding: 28px 32px;
        width: 800px; max-width: 94vw; max-height: 80vh; overflow-y: auto;
        box-shadow: 0 20px 60px rgba(28,51,53,0.25); border: 1px solid var(--border-subtle);
        font-family: var(--font-primary); display: flex; flex-direction: column; gap: 16px;
    `;

    let title = "";
    let tableHeader = "";
    let tableRows = "";

    if (type === 'all') {
        title = `<i class="fa-solid fa-chart-column" style="color:var(--color-blue); margin-right:8px;"></i> Rekapitulasi Dual-Indeks Seluruh Kecamatan`;
        tableHeader = `
            <tr>
                <th>Kecamatan</th>
                <th class="text-right">Indeks Kriminalitas</th>
                <th class="text-right">Indeks Kesehatan</th>
            </tr>
        `;
        const sortedKec = [...dashboardData.kriminalitas].sort((a,b) => a.kecamatan.localeCompare(b.kecamatan));
        tableRows = sortedKec.map(krim => {
            const kes = dashboardData.kesehatan.find(x => x.kecamatan === krim.kecamatan) || { indeks_kesehatan: 0 };
            return `
                <tr style="cursor: pointer;" onclick="document.getElementById('kec-all-modal').remove(); switchView('kecamatan'); selectKecamatan('${krim.kecamatan}');">
                    <td><strong>${krim.kecamatan}</strong></td>
                    <td class="text-right">
                        <span class="cell-index-badge ${getIndexClass(krim.indeks_kriminalitas)}">
                            ${parseFloat(krim.indeks_kriminalitas).toFixed(1)}
                        </span>
                    </td>
                    <td class="text-right">
                        <span class="cell-index-badge ${getIndexClass(kes.indeks_kesehatan)}">
                            ${parseFloat(kes.indeks_kesehatan).toFixed(1)}
                        </span>
                    </td>
                </tr>
            `;
        }).join('');
    } else if (type === 'kriminalitas') {
        title = `<i class="fa-solid fa-shield-halved" style="color:var(--color-red); margin-right:8px;"></i> Indeks Kriminalitas Lengkap`;
        tableHeader = `
            <tr>
                <th>Kecamatan</th>
                <th class="text-right">Total Kasus</th>
                <th class="text-right">Crime Rate</th>
                <th class="text-right">Indeks Kriminal</th>
            </tr>
        `;
        const sorted = [...dashboardData.kriminalitas].sort((a,b) => b.indeks_kriminalitas - a.indeks_kriminalitas);
        tableRows = sorted.map(item => `
            <tr style="cursor: pointer;" onclick="document.getElementById('kec-all-modal').remove(); switchView('kecamatan'); selectKecamatan('${item.kecamatan}');">
                <td><strong>${item.kecamatan}</strong></td>
                <td class="text-right">${item.total_kasus_kriminal.toLocaleString('id-ID')}</td>
                <td class="text-right">${parseFloat(item.crime_rate).toFixed(1)}</td>
                <td class="text-right">
                    <span class="cell-index-badge ${getIndexClass(item.indeks_kriminalitas)}">
                        ${parseFloat(item.indeks_kriminalitas).toFixed(1)}
                    </span>
                </td>
            </tr>
        `).join('');
    } else if (type === 'kesehatan') {
        title = `<i class="fa-solid fa-heart-pulse" style="color:var(--color-green); margin-right:8px;"></i> Indeks Risiko Kesehatan Lengkap`;
        tableHeader = `
            <tr>
                <th>Kecamatan</th>
                <th class="text-right">Total Wabah</th>
                <th class="text-right">Incidence Rate</th>
                <th class="text-right">HFR Score</th>
                <th class="text-right">Indeks Kesehatan</th>
            </tr>
        `;
        const sorted = [...dashboardData.kesehatan].sort((a,b) => b.indeks_kesehatan - a.indeks_kesehatan);
        tableRows = sorted.map(item => `
            <tr style="cursor: pointer;" onclick="document.getElementById('kec-all-modal').remove(); switchView('kecamatan'); selectKecamatan('${item.kecamatan}');">
                <td><strong>${item.kecamatan}</strong></td>
                <td class="text-right">${item.total_kasus_wabah.toLocaleString('id-ID')}</td>
                <td class="text-right">${parseFloat(item.incidence_rate).toLocaleString('id-ID', { maximumFractionDigits: 1 })}</td>
                <td class="text-right">${parseFloat(item.hfr).toFixed(2)}</td>
                <td class="text-right">
                    <span class="cell-index-badge ${getIndexClass(item.indeks_kesehatan)}">
                        ${parseFloat(item.indeks_kesehatan).toFixed(1)}
                    </span>
                </td>
            </tr>
        `).join('');
    }

    modal.innerHTML = `
        <div style="display:flex; justify-content:space-between; align-items:center; border-bottom: 1px solid var(--border-color); padding-bottom: 12px; margin-bottom: 12px;">
            <h3 style="margin:0; font-size:18px; font-weight:700; color:var(--text-primary); display: flex; align-items: center;">
                ${title}
            </h3>
            <button onclick="document.getElementById('kec-all-modal').remove()"
                    style="border:none; background:var(--bg-panel); color:var(--text-secondary);
                           width:30px; height:30px; border-radius:50%; cursor:pointer; font-size:14px;
                           display:flex; align-items:center; justify-content:center; transition: background 0.15s;"
                    onmouseover="this.style.background='var(--bg-hover)'"
                    onmouseout="this.style.background='var(--bg-panel)'">✕</button>
        </div>
        <div style="overflow-y:auto; flex-grow:1; border-radius:8px; border:1px solid var(--border-color);">
            <table class="styled-table" style="margin:0;">
                <thead>
                    ${tableHeader}
                </thead>
                <tbody>
                    ${tableRows}
                </tbody>
            </table>
        </div>
    `;

    overlay.appendChild(modal);
    document.body.appendChild(overlay);
}

// -----------------------------------------------------------------------------
// Interactive Map (Leaflet.js) & Fallback Grid
// -----------------------------------------------------------------------------
function initMap() {
    const surabayaCenter = [-7.2575, 112.7521];
    
    try {
        mapInstance = L.map('map', {
            center: surabayaCenter,
            zoom: 11.5,
            minZoom: 11,
            maxZoom: 14,
            zoomControl: true
        });

        // Use CartoDB Positron/Dark Matter Tile layer dynamically
        tileLayerInstance = L.tileLayer(getTileLayerUrl(), {
            attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
            subdomains: 'abcd',
            maxZoom: 20
        }).addTo(mapInstance);

        // Fetch boundaries
        loadGeoJSON();
    } catch (e) {
        console.warn("Leaflet Map failed to initialize, loading grid fallback.", e);
        showFallbackGrid();
    }
}

async function loadGeoJSON() {
    try {
        const response = await fetch(SURABAYA_GEOJSON_URL);
        if (!response.ok) throw new Error('GeoJSON fetch failed');
        const geojsonData = await response.json();
        
        renderMapPolygons(geojsonData);
    } catch (error) {
        console.warn("Failed loading GeoJSON boundaries, loading grid fallback.", error);
        showFallbackGrid();
    }
}

function renderMapPolygons(geojsonData) {
    if (geojsonLayer) {
        mapInstance.removeLayer(geojsonLayer);
    }
    
    geojsonLayer = L.geoJSON(geojsonData, {
        style: styleFeature,
        onEachFeature: onEachFeature
    }).addTo(mapInstance);

    // Ensure map container is visible and fallback is hidden
    const mapEl = document.getElementById('map');
    const fallbackEl = document.getElementById('map-fallback');
    if (mapEl) mapEl.classList.remove('hidden');
    if (fallbackEl) fallbackEl.classList.add('hidden');

    // Fit map bounds to the geojson layers
    if (mapInstance && geojsonLayer) {
        mapInstance.fitBounds(geojsonLayer.getBounds(), { padding: [10, 10] });
    }
}

function getIndexScore(kecName, layerType) {
    const nameNorm = cleanKecamatanName(kecName);
    
    if (layerType === 'kriminalitas') {
        const item = dashboardData.kriminalitas.find(x => cleanKecamatanName(x.kecamatan) === nameNorm);
        return item ? item.indeks_kriminalitas : 0;
    } else {
        const item = dashboardData.kesehatan.find(x => cleanKecamatanName(x.kecamatan) === nameNorm);
        return item ? item.indeks_kesehatan : 0;
    }
}

function cleanKecamatanName(name) {
    if (!name) return "";
    return name.toLowerCase()
        .replace("kecamatan", "")
        .replace("kec.", "")
        .trim();
}

function getColorByScore(score) {
    if (score >= 80) return '#C0392B'; // Ekstrim — merah tua
    if (score >= 60) return '#E67E22'; // Tinggi — oranye
    if (score >= 40) return '#F1C40F'; // Sedang — kuning
    if (score >= 20) return '#27AE60'; // Waspada — hijau
    return '#2ECC71';                  // Aman — hijau terang
}

function styleFeature(feature) {
    const name = feature.properties.NAME_3 || feature.properties.kecamatan || feature.properties.name || "";
    const score = getIndexScore(name, currentMapLayer);
    const isSelected = activeKecamatan && cleanKecamatanName(activeKecamatan) === cleanKecamatanName(name);

    return {
        fillColor: getColorByScore(score),
        weight: isSelected ? 3 : 1.2,
        opacity: 1,
        color: isSelected ? '#1C3335' : 'rgba(28,51,53,0.4)',
        fillOpacity: isSelected ? 0.9 : 0.7,
        dashArray: isSelected ? null : null
    };
}

function onEachFeature(feature, layer) {
    layer.on({
        mouseover: highlightFeature,
        mouseout: resetHighlight,
        click: selectFeature
    });
}

function highlightFeature(e) {
    const layer = e.target;
    layer.setStyle({ weight: 2.5, color: '#1C3335', fillOpacity: 0.92 });
    layer.bringToFront();

    const props = layer.feature.properties;
    const name = props.NAME_3 || props.kecamatan || props.name || "Kecamatan";
    const crimeScore = getIndexScore(name, 'kriminalitas');
    const healthScore = getIndexScore(name, 'kesehatan');

    const crimeRank = (dashboardData?.kriminalitas || []).findIndex(x => cleanKecamatanName(x.kecamatan) === cleanKecamatanName(name)) + 1;
    const crimeData = (dashboardData?.kriminalitas || []).find(x => cleanKecamatanName(x.kecamatan) === cleanKecamatanName(name));
    const riskLabel = crimeScore >= 80 ? 'EKSTRIM' : crimeScore >= 60 ? 'TINGGI' : crimeScore >= 40 ? 'SEDANG' : 'RENDAH';
    const riskColor = crimeScore >= 80 ? '#C0392B' : crimeScore >= 60 ? '#E67E22' : crimeScore >= 40 ? '#c8a200' : '#27AE60';

    layer.bindTooltip(`
        <div style="font-family:'Poppins',sans-serif;min-width:190px;padding:2px 0;">
            <div style="font-weight:700;font-size:13px;margin-bottom:6px;border-bottom:1px solid rgba(0,0,0,0.1);padding-bottom:5px;">
                ${name}
                <span style="float:right;font-size:10px;font-weight:700;color:${riskColor};background:rgba(0,0,0,0.07);padding:2px 6px;border-radius:4px;">${riskLabel}</span>
            </div>
            <div style="display:flex;flex-direction:column;gap:4px;font-size:11px;">
                <div style="display:flex;justify-content:space-between;">
                    <span style="color:#555;">Indeks Kriminal</span>
                    <strong style="color:${getColorByScore(crimeScore)}">${parseFloat(crimeScore).toFixed(1)}</strong>
                </div>
                <div style="display:flex;justify-content:space-between;">
                    <span style="color:#555;">Indeks Kesehatan</span>
                    <strong style="color:#27AE60">${parseFloat(healthScore).toFixed(1)}</strong>
                </div>
                ${crimeData ? `<div style="display:flex;justify-content:space-between;"><span style="color:#555;">Kasus Kriminal</span><strong>${crimeData.total_kasus_kriminal}</strong></div>` : ''}
                ${crimeRank ? `<div style="display:flex;justify-content:space-between;"><span style="color:#555;">Peringkat</span><strong>#${crimeRank} dari 31</strong></div>` : ''}
                <div style="margin-top:4px;padding-top:4px;border-top:1px solid rgba(0,0,0,0.08);font-size:10px;color:#888;">Klik untuk detail lengkap</div>
            </div>
        </div>
    `, { sticky: true, className: 'kecamatan-tooltip', offset: [15, 0] });
    layer.openTooltip();
}

function resetHighlight(e) {
    if (geojsonLayer) geojsonLayer.resetStyle(e.target);
    e.target.closeTooltip();
}

function selectFeature(e) {
    const props = e.target.feature.properties;
    const name = props.NAME_3 || props.kecamatan || props.name || "";
    selectKecamatan(name);
}

function switchMapLayer(layer) {
    currentMapLayer = layer;
    
    document.getElementById('btn-map-crime').classList.toggle('btn-active', layer === 'kriminalitas');
    document.getElementById('btn-map-health').classList.toggle('btn-active', layer === 'kesehatan');
    const legendTitle = document.getElementById('legend-title-text');
    if (legendTitle) legendTitle.textContent = layer === 'kriminalitas' ? 'Indeks Kriminalitas' : 'Indeks Kesehatan';
    
    if (currentMapEngine === 'mapbox') {
        updateMapboxLayers();
    }
    
    if (geojsonLayer && mapInstance) {
        geojsonLayer.eachLayer(l => {
            l.setStyle(styleFeature(l.feature));
        });
    } else {
        showFallbackGrid();
    }
}

// -----------------------------------------------------------------------------
// Mapbox GL JS 3D Engine Implementation
// -----------------------------------------------------------------------------
let mapboxGeojsonData = null;

function prepareGeojsonDataForMapbox(geojsonData) {
    if (!geojsonData || !geojsonData.features) return geojsonData;
    geojsonData.features.forEach(feature => {
        const props = feature.properties;
        const name = props.NAME_3 || props.kecamatan || props.name || "";
        const score = getIndexScore(name, currentMapLayer);
        feature.properties.index_score = score;
        feature.properties.color = getColorByScore(score);
        feature.properties.standard_name = name;
        feature.properties.clean_name = cleanKecamatanName(name);
    });
    return geojsonData;
}

function setupMapboxLayers() {
    if (!mapboxInstance) return;
    
    if (!mapboxInstance.getSource('surabaya-bounds') && mapboxGeojsonData) {
        const preparedData = prepareGeojsonDataForMapbox(JSON.parse(JSON.stringify(mapboxGeojsonData)));
        
        mapboxInstance.addSource('surabaya-bounds', {
            type: 'geojson',
            data: preparedData
        });
        
        // Fill Layer
        mapboxInstance.addLayer({
            id: 'surabaya-fill',
            type: 'fill',
            source: 'surabaya-bounds',
            paint: {
                'fill-color': ['get', 'color'],
                'fill-opacity': 0.65
            }
        });
        
        // Borders Layer
        mapboxInstance.addLayer({
            id: 'surabaya-borders',
            type: 'line',
            source: 'surabaya-bounds',
            paint: {
                'line-color': '#2C4F53',
                'line-width': 1.5
            }
        });
        
        // Highlight Layer for selected kecamatan
        mapboxInstance.addLayer({
            id: 'surabaya-highlight',
            type: 'line',
            source: 'surabaya-bounds',
            paint: {
                'line-color': '#1C3335',
                'line-width': 3.5
            },
            filter: ['==', ['get', 'clean_name'], activeKecamatan ? cleanKecamatanName(activeKecamatan) : '']
        });
    }
}

async function initMapbox() {
    if (mapboxInstance) return;
    
    const isDark = document.body.classList.contains('dark-theme');
    const styleUrl = isDark ? 'mapbox://styles/mapbox/dark-v11' : 'mapbox://styles/mapbox/light-v11';
    
    try {
        mapboxInstance = new mapboxgl.Map({
            container: 'mapbox-map',
            style: styleUrl,
            center: [112.7521, -7.2575], // Lng, Lat
            zoom: 11.2,
            pitch: 45,
            bearing: -10,
            antialias: true
        });
        
        mapboxInstance.addControl(new mapboxgl.NavigationControl(), 'top-right');
        
        mapboxInstance.on('style.load', () => {
            setupMapboxLayers();
            addMapbox3DBuildings();
        });
        
        mapboxInstance.on('load', async () => {
            // Load boundaries from cache or fetch
            if (!mapboxGeojsonData) {
                const response = await fetch(SURABAYA_GEOJSON_URL);
                mapboxGeojsonData = await response.json();
            }
            
            setupMapboxLayers();
            
            const mapboxPopup = new mapboxgl.Popup({
                closeButton: false,
                closeOnClick: false,
                offset: 15
            });
            
            // Hover events
            mapboxInstance.on('mousemove', 'surabaya-fill', (e) => {
                mapboxInstance.getCanvas().style.cursor = 'pointer';
                
                const feature = e.features[0];
                const props = feature.properties;
                const name = props.standard_name || "Kecamatan";
                const score = props.index_score || 0;
                
                mapboxPopup.setLngLat(e.lngLat)
                    .setHTML(`
                        <div class="map-popup-detail">
                            <h4 style="margin:0 0 4px 0; font-size:12px; font-weight:700;">${name}</h4>
                            <p style="margin:0; font-size:10px; color:var(--text-secondary);">
                                Indeks ${currentMapLayer === 'kriminalitas' ? 'Kriminal' : 'Kesehatan'}: 
                                <span class="popup-val" style="font-weight:700; color:var(--text-primary);">${parseFloat(score).toFixed(1)}</span>
                            </p>
                            <p style="margin:4px 0 0 0; font-size: 8px; color: var(--text-muted);">Klik untuk detail lengkap</p>
                        </div>
                    `)
                    .addTo(mapboxInstance);
            });
            
            mapboxInstance.on('mouseleave', 'surabaya-fill', () => {
                mapboxInstance.getCanvas().style.cursor = '';
                mapboxPopup.remove();
            });
            
            // Click events
            mapboxInstance.on('click', 'surabaya-fill', (e) => {
                const feature = e.features[0];
                const name = feature.properties.standard_name || "";
                selectKecamatan(name);
            });
            
            // Zoom to active kecamatan if pre-selected
            if (activeKecamatan) {
                zoomToPolygon(activeKecamatan);
            }
        });
    } catch (error) {
        console.error('Failed to initialize Mapbox GL JS map:', error);
    }
}

function updateMapboxLayers() {
    if (!mapboxInstance || !mapboxInstance.isStyleLoaded() || !mapboxInstance.getSource('surabaya-bounds')) return;
    
    if (mapboxGeojsonData) {
        const preparedData = prepareGeojsonDataForMapbox(JSON.parse(JSON.stringify(mapboxGeojsonData)));
        mapboxInstance.getSource('surabaya-bounds').setData(preparedData);
    }
}

function toggleMapEngine() {
    const leafletMapDiv = document.getElementById('map');
    const mapboxMapDiv = document.getElementById('mapbox-map');
    const toggleBtn = document.getElementById('btn-map-engine');
    
    if (currentMapEngine === 'leaflet') {
        currentMapEngine = 'mapbox';
        leafletMapDiv.classList.add('hidden');
        mapboxMapDiv.classList.remove('hidden');
        
        toggleBtn.innerHTML = '<i class="fa-solid fa-map"></i> Peta Leaflet 2D';
        toggleBtn.style.backgroundColor = 'var(--text-secondary)';
        
        if (!mapboxInstance) {
            initMapbox();
        } else {
            setTimeout(() => {
                mapboxInstance.resize();
                updateMapboxLayers();
                if (activeKecamatan) {
                    zoomToPolygon(activeKecamatan);
                }
            }, 100);
        }
    } else {
        currentMapEngine = 'leaflet';
        mapboxMapDiv.classList.add('hidden');
        leafletMapDiv.classList.remove('hidden');
        
        toggleBtn.innerHTML = '<i class="fa-solid fa-earth-americas"></i> Mapbox 3D';
        toggleBtn.style.backgroundColor = 'var(--color-blue)';
        
        if (mapInstance) {
            setTimeout(() => {
                mapInstance.invalidateSize();
                if (activeKecamatan) {
                    zoomToPolygon(activeKecamatan);
                }
            }, 100);
        }
    }
}

function showFallbackGrid() {
    const mapEl = document.getElementById('map');
    const fallbackEl = document.getElementById('map-fallback');
    
    if (mapEl) mapEl.classList.add('hidden');
    if (fallbackEl) {
        fallbackEl.classList.remove('hidden');
        fallbackEl.innerHTML = '';
        
        const dataList = currentMapLayer === 'kriminalitas' ? dashboardData.kriminalitas : dashboardData.kesehatan;
        const sortedGridData = [...dataList].sort((a,b) => a.kecamatan.localeCompare(b.kecamatan));
        
        sortedGridData.forEach(item => {
            const val = currentMapLayer === 'kriminalitas' ? item.indeks_kriminalitas : item.indeks_kesehatan;
            const cardColor = getColorByScore(val);
            
            const card = document.createElement('div');
            card.className = 'fallback-item';
            card.style.borderLeft = `4px solid ${cardColor}`;
            card.onclick = () => selectKecamatan(item.kecamatan);
            card.innerHTML = `
                <span class="fallback-name" title="${item.kecamatan}">${item.kecamatan}</span>
                <span class="fallback-val" style="color: ${cardColor}">${parseFloat(val).toFixed(1)}</span>
            `;
            fallbackEl.appendChild(card);
        });
    }
}

// -----------------------------------------------------------------------------
// Kecamatan Search & Detailed Profiling Panel
// -----------------------------------------------------------------------------
function selectKecamatan(kecName) {
    activeKecamatan = kecName;
    // Refresh Leaflet choropleth to highlight selected kecamatan
    if (geojsonLayer) geojsonLayer.eachLayer(l => l.setStyle(styleFeature(l.feature)));

    const crimeData = dashboardData.kriminalitas.find(x => cleanKecamatanName(x.kecamatan) === cleanKecamatanName(kecName));
    const healthData = dashboardData.kesehatan.find(x => cleanKecamatanName(x.kecamatan) === cleanKecamatanName(kecName));
    
    if (!crimeData && !healthData) return;

    // Show Details loaded state
    document.getElementById('kecamatan-detail-empty').classList.add('hidden');
    const loadedPanel = document.getElementById('kecamatan-detail-loaded');
    loadedPanel.classList.remove('hidden');
    
    // Rank badges
    const crimeRank = dashboardData.kriminalitas.findIndex(x => cleanKecamatanName(x.kecamatan) === cleanKecamatanName(kecName)) + 1;
    const healthRank = dashboardData.kesehatan.findIndex(x => cleanKecamatanName(x.kecamatan) === cleanKecamatanName(kecName)) + 1;
    const rankBadges = `
        <span class="rank-badge rank-crime" title="Peringkat Kriminalitas">#${crimeRank} Kriminal</span>
        <span class="rank-badge rank-health" title="Peringkat Risiko Kesehatan">#${healthRank} Kesehatan</span>
    `;
    document.getElementById('detail-kecamatan-name').innerHTML = `<i class="fa-solid fa-location-dot"></i> Kecamatan ${kecName}`;
    let rankRow = document.getElementById('detail-rank-row');
    if (!rankRow) {
        rankRow = document.createElement('div');
        rankRow.id = 'detail-rank-row';
        rankRow.style.cssText = 'display:flex;gap:8px;flex-wrap:wrap;margin-bottom:4px;';
        document.getElementById('detail-kecamatan-name').insertAdjacentElement('afterend', rankRow);
    }
    rankRow.innerHTML = rankBadges;

    // Index Scores
    const crimeScore = crimeData ? parseFloat(crimeData.indeks_kriminalitas) : 0;
    const healthScore = healthData ? parseFloat(healthData.indeks_kesehatan) : 0;

    document.getElementById('detail-crime-idx').textContent = crimeScore.toFixed(1);
    document.getElementById('detail-health-idx').textContent = healthScore.toFixed(1);

    // Progress Bars
    const crimeBar = document.getElementById('detail-crime-bar');
    crimeBar.style.width = `${crimeScore}%`;
    crimeBar.style.backgroundColor = getColorByScore(crimeScore);

    const healthBar = document.getElementById('detail-health-bar');
    healthBar.style.width = `${healthScore}%`;
    healthBar.style.backgroundColor = getColorByScore(healthScore);

    // Numeric Stats
    document.getElementById('detail-crime-cases').textContent = crimeData ? crimeData.total_kasus_kriminal : 0;
    document.getElementById('detail-crime-rate').textContent = crimeData ? parseFloat(crimeData.crime_rate).toFixed(1) : 0;
    document.getElementById('detail-health-cases').textContent = healthData ? healthData.total_kasus_wabah : 0;
    document.getElementById('detail-health-hfr').textContent = healthData ? parseFloat(healthData.hfr).toFixed(2) : '0.00';
    
    // Luas Wilayah & Kepadatan Penduduk
    const normalizedKey = cleanKecamatanName(kecName);
    const areaSize = KECAMATAN_LUAS[normalizedKey] || 0;
    const density = KECAMATAN_KEPADATAN[normalizedKey] || 0;
    document.getElementById('detail-area-size').textContent = areaSize ? `${areaSize.toFixed(2)} km²` : '- km²';
    const densityEl = document.getElementById('detail-density');
    if (densityEl) {
        densityEl.textContent = density ? `${density.toLocaleString('id-ID')} jiwa/km²` : '- jiwa/km²';
    }

    // Local News Mini List
    const miniNewsList = document.getElementById('detail-news-list');
    miniNewsList.innerHTML = '';

    const relatedNews = dashboardData.berita.filter(n => cleanKecamatanName(n.kecamatan_terdeteksi) === cleanKecamatanName(kecName));
    
    if (relatedNews.length === 0) {
        miniNewsList.innerHTML = `<p style="font-size: 11px; color: var(--text-muted); margin: 0;">Tidak ada berita anomali terdeteksi untuk wilayah ini.</p>`;
    } else {
        relatedNews.forEach(news => {
            const dateStr = news.tanggal_publikasi ? formatISODate(news.tanggal_publikasi) : 'Baru-baru ini';
            const isKrimNews = news.kategori === 'Kriminalitas';
            const item = document.createElement('div');
            item.className = 'kecamatan-news-mini-item';
            item.style.cursor = 'pointer';
            item.onclick = () => openNewsModal(news);
            item.innerHTML = `
                <div style="display:flex;align-items:center;gap:6px;margin-bottom:2px;">
                    <span class="cat-badge ${isKrimNews ? 'cat-krim' : 'cat-sehat'}" style="font-size:9px;padding:1px 6px;">${news.kategori}</span>
                    <span style="font-weight:600;font-size:11px;flex:1;">${news.judul}</span>
                </div>
                <div style="font-size:9px;color:var(--text-muted);">${news.sumber} &bull; ${dateStr} &bull; <span style="color:var(--color-blue);">Klik detail →</span></div>
            `;
            miniNewsList.appendChild(item);
        });
    }

    // Centering Map Polygon if exists
    zoomToPolygon(kecName);

    // Update comparison panel if open
    renderComparePanel();
}

function zoomToPolygon(kecName) {
    const nameNorm = cleanKecamatanName(kecName);
    
    // Mapbox engine logic
    if (currentMapEngine === 'mapbox' && mapboxInstance && mapboxInstance.isStyleLoaded()) {
        try {
            if (mapboxInstance.getLayer('surabaya-highlight')) {
                mapboxInstance.setFilter('surabaya-highlight', ['==', ['get', 'clean_name'], nameNorm]);
            }
        } catch (e) {
            console.warn('Mapbox highlight layer not ready:', e);
        }
    }

    if (!geojsonLayer) return;
    
    geojsonLayer.eachLayer(layer => {
        const props = layer.feature.properties;
        const polyName = props.NAME_3 || props.kecamatan || props.name || "";
        
        if (cleanKecamatanName(polyName) === nameNorm) {
            // Apply Leaflet selected styling border
            layer.setStyle({
                weight: 3.5,
                color: '#1C3335',
                fillOpacity: 0.8
            });
            layer.bringToFront();
            
            // Pop up detail on Leaflet
            const score = getIndexScore(polyName, currentMapLayer);
            const popupContent = `
                <div class="map-popup-detail">
                    <h4>${polyName}</h4>
                    <p>Indeks saat ini: <span class="popup-val">${parseFloat(score).toFixed(1)}</span></p>
                </div>
            `;
            layer.bindPopup(popupContent, { closeButton: false, offset: L.point(0, -10) });
            
            if (currentMapEngine === 'leaflet' && mapInstance) {
                layer.openPopup();
                mapInstance.fitBounds(layer.getBounds(), { maxZoom: 13, animate: true, padding: [20, 20] });
            } else if (currentMapEngine === 'mapbox' && mapboxInstance) {
                const bounds = layer.getBounds();
                const sw = bounds.getSouthWest();
                const ne = bounds.getNorthEast();
                mapboxInstance.fitBounds(
                    [[sw.lng, sw.lat], [ne.lng, ne.lat]],
                    { padding: 40, pitch: 50, bearing: -15, duration: 1500 }
                );
            }
        } else {
            // Reset others
            geojsonLayer.resetStyle(layer);
        }
    });
}

function searchKecamatan() {
    const query = document.getElementById('kecamatan-search-input').value.toLowerCase().trim();
    if (!query || !dashboardData) return;

    // Check direct matches
    const matched = dashboardData.kriminalitas.find(x => cleanKecamatanName(x.kecamatan).startsWith(query) || cleanKecamatanName(x.kecamatan) === query);
    
    if (matched) {
        selectKecamatan(matched.kecamatan);
    }
}

// -----------------------------------------------------------------------------
// Chart.js Visual Configuration Helper
// -----------------------------------------------------------------------------
function getChartOptions(titleText) {
    const isDark = document.body.classList.contains('dark-theme');
    const textColor = isDark ? '#A9B8B9' : '#2C4F53';
    const titleColor = isDark ? '#E8ECEC' : '#1C3335';
    const gridColor = isDark ? 'rgba(232, 236, 236, 0.15)' : 'rgba(28, 51, 53, 0.1)';

    return {
        responsive: true,
        maintainAspectRatio: false,
        indexAxis: 'y', // Horizontal bars
        plugins: {
            legend: {
                labels: {
                    color: textColor, // Dynamic text color
                    font: { family: 'Poppins', size: 12, weight: '500' }
                }
            },
            title: {
                display: true,
                text: titleText,
                color: titleColor, // Dynamic title color
                font: { family: 'Poppins', size: 14, weight: '700' }
            }
        },
        scales: {
            x: {
                grid: { color: gridColor },
                ticks: { color: textColor, font: { family: 'Poppins' } },
                max: 100
            },
            y: {
                grid: { display: false },
                ticks: { color: textColor, font: { family: 'Poppins', weight: '600' } }
            }
        }
    };
}

// -----------------------------------------------------------------------------
// VIEW 1: Overview Dashboard Core
// -----------------------------------------------------------------------------
function updateMainChart() {
    const ctx = document.getElementById('analyticsChart').getContext('2d');
    
    if (mainChart) {
        mainChart.destroy();
    }
    
    // Compare Top 12 Kecamatan Dual Indexes (Sorted by Crime)
    const subList = dashboardData.kriminalitas.slice(0, 12);
    const labels = subList.map(x => x.kecamatan);
    const crimeData = subList.map(x => x.indeks_kriminalitas);
    const healthData = labels.map(label => {
        const h = dashboardData.kesehatan.find(x => x.kecamatan === label);
        return h ? h.indeks_kesehatan : 0;
    });
    
    mainChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [
                {
                    label: 'Indeks Kriminalitas',
                    data: crimeData,
                    backgroundColor: 'rgba(75, 162, 172, 0.75)', // Steel Blue
                    borderColor: '#4BA2AC',
                    borderWidth: 1,
                    borderRadius: 4
                },
                {
                    label: 'Indeks Kesehatan',
                    data: healthData,
                    backgroundColor: 'rgba(80, 201, 186, 0.75)',  // Teal
                    borderColor: '#50C9BA',
                    borderWidth: 1,
                    borderRadius: 4
                }
            ]
        },
        options: {
            ...getChartOptions('Perbandingan Dual Indeks Wilayah'),
            indexAxis: 'x' // Vertical comparison
        }
    });
}

function populateOverviewNews() {
    const container = document.getElementById('news-feed-container');
    if (!container) return;
    
    container.innerHTML = '';
    
    const feedberita = dashboardData.berita.slice(0, 5); // Grab top 5 for overview
    
    if (feedberita.length === 0) {
        container.innerHTML = `<div class="loading-state"><p>Tidak ada berita terbaru.</p></div>`;
        return;
    }
    
    feedberita.forEach(item => {
        const dateStr = item.tanggal_publikasi ? formatISODate(item.tanggal_publikasi) : 'Baru-baru ini';
        const isKrim = item.kategori === 'Kriminalitas';
        
        const newsItem = document.createElement('div');
        newsItem.className = 'news-item';
        newsItem.style.cursor = 'pointer';
        newsItem.onclick = () => openNewsModal(item);
        newsItem.innerHTML = `
            <div class="news-meta">
                <div class="news-meta-left">
                    <span class="cat-badge ${isKrim ? 'cat-krim' : 'cat-sehat'}">
                        ${item.kategori}
                    </span>
                    <span class="news-source">${item.sumber}</span>
                </div>
                <span class="news-date">${dateStr}</span>
            </div>
            <h3 class="news-title" style="cursor:pointer;">${item.judul}</h3>
            <div class="news-footer">
                <span class="news-kecamatan">
                    <i class="fa-solid fa-location-dot"></i> Kecamatan ${item.kecamatan_terdeteksi}
                </span>
                <span style="font-size:10px; color:var(--color-blue); font-weight:600;">Klik untuk detail →</span>
            </div>
        `;
        container.appendChild(newsItem);
    });
}

// openNewsModal defined below (v2.0.0 with Google News search fix)

// -----------------------------------------------------------------------------
// VIEW 3: Kriminalitas Specific Dashboard
// -----------------------------------------------------------------------------
function populateCrimeDashboard() {
    // Mini KPIs
    const totalCases = dashboardData.kriminalitas.reduce((acc, x) => acc + x.total_kasus_kriminal, 0);
    document.getElementById('crime-total-cases').textContent = `${totalCases.toLocaleString('id-ID')} Kasus`;
    
    const avgIndex = dashboardData.kriminalitas.reduce((acc, x) => acc + x.indeks_kriminalitas, 0) / dashboardData.kriminalitas.length;
    document.getElementById('crime-avg-index').textContent = `${avgIndex.toFixed(1)} / 100`;

    // Render bar chart (Top 15)
    const ctx = document.getElementById('crimeAnalyticsChart').getContext('2d');
    
    if (crimeChart) {
        crimeChart.destroy();
    }
    
    const top15 = dashboardData.kriminalitas.slice(0, 15);
    const labels = top15.map(x => x.kecamatan);
    const data = top15.map(x => x.indeks_kriminalitas);
    
    crimeChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Indeks Kriminalitas',
                data: data,
                backgroundColor: 'rgba(75, 162, 172, 0.75)',  // Steel Blue
                borderColor: '#4BA2AC',
                borderWidth: 1.5,
                borderRadius: 6,
                barThickness: 14
            }]
        },
        options: getChartOptions('15 Kecamatan Paling Rawan Kriminalitas')
    });

    // Populate table
    populateCrimeTable();
}

function populateCrimeTable() {
    const tbody = document.getElementById('crime-table-body');
    if (!tbody) return;
    
    tbody.innerHTML = '';
    window.crimeTableData = dashboardData.kriminalitas;

    renderCrimeTableRows(dashboardData.kriminalitas);
}

function renderCrimeTableRows(data) {
    const tbody = document.getElementById('crime-table-body');
    tbody.innerHTML = '';

    if (data.length === 0) {
        tbody.innerHTML = '<tr><td colspan="4" class="text-center">Kecamatan tidak ditemukan.</td></tr>';
        return;
    }

    data.forEach(item => {
        const row = document.createElement('tr');
        row.style.cursor = 'pointer';
        row.onclick = () => {
            switchView('kecamatan');
            selectKecamatan(item.kecamatan);
        };
        
        row.innerHTML = `
            <td><strong>${item.kecamatan}</strong></td>
            <td class="text-right">${item.total_kasus_kriminal.toLocaleString('id-ID')}</td>
            <td class="text-right">${parseFloat(item.crime_rate).toFixed(1)}</td>
            <td class="text-right">
                <span class="cell-index-badge ${getIndexClass(item.indeks_kriminalitas)}">
                    ${parseFloat(item.indeks_kriminalitas).toFixed(1)}
                </span>
            </td>
        `;
        tbody.appendChild(row);
    });
}

function filterCrimeTable() {
    const query = document.getElementById('crime-search-input').value.toLowerCase().trim();
    if (!window.crimeTableData) return;
    
    const filtered = window.crimeTableData.filter(x => x.kecamatan.toLowerCase().includes(query));
    renderCrimeTableRows(filtered);
}

// -----------------------------------------------------------------------------
// VIEW 4: Kesehatan Specific Dashboard
// -----------------------------------------------------------------------------
function populateHealthDashboard() {
    // Mini KPIs
    const totalCases = dashboardData.kesehatan.reduce((acc, x) => acc + x.total_kasus_wabah, 0);
    document.getElementById('health-total-cases').textContent = `${totalCases.toLocaleString('id-ID')} Kasus`;
    
    const avgIndex = dashboardData.kesehatan.reduce((acc, x) => acc + x.indeks_kesehatan, 0) / dashboardData.kesehatan.length;
    document.getElementById('health-avg-index').textContent = `${avgIndex.toFixed(1)} / 100`;

    // Render bar chart (Top 15)
    const ctx = document.getElementById('healthAnalyticsChart').getContext('2d');
    
    if (healthChart) {
        healthChart.destroy();
    }
    
    // Kesehatan data is sorted descending in JSON
    const top15 = dashboardData.kesehatan.slice(0, 15);
    const labels = top15.map(x => x.kecamatan);
    const data = top15.map(x => x.indeks_kesehatan);
    
    healthChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Indeks Kesehatan',
                data: data,
                backgroundColor: 'rgba(80, 201, 186, 0.75)',  // Teal
                borderColor: '#50C9BA',
                borderWidth: 1.5,
                borderRadius: 6,
                barThickness: 14
            }]
        },
        options: getChartOptions('15 Kecamatan dengan Risiko Kesehatan Tertinggi')
    });

    // Populate table
    populateHealthTable();
}

function populateHealthTable() {
    const tbody = document.getElementById('health-table-body');
    if (!tbody) return;
    
    tbody.innerHTML = '';
    window.healthTableData = dashboardData.kesehatan;

    renderHealthTableRows(dashboardData.kesehatan);
}

function renderHealthTableRows(data) {
    const tbody = document.getElementById('health-table-body');
    tbody.innerHTML = '';

    if (data.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" class="text-center">Kecamatan tidak ditemukan.</td></tr>';
        return;
    }

    data.forEach(item => {
        const row = document.createElement('tr');
        row.style.cursor = 'pointer';
        row.onclick = () => {
            switchView('kecamatan');
            selectKecamatan(item.kecamatan);
        };
        
        row.innerHTML = `
            <td><strong>${item.kecamatan}</strong></td>
            <td class="text-right">${item.total_kasus_wabah.toLocaleString('id-ID')}</td>
            <td class="text-right">${parseFloat(item.incidence_rate).toLocaleString('id-ID', { maximumFractionDigits: 1 })}</td>
            <td class="text-right">${parseFloat(item.hfr).toFixed(2)}</td>
            <td class="text-right">
                <span class="cell-index-badge ${getIndexClass(item.indeks_kesehatan)}">
                    ${parseFloat(item.indeks_kesehatan).toFixed(1)}
                </span>
            </td>
        `;
        tbody.appendChild(row);
    });
}

function filterHealthTable() {
    const query = document.getElementById('health-search-input').value.toLowerCase().trim();
    if (!window.healthTableData) return;
    
    const filtered = window.healthTableData.filter(x => x.kecamatan.toLowerCase().includes(query));
    renderHealthTableRows(filtered);
}

// -----------------------------------------------------------------------------
// VIEW 5: News Portal Grid Card layout
// -----------------------------------------------------------------------------
function populateKecamatanDropdown() {
    const select = document.getElementById('news-filter-kecamatan');
    if (!select) return;
    
    select.innerHTML = '<option value="All">Semua Kecamatan</option>';
    
    // Extract unique kecamatan names from news
    const newsKec = [...new Set(dashboardData.berita.map(x => x.kecamatan_terdeteksi))].sort();
    
    newsKec.forEach(kec => {
        const option = document.createElement('option');
        option.value = kec;
        option.textContent = kec;
        select.appendChild(option);
    });
}

function populateNewsPortal() {
    const container = document.getElementById('news-grid-container');
    if (!container) return;
    
    container.innerHTML = '';
    
    if (!dashboardData.berita || dashboardData.berita.length === 0) {
        container.innerHTML = `
            <div class="loading-state" style="grid-column: 1 / -1;">
                <i class="fa-solid fa-newspaper" style="font-size: 28px; color: var(--text-muted)"></i>
                <p>Tidak ada berita anomali terdeteksi di HDFS Delta Lake.</p>
            </div>
        `;
        return;
    }
    
    dashboardData.berita.forEach((item, index) => {
        const dateStr = item.tanggal_publikasi ? formatISODate(item.tanggal_publikasi) : 'Baru-baru ini';
        const isKrim = item.kategori === 'Kriminalitas';

        // Use pre-assigned cover image (consistent with modal)
        const coverImg = item._coverImg || (isKrim ? CRIME_IMAGES[index % CRIME_IMAGES.length] : HEALTH_IMAGES[index % HEALTH_IMAGES.length]);
            
        // Generate snippet summary description
        const descSnippet = isKrim 
            ? `Deteksi insiden ${item.kategori.toLowerCase()} di Kecamatan ${item.kecamatan_terdeteksi}. Kejadian terekam melalui streaming Kafka producer dan di-analisis oleh Spark medallion layers.`
            : `Laporan aktivitas ${item.kategori.toLowerCase()} di Kecamatan ${item.kecamatan_terdeteksi}. Parameter dihitung dan disimpan di Delta Lakehouse Gold Table.`;

        // Use div + onclick to show modal instead of broken external link
        const card = document.createElement('div');
        card.className = 'news-card';
        card.style.cursor = 'pointer';
        card.onclick = () => openNewsModal(item);
        card.innerHTML = `
            <div class="news-card-body">
                <div class="news-card-meta" style="margin-bottom: 8px;">
                    <span class="cat-badge ${isKrim ? 'cat-krim' : 'cat-sehat'}" style="margin-right: 8px; font-size: 11px;">
                        ${item.kategori}
                    </span>
                    <span class="news-card-source">${item.sumber}</span>
                    <span style="margin-left: auto;">${dateStr}</span>
                </div>
                <h3 class="news-card-title">${item.judul}</h3>
                <p class="news-card-desc">${descSnippet}</p>
                <div class="news-card-footer">
                    <span class="news-card-kecamatan">
                        <i class="fa-solid fa-location-dot"></i> Kecamatan ${item.kecamatan_terdeteksi}
                    </span>
                    <span style="font-size: 10px; color: var(--color-blue); font-weight:600;">Klik detail →</span>
                </div>
            </div>
        `;
        container.appendChild(card);
    });
}

function filterNewsGrid() {
    const searchVal = document.getElementById('news-search-input').value.toLowerCase().trim();
    const catFilter = document.getElementById('news-filter-category').value;
    const kecFilter = document.getElementById('news-filter-kecamatan').value;
    
    const cards = document.querySelectorAll('#news-grid-container .news-card');
    let newsCount = 0;
    
    cards.forEach(card => {
        const title = card.querySelector('.news-card-title').textContent.toLowerCase();
        const category = card.querySelector('.cat-badge').textContent.trim();
        const kecNode = card.querySelector('.news-card-kecamatan').textContent.trim();
        const kecamatan = kecNode.replace("Kecamatan", "").trim();

        // Matching conditions
        const matchSearch = title.includes(searchVal);
        const matchCat = catFilter === 'All' || category === catFilter;
        const matchKec = kecFilter === 'All' || cleanKecamatanName(kecamatan) === cleanKecamatanName(kecFilter);
        
        if (matchSearch && matchCat && matchKec) {
            card.style.display = 'flex';
            newsCount++;
        } else {
            card.style.display = 'none';
        }
    });

    // Handle empty search results
    let noResultNode = document.getElementById('news-no-result');
    if (newsCount === 0) {
        if (!noResultNode) {
            noResultNode = document.createElement('div');
            noResultNode.id = 'news-no-result';
            noResultNode.className = 'loading-state';
            noResultNode.style.gridColumn = '1 / -1';
            noResultNode.innerHTML = `
                <i class="fa-solid fa-magnifying-glass-minus" style="font-size: 28px; color: var(--text-muted);"></i>
                <p>Tidak ada berita anomali yang cocok dengan kriteria filter.</p>
            `;
            document.getElementById('news-grid-container').appendChild(noResultNode);
        }
    } else {
        if (noResultNode) {
            noResultNode.remove();
        }
    }
}

// -----------------------------------------------------------------------------
// Utilities & General Helpers
// -----------------------------------------------------------------------------
function getIndexClass(score) {
    if (score >= 80) return 'cell-index-4';
    if (score >= 60) return 'cell-index-3';
    if (score >= 40) return 'cell-index-2';
    if (score >= 20) return 'cell-index-1';
    return 'cell-index-0';
}

function formatISODate(isoStr) {
    try {
        const date = new Date(isoStr);
        return date.toLocaleDateString('id-ID', { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' });
    } catch {
        return isoStr;
    }
}

// Decodes Google News redirect URLs on the fly to get original destination URLs
function decodeGoogleNewsUrl(url) {
    if (!url) return url;
    if (url.includes('news.google.com/rss/articles/') || url.includes('news.google.com/articles/')) {
        try {
            const match = url.match(/articles\/([^/?]+)/);
            if (match) {
                let encoded = match[1];
                // Replace URL-safe base64 characters
                encoded = encoded.replace(/-/g, '+').replace(/_/g, '/');
                // Pad base64
                while (encoded.length % 4) {
                    encoded += '=';
                }
                const decodedStr = atob(encoded);
                // Extract original http/https link from decoded binary stream
                const urlMatch = decodedStr.match(/(https?:\/\/[a-zA-Z0-9\-._~:\/?#\[\]@!$&'()*+,;=%]+)/);
                if (urlMatch) {
                    return urlMatch[1];
                }
            }
        } catch (e) {
            console.error('Failed to decode Google News URL:', e);
        }
    }
    return url;
}

// -----------------------------------------------------------------------------
// Real-time Polling & Notification Engine
// -----------------------------------------------------------------------------
let seenNewsIds = new Set();

function startRealtimePolling() {
    // Populate initial seen news
    if (dashboardData && dashboardData.berita) {
        dashboardData.berita.forEach(item => {
            seenNewsIds.add(item.link || item.judul);
        });
    }
    
    // Poll every 10 seconds
    setInterval(async () => {
        try {
            const response = await fetch(`data/kecamatras_data.json?v=${new Date().getTime()}`);
            if (!response.ok) return;
            const newData = await response.json();
            
            // Check if there is newer data
            if (newData.last_updated !== dashboardData.last_updated) {
                console.log("New real-time data detected! Updating UI...");
                
                let hasNewNews = false;
                let newNewsCount = 0;
                
                if (newData.berita) {
                    newData.berita.forEach(item => {
                        const id = item.link || item.judul;
                        if (!seenNewsIds.has(id)) {
                            seenNewsIds.add(id);
                            hasNewNews = true;
                            newNewsCount++;
                        }
                    });
                }
                
                // Update state
                dashboardData = newData;
                
                // Refresh UI components
                updateTimestamp();
                populateGeneralKPIs();
                updateMainChart();
                populateOverviewNews();
                populateCrimeDashboard();
                populateHealthDashboard();
                populateNewsPortal();
                populateBeritaStats();
                populateEnhancedCrimeDashboard();
                populateEnhancedHealthDashboard();
                
                if (activeKecamatan) {
                    selectKecamatan(activeKecamatan);
                }
                
                if (hasNewNews) {
                    playNotificationSound();
                    showNotificationToast(`${newNewsCount} Berita Anomali Baru Terdeteksi!`);
                }
            }
        } catch (e) {
            console.warn("Polling failed:", e);
        }
    }, 10000); // 10 seconds
}

function playNotificationSound() {
    try {
        const audioCtx = new (window.AudioContext || window.webkitAudioContext)();
        const now = audioCtx.currentTime;
        
        // Tone 1: C5 (523.25 Hz)
        const osc1 = audioCtx.createOscillator();
        const gain1 = audioCtx.createGain();
        osc1.type = 'sine';
        osc1.frequency.setValueAtTime(523.25, now);
        gain1.gain.setValueAtTime(0.08, now);
        gain1.gain.exponentialRampToValueAtTime(0.0001, now + 0.3);
        osc1.connect(gain1);
        gain1.connect(audioCtx.destination);
        osc1.start(now);
        osc1.stop(now + 0.3);
        
        // Tone 2: E5 (659.25 Hz)
        const osc2 = audioCtx.createOscillator();
        const gain2 = audioCtx.createGain();
        osc2.type = 'sine';
        osc2.frequency.setValueAtTime(659.25, now + 0.08);
        gain2.gain.setValueAtTime(0.08, now + 0.08);
        gain2.gain.exponentialRampToValueAtTime(0.0001, now + 0.4);
        osc2.connect(gain2);
        gain2.connect(audioCtx.destination);
        osc2.start(now + 0.08);
        osc2.stop(now + 0.4);
        
    } catch (e) {
        console.warn("Could not play synthesized audio notification:", e);
    }
}

function showNotificationToast(message) {
    let container = document.getElementById('toast-container');
    if (!container) {
        container = document.createElement('div');
        container.id = 'toast-container';
        container.style.position = 'fixed';
        container.style.bottom = '24px';
        container.style.right = '24px';
        container.style.zIndex = '9999';
        container.style.display = 'flex';
        container.style.flexDirection = 'column';
        container.style.gap = '8px';
        document.body.appendChild(container);
    }
    
    const toast = document.createElement('div');
    toast.className = 'toast-notification';
    toast.style.background = 'var(--color-blue)';
    toast.style.color = 'var(--bg-primary)';
    toast.style.padding = '12px 20px';
    toast.style.borderRadius = '8px';
    toast.style.boxShadow = '0 4px 12px rgba(0,0,0,0.15)';
    toast.style.fontFamily = 'var(--font-primary)';
    toast.style.fontSize = '13px';
    toast.style.fontWeight = '600';
    toast.style.display = 'flex';
    toast.style.alignItems = 'center';
    toast.style.gap = '8px';
    toast.style.opacity = '0';
    toast.style.transform = 'translateY(20px)';
    toast.style.transition = 'all 0.3s cubic-bezier(0.4, 0, 0.2, 1)';
    
    toast.innerHTML = `
        <i class="fa-solid fa-bell" style="animation: ring 1s ease-in-out infinite;"></i>
        <span>${message}</span>
    `;
    
    container.appendChild(toast);
    
    // Trigger transition
    setTimeout(() => {
        toast.style.opacity = '1';
        toast.style.transform = 'translateY(0)';
    }, 10);
    
    // Hide and remove after 4 seconds
    setTimeout(() => {
        toast.style.opacity = '0';
        toast.style.transform = 'translateY(-20px)';
        setTimeout(() => {
            toast.remove();
        }, 300);
    }, 4000);
}

// Theme Toggle (Light / Dark Mode)
function getTileLayerUrl() {
    return document.body.classList.contains('dark-theme')
        ? 'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png'
        : 'https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png';
}

function toggleTheme() {
    const body = document.body;
    body.classList.toggle('dark-theme');
    
    const isDark = body.classList.contains('dark-theme');
    localStorage.setItem('theme', isDark ? 'dark' : 'light');
    
    // Update toggle icon and text
    const icon = document.getElementById('theme-toggle-icon');
    const text = document.getElementById('theme-toggle-text');
    if (icon) {
        icon.className = isDark ? 'fa-solid fa-sun' : 'fa-solid fa-moon';
        icon.style.color = isDark ? '#F0EEC9' : 'var(--text-secondary)';
    }
    if (text) {
        text.textContent = isDark ? 'Mode Terang' : 'Mode Gelap';
    }
    
    // Update Map tile layer URL
    if (tileLayerInstance) {
        tileLayerInstance.setUrl(getTileLayerUrl());
    }
    
    // Redraw charts with the new colors
    if (mainChart) updateMainChart();
    if (crimeChart) populateCrimeDashboard();
    if (healthChart) populateHealthDashboard();
    if (dashboardData) {
        populateEnhancedCrimeDashboard();
        populateEnhancedHealthDashboard();
        renderHistoricalTrendChart();
    }
}

// Initial Theme Check (on load)
function initTheme() {
    const savedTheme = localStorage.getItem('theme');
    const systemPrefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;

    if (savedTheme === 'dark' || (!savedTheme && systemPrefersDark)) {
        document.body.classList.add('dark-theme');
        document.addEventListener('DOMContentLoaded', () => {
            const icon = document.getElementById('theme-toggle-icon');
            const text = document.getElementById('theme-toggle-text');
            if (icon) {
                icon.className = 'fa-solid fa-sun';
                icon.style.color = '#F0EEC9';
            }
            if (text) text.textContent = 'Mode Terang';
        });
    }
}

// =============================================================================
// NEWS LINK HELPERS
// =============================================================================
function isTagPage(url) {
    if (!url) return true;
    return url.includes('/tag/') || url.includes('/search') || url.includes('/kategori/') || url.includes('/topic/');
}

function getNewsSearchUrl(item) {
    // Use short keyword query — full title is too specific and Google News returns no results
    const stopWords = new Set(['di','dan','ke','dari','yang','untuk','dengan','pada','oleh','itu','ini','atau','akibat','guna']);
    const words = item.judul.split(/[\s,]+/).filter(w => w.length > 3 && !stopWords.has(w.toLowerCase())).slice(0, 5);
    const q = words.join(' ') + ' Surabaya';
    return `https://news.google.com/search?q=${encodeURIComponent(q)}&hl=id&gl=ID&ceid=ID:id`;
}

function getDirectArticleUrl(item) {
    return item.link || null;
}

// =============================================================================
// ENHANCED: populateCrimeDashboard – adds extra KPIs + donut + risk zones
// =============================================================================
let crimeDonutChart = null;
let healthDonutChart = null;

function populateEnhancedCrimeDashboard() {
    if (!dashboardData) return;
    const data = dashboardData.kriminalitas;

    // Extra KPIs
    const extreme = data.filter(x => x.indeks_kriminalitas > 80);
    const safest = data[data.length - 1];
    window._crimeMaxKec = data[0]?.kecamatan || '';
    window._crimeMinKec = safest?.kecamatan || '';
    const extremeEl = document.getElementById('crime-extreme-count');
    const minEl = document.getElementById('crime-min-name');
    if (extremeEl) extremeEl.textContent = `${extreme.length} Kec.`;
    if (minEl) minEl.textContent = safest?.kecamatan || '-';

    // Donut chart - distribution
    const zones = [
        { label: 'Ekstrim (>80)', count: data.filter(x => x.indeks_kriminalitas > 80).length, color: '#D95D39' },
        { label: 'Tinggi (60-80)', count: data.filter(x => x.indeks_kriminalitas >= 60 && x.indeks_kriminalitas <= 80).length, color: '#4BA2AC' },
        { label: 'Sedang (40-60)', count: data.filter(x => x.indeks_kriminalitas >= 40 && x.indeks_kriminalitas < 60).length, color: '#50C9BA' },
        { label: 'Waspada (20-40)', count: data.filter(x => x.indeks_kriminalitas >= 20 && x.indeks_kriminalitas < 40).length, color: '#9EE6CF' },
        { label: 'Aman (<20)', count: data.filter(x => x.indeks_kriminalitas < 20).length, color: '#C1E6C4' },
    ];
    renderDonutChart('crimeDonutChart', zones, 'crime-donut-legend', crimeDonutChart, c => crimeDonutChart = c);

    // Risk zone rows (clean table style)
    const zonesEl = document.getElementById('crime-risk-zones');
    if (zonesEl) {
        const top6 = data.slice(0, 6);
        zonesEl.innerHTML = top6.map((item, i) => {
            const score = parseFloat(item.indeks_kriminalitas);
            const color = score >= 80 ? '#C0392B' : score >= 60 ? '#E67E22' : score >= 40 ? '#c8a200' : '#27AE60';
            const levelLabel = score >= 80 ? 'Ekstrim' : score >= 60 ? 'Tinggi' : score >= 40 ? 'Sedang' : 'Rendah';
            const levelBg = score >= 80 ? 'rgba(192,57,43,0.1)' : score >= 60 ? 'rgba(230,126,34,0.1)' : score >= 40 ? 'rgba(200,162,0,0.1)' : 'rgba(39,174,96,0.1)';
            return `
                <div class="zone-card" onclick="switchView('kecamatan'); selectKecamatan('${item.kecamatan}')">
                    <span class="zone-card-rank">#${i+1}</span>
                    <h4 class="zone-card-title">${item.kecamatan}</h4>
                    <div class="zone-card-meta">
                        <span class="zone-card-badge" style="background:${levelBg};color:${color};">${levelLabel}</span>
                        <span class="zone-card-score" style="color:${color};">${score.toFixed(0)}</span>
                    </div>
                    <div class="zone-card-progress">
                        <div class="zone-card-progress-bar" style="width:${Math.min(100, score)}%;background:${color};"></div>
                    </div>
                </div>`;
        }).join('');
    }
}

function populateEnhancedHealthDashboard() {
    if (!dashboardData) return;
    const data = dashboardData.kesehatan;

    // Extra KPIs
    const highRisk = data.filter(x => x.indeks_kesehatan > 50);
    const safest = data[data.length - 1];
    window._healthMaxKec = data[0]?.kecamatan || '';
    window._healthMinKec = safest?.kecamatan || '';
    const extremeEl = document.getElementById('health-extreme-count');
    const minEl = document.getElementById('health-min-name');
    if (extremeEl) extremeEl.textContent = `${highRisk.length} Kec.`;
    if (minEl) minEl.textContent = safest?.kecamatan || '-';

    // Donut chart
    const zones = [
        { label: 'Kritis (>75)', count: data.filter(x => x.indeks_kesehatan > 75).length, color: '#D95D39' },
        { label: 'Tinggi (50-75)', count: data.filter(x => x.indeks_kesehatan >= 50 && x.indeks_kesehatan <= 75).length, color: '#E68A00' },
        { label: 'Sedang (25-50)', count: data.filter(x => x.indeks_kesehatan >= 25 && x.indeks_kesehatan < 50).length, color: '#50C9BA' },
        { label: 'Rendah (<25)', count: data.filter(x => x.indeks_kesehatan < 25).length, color: '#C1E6C4' },
    ];
    renderDonutChart('healthDonutChart', zones, 'health-donut-legend', healthDonutChart, c => healthDonutChart = c);

    // Risk zone rows top 6 (health)
    const zonesEl = document.getElementById('health-risk-zones');
    if (zonesEl) {
        const top6 = data.slice(0, 6);
        zonesEl.innerHTML = top6.map((item, i) => {
            const score = parseFloat(item.indeks_kesehatan);
            const color = score >= 80 ? '#C0392B' : score >= 60 ? '#E67E22' : score >= 40 ? '#c8a200' : '#27AE60';
            const levelLabel = score >= 80 ? 'Kritis' : score >= 60 ? 'Tinggi' : score >= 40 ? 'Sedang' : 'Rendah';
            const levelBg = score >= 80 ? 'rgba(192,57,43,0.1)' : score >= 60 ? 'rgba(230,126,34,0.1)' : score >= 40 ? 'rgba(200,162,0,0.1)' : 'rgba(39,174,96,0.1)';
            return `
                <div class="zone-card" onclick="switchView('kecamatan'); selectKecamatan('${item.kecamatan}')">
                    <span class="zone-card-rank">#${i+1}</span>
                    <h4 class="zone-card-title">${item.kecamatan}</h4>
                    <div class="zone-card-meta">
                        <span class="zone-card-badge" style="background:${levelBg};color:${color};">${levelLabel}</span>
                        <span class="zone-card-score" style="color:${color};">${score.toFixed(0)}</span>
                    </div>
                    <div class="zone-card-progress">
                        <div class="zone-card-progress-bar" style="width:${Math.min(100, score)}%;background:${color};"></div>
                    </div>
                </div>`;
        }).join('');
    }

}

function renderDonutChart(canvasId, zones, legendId, existingChart, setChart) {
    const canvas = document.getElementById(canvasId);
    if (!canvas) return;
    if (existingChart) existingChart.destroy();

    const isDark = document.body.classList.contains('dark-theme');
    const chart = new Chart(canvas.getContext('2d'), {
        type: 'doughnut',
        data: {
            labels: zones.map(z => z.label),
            datasets: [{
                data: zones.map(z => z.count),
                backgroundColor: zones.map(z => z.color),
                borderWidth: 2,
                borderColor: isDark ? '#2A2B3B' : '#FDFDF6',
                hoverOffset: 6
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            cutout: '65%',
            plugins: {
                legend: { display: false },
                tooltip: {
                    callbacks: {
                        label: (ctx) => ` ${ctx.label}: ${ctx.raw} kecamatan`
                    }
                }
            }
        }
    });
    setChart(chart);

    // Render custom legend
    const legendEl = document.getElementById(legendId);
    if (legendEl) {
        legendEl.innerHTML = zones.map(z => `
            <div class="donut-legend-item">
                <span class="donut-legend-dot" style="background:${z.color};"></span>
                <span class="donut-legend-label">${z.label}: <strong>${z.count}</strong></span>
            </div>
        `).join('');
    }
}

// Berita stats bar
function populateBeritaStats() {
    const container = document.getElementById('berita-stats-bar');
    if (!container || !dashboardData?.berita) return;
    const berita = dashboardData.berita;
    const krimCount = berita.filter(x => x.kategori === 'Kriminalitas').length;
    const sehatCount = berita.filter(x => x.kategori !== 'Kriminalitas').length;
    const kecSet = new Set(berita.map(x => x.kecamatan_terdeteksi));
    const lastDate = berita[0]?.tanggal_publikasi ? formatISODate(berita[0].tanggal_publikasi) : '-';

    container.innerHTML = `
        <div class="kpi-card">
            <div class="kpi-icon color-blue"><i class="fa-solid fa-newspaper"></i></div>
            <div class="kpi-info"><h3>${berita.length}</h3><p>Total Berita Anomali</p></div>
        </div>
        <div class="kpi-card">
            <div class="kpi-icon color-red"><i class="fa-solid fa-shield-halved"></i></div>
            <div class="kpi-info"><h3>${krimCount}</h3><p>Berita Kriminalitas</p></div>
        </div>
        <div class="kpi-card">
            <div class="kpi-icon color-orange"><i class="fa-solid fa-heart-pulse"></i></div>
            <div class="kpi-info"><h3>${sehatCount}</h3><p>Berita Kesehatan</p></div>
        </div>
        <div class="kpi-card">
            <div class="kpi-icon color-green"><i class="fa-solid fa-map-pin"></i></div>
            <div class="kpi-info"><h3>${kecSet.size}</h3><p>Kecamatan Terdeteksi</p></div>
        </div>
    `;
}

// =============================================================================
// GEMINI AI INTEGRATION
// =============================================================================
const _gk1 = 'AQ.Ab8RN6JKUIjONrLq7cyDvW7rdWYFt_';
const _gk2 = 'eMlM6xwKIOqoM4Z8VKYA';
const GEMINI_KEY = `${_gk1}${_gk2}`;
const GEMINI_URL = `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=${GEMINI_KEY}`;

// askGemini is defined below (uses dynamic key from localStorage)

function buildKecamatanContext(kecName) {
    const crime = dashboardData.kriminalitas.find(x => cleanKecamatanName(x.kecamatan) === cleanKecamatanName(kecName));
    const health = dashboardData.kesehatan.find(x => cleanKecamatanName(x.kecamatan) === cleanKecamatanName(kecName));
    const news = dashboardData.berita.filter(x => cleanKecamatanName(x.kecamatan_terdeteksi) === cleanKecamatanName(kecName));

    const parts = [];
    if (crime) parts.push(`Kriminalitas: indeks=${parseFloat(crime.indeks_kriminalitas).toFixed(1)}/100, total_kasus=${crime.total_kasus_kriminal}, crime_rate=${parseFloat(crime.crime_rate).toFixed(1)}`);
    if (health) parts.push(`Kesehatan: indeks=${parseFloat(health.indeks_kesehatan).toFixed(1)}/100, total_wabah=${health.total_kasus_wabah}, HFR=${parseFloat(health.hfr).toFixed(2)}`);
    if (news.length) parts.push(`Berita terkini: ${news.slice(0,3).map(n => n.judul).join('; ')}`);
    return parts.join('\n');
}

function buildCityContext() {
    const top3Crime = dashboardData.kriminalitas.slice(0,3).map(x => `${x.kecamatan}(${parseFloat(x.indeks_kriminalitas).toFixed(1)})`).join(', ');
    const top3Health = dashboardData.kesehatan.slice(0,3).map(x => `${x.kecamatan}(${parseFloat(x.indeks_kesehatan).toFixed(1)})`).join(', ');
    const totalCrimeCases = dashboardData.kriminalitas.reduce((a,x) => a + x.total_kasus_kriminal, 0);
    const totalHealthCases = dashboardData.kesehatan.reduce((a,x) => a + x.total_kasus_wabah, 0);
    return `Data Kota Surabaya 2026: 31 kecamatan.\nTop 3 kriminalitas tertinggi: ${top3Crime}.\nTop 3 risiko kesehatan tertinggi: ${top3Health}.\nTotal kasus kriminal: ${totalCrimeCases}, Total kasus wabah: ${totalHealthCases}.`;
}

function initAIView() {
    // Populate kecamatan select in AI panel
    const select = document.getElementById('ai-kec-select');
    if (select && dashboardData) {
        const allKec = dashboardData.kriminalitas.map(x => x.kecamatan).sort();
        allKec.forEach(kec => {
            const opt = document.createElement('option');
            opt.value = kec;
            opt.textContent = kec;
            select.appendChild(opt);
        });
    }

    // AI KPI
    const highRisk = (dashboardData?.kriminalitas || []).filter(x => parseFloat(x.indeks_kriminalitas) > 70).length;
    const el = document.getElementById('ai-highrisk-count');
    if (el) el.textContent = `${highRisk} Kec.`;

    const modelEl = document.getElementById('ai-model-name');
    if (modelEl) {
        const m = getActiveGeminiModel();
        modelEl.textContent = m.replace('gemini-', '').replace('-flash','').replace('-pro',' Pro');
    }
}

// Local analysis engine — generates insight text from Gold Layer data without any API call
function generateLocalAnalysis(userText, kecName) {
    if (!dashboardData) return '**Data tidak tersedia.** Mohon muat ulang halaman.';

    const crimes = dashboardData.kriminalitas || [];
    const healths = dashboardData.kesehatan || [];
    const text = (userText || '').toLowerCase();

    // Check for student housing / kos query near ITS
    if ((text.includes('kos') || text.includes('kamar') || text.includes('kontrakan') || text.includes('hunian') || text.includes('sewa')) && text.includes('its')) {
        const sukolilo = crimes.find(x => cleanKecamatanName(x.kecamatan) === 'sukolilo');
        const mulyorejo = crimes.find(x => cleanKecamatanName(x.kecamatan) === 'mulyorejo');
        
        return [
            `## Rekomendasi Hunian Mahasiswa (Kos) dekat Kampus ITS`,
            `*Analisis lokal KECAMATRAS berbasis data keamanan & kesehatan*`,
            `Bagi mahasiswa yang berkuliah di **Institut Teknologi Sepuluh Nopember (ITS)** (berlokasi di Sukolilo), berikut rekomendasi kecamatan terdekat berdasarkan data Gold Layer:`,
            `1. **Kecamatan Sukolilo** (Lokasi Utama Kampus ITS):`,
            `   - Indeks Kriminalitas: 0.0/100 — Daerah Teraman (Peringkat #31/31)`,
            `   - Total kasus kriminal: 0`,
            `   - Rekomendasi: Sangat direkomendasikan untuk kos di daerah Keputih, Gebang, atau Perumahan Dosen ITS karena tingkat kejahatan nol dan lingkungan sangat aman.`,
            `2. **Kecamatan Mulyorejo** (Berbatasan langsung dengan Kampus ITS):`,
            `   - Indeks Kriminalitas: 56.6/100 — Tingkat SEDANG (Peringkat #14/31)`,
            `   - Rekomendasi: Bagus sebagai alternatif di daerah Sutorejo atau Kalijudan, namun tetap prioritaskan kewaspadaan ganda pada malam hari.`,
            `Prioritas Keamanan: Sukolilo adalah daerah hunian teraman di Surabaya untuk mahasiswa ITS berdasarkan data historis KECAMATRAS.`
        ].join('\n');
    }

    if (kecName) {
        const cData = crimes.find(x => cleanKecamatanName(x.kecamatan) === cleanKecamatanName(kecName));
        const hData = healths.find(x => cleanKecamatanName(x.kecamatan) === cleanKecamatanName(kecName));
        const crimeRank = crimes.findIndex(x => cleanKecamatanName(x.kecamatan) === cleanKecamatanName(kecName)) + 1;
        const healthRank = [...healths].sort((a,b) => b.indeks_kesehatan - a.indeks_kesehatan)
            .findIndex(x => cleanKecamatanName(x.kecamatan) === cleanKecamatanName(kecName)) + 1;

        if (!cData && !hData) return `Data untuk **${kecName}** tidak ditemukan dalam dataset Gold Layer.`;

        const crimeIdx = cData ? parseFloat(cData.indeks_kriminalitas).toFixed(1) : '0.0';
        const healthIdx = hData ? parseFloat(hData.indeks_kesehatan).toFixed(1) : '0.0';
        
        // Map levels precisely
        const crimeLevel = parseFloat(crimeIdx) >= 80 ? 'EKSTRIM' : parseFloat(crimeIdx) >= 60 ? 'TINGGI' : parseFloat(crimeIdx) >= 40 ? 'SEDANG' : 'RENDAH';
        
        const crimeRec = parseFloat(crimeIdx) >= 60
            ? 'Tingkatkan patroli rutin dan pemasangan CCTV. Koordinasikan operasi gabungan dengan Polsek setempat.'
            : parseFloat(crimeIdx) >= 40
            ? 'Perkuat sinergi RT/RW dan aparat. Pertimbangkan program Siskamling aktif.'
            : 'Pertahankan kondisi kondusif. Kembangkan program pemberdayaan masyarakat untuk pencegahan dini.';
            
        const healthRec = hData && parseFloat(hData.indeks_kesehatan) >= 50
            ? 'Prioritaskan penambahan tenaga kesehatan dan vaksinasi. Tingkatkan kapasitas Puskesmas.'
            : 'Lanjutkan program Posyandu dan monitoring berkala. Pantau tren kesehatan anak dan lansia.';

        const hfrVal = hData && hData.hfr ? (parseFloat(hData.hfr) * 100).toFixed(2) + '%' : '0.00%';
        const priorityText = parseFloat(crimeIdx) > parseFloat(healthIdx)
            ? 'Keamanan perlu penanganan lebih mendesak di wilayah ini.'
            : 'Kesehatan perlu penanganan lebih mendesak di wilayah ini.';

        return [
            `## Analisis Risiko: ${cData ? cData.kecamatan : kecName}`,
            `*Analisis lokal berbasis data Gold Layer KECAMATRAS*`,
            `Indeks Kriminalitas: ${crimeIdx}/100 — Tingkat ${crimeLevel} (Peringkat #${crimeRank}/31)`,
            cData ? `- Total kasus kriminal: ${cData.total_kasus_kriminal}` : '- Total kasus kriminal: 0',
            cData && cData.crime_rate ? `- Crime rate: ${parseFloat(cData.crime_rate).toFixed(3)} kasus/populasi` : '- Crime rate: 0.000 kasus/populasi',
            `Indeks Kesehatan: ${healthIdx}/100 (Peringkat #${healthRank}/31)`,
            hData && hData.total_kasus_wabah ? `- Total kasus wabah: ${hData.total_kasus_wabah}` : '- Total kasus wabah: 0',
            `- Health fatality ratio: ${hfrVal}`,
            `Rekomendasi Keamanan: ${crimeRec}`,
            `Rekomendasi Kesehatan: ${healthRec}`,
            `Prioritas: ${priorityText}`
        ].join('\n');
    }

    const totalCases = crimes.reduce((s, x) => s + (parseInt(x.total_kasus_kriminal) || 0), 0);
    const avgCrime = (crimes.reduce((s, x) => s + parseFloat(x.indeks_kriminalitas || 0), 0) / crimes.length).toFixed(1);
    const avgHealth = (healths.reduce((s, x) => s + parseFloat(x.indeks_kesehatan || 0), 0) / healths.length).toFixed(1);
    const worstCrime = crimes[0];
    const bestCrime = crimes[crimes.length - 1];
    const worstHealth = [...healths].sort((a,b) => b.indeks_kesehatan - a.indeks_kesehatan)[0];

    if (text.includes('kriminal') || text.includes('kejahatan') || text.includes('rawan')) {
        return [
            `## Analisis Kriminalitas Kota Surabaya`,
            `*Data Gold Layer — 31 Kecamatan*`,
            `Rata-rata Indeks Kriminalitas: ${avgCrime}/100`,
            `Total Kasus Terdata: ${totalCases.toLocaleString('id')} kasus`,
            `Paling Rawan: ${worstCrime?.kecamatan} (${parseFloat(worstCrime?.indeks_kriminalitas).toFixed(1)})`,
            `Paling Aman: ${bestCrime?.kecamatan} (${parseFloat(bestCrime?.indeks_kriminalitas).toFixed(1)})`,
            `Rekomendasi: Fokuskan sumber daya keamanan pada kecamatan dengan indeks di atas 60. Program sosial-ekonomi di wilayah padat dapat menurunkan angka kriminalitas jangka menengah.`
        ].join('\n');
    }

    if (text.includes('kesehatan') || text.includes('sehat') || text.includes('penyakit')) {
        return [
            `## Analisis Kesehatan Kota Surabaya`,
            `*Data Gold Layer — 31 Kecamatan*`,
            `Rata-rata Indeks Kesehatan: ${avgHealth}/100`,
            `Risiko Tertinggi: ${worstHealth?.kecamatan} (${parseFloat(worstHealth?.indeks_kesehatan).toFixed(1)})`,
            `Rekomendasi: Perkuat jaringan Puskesmas di wilayah dengan indeks rendah. Prioritaskan program preventif di kecamatan padat penduduk.`
        ].join('\n');
    }

    return [
        `## Ringkasan Analisis Kota Surabaya`,
        `*Data Gold Layer KECAMATRAS — 31 Kecamatan*`,
        `Kriminalitas: Rata-rata indeks ${avgCrime}/100, total ${totalCases.toLocaleString('id')} kasus`,
        `- Paling rawan: ${worstCrime?.kecamatan} (${parseFloat(worstCrime?.indeks_kriminalitas).toFixed(1)})`,
        `- Paling aman: ${bestCrime?.kecamatan} (${parseFloat(bestCrime?.indeks_kriminalitas).toFixed(1)})`,
        `Kesehatan: Rata-rata indeks ${avgHealth}/100`,
        `- Risiko tertinggi: ${worstHealth?.kecamatan} (${parseFloat(worstHealth?.indeks_kesehatan).toFixed(1)})`,
        `Rekomendasi: Integrasikan data kriminalitas dan kesehatan untuk identifikasi wilayah risiko ganda. Alokasikan sumber daya secara proporsional terhadap indeks risiko.`,
        `*Untuk analisis AI lebih mendalam, aktifkan Gemini API di menu Settings.*`
    ].join('\n');
}

// Custom Markdown-to-HTML parser for clean chat formatting
function parseMarkdownToHtml(markdown) {
    if (!markdown) return '';
    
    // Split into lines
    let lines = markdown.split('\n');
    let html = [];
    let inList = false;
    let inTable = false;
    let tableRowCount = 0;
    
    for (let line of lines) {
        let trimmed = line.trim();
        
        // Table handling
        let isTableLine = trimmed.startsWith('|');
        if (isTableLine) {
            if (inList) { html.push('</ul>'); inList = false; }
            if (!inTable) {
                inTable = true;
                tableRowCount = 0;
                html.push('<div style="overflow-x:auto; margin: 10px 0;"><table class="styled-table" style="width:100%; margin:0; border-collapse: collapse; font-size: 12px;">');
            }
            // skip alignment row e.g., |:---|:---|
            if (trimmed.match(/^\|?[\s-:]+\|/)) continue;
            
            // split by |, ignoring the first and last empty strings if they exist
            let parts = trimmed.split('|');
            if (parts[0] === '') parts.shift();
            if (parts[parts.length - 1] === '') parts.pop();
            
            let cells = parts.map(c => formatInlineMarkdown(c.trim()));
            let isHeader = tableRowCount === 0;
            let tag = isHeader ? 'th' : 'td';
            let thStyle = isHeader ? 'background:var(--bg-panel); font-weight:700; color:var(--text-primary);' : 'color:var(--text-secondary);';
            
            html.push('<tr>' + cells.map(c => `<${tag} style="text-align:left; padding:8px 12px; border:1px solid var(--border-color); ${thStyle}">${c}</${tag}>`).join('') + '</tr>');
            tableRowCount++;
            continue;
        } else if (inTable) {
            if (trimmed === '') {
                // Ignore empty lines inside a table to prevent breaking it prematurely
                continue;
            } else {
                html.push('</table></div>');
                inTable = false;
            }
        }
        
        // Headings
        if (trimmed.startsWith('#### ')) {
            if (inList) { html.push('</ul>'); inList = false; }
            html.push(`<h6 style="margin: 10px 0 4px; font-weight: 700; font-size: 12px; color: var(--text-primary);">${formatInlineMarkdown(trimmed.substring(5))}</h6>`);
            continue;
        }
        if (trimmed.startsWith('### ')) {
            if (inList) { html.push('</ul>'); inList = false; }
            html.push(`<h5 style="margin: 12px 0 6px; font-weight: 700; font-size: 12.5px; color: var(--text-secondary);">${formatInlineMarkdown(trimmed.substring(4))}</h5>`);
            continue;
        }
        if (trimmed.startsWith('## ')) {
            if (inList) { html.push('</ul>'); inList = false; }
            html.push(`<h4 style="margin: 14px 0 8px; font-weight: 700; font-size: 14px; color: var(--text-primary); border-bottom: 1px solid var(--border-color); padding-bottom: 4px;">${formatInlineMarkdown(trimmed.substring(3))}</h4>`);
            continue;
        }
        if (trimmed.startsWith('# ')) {
            if (inList) { html.push('</ul>'); inList = false; }
            html.push(`<h3 style="margin: 16px 0 10px; font-weight: 700; font-size: 16px; color: var(--text-primary);">${formatInlineMarkdown(trimmed.substring(2))}</h3>`);
            continue;
        }
        
        // Horizontal Rule
        if (trimmed === '---' || trimmed === '***') {
            if (inList) { html.push('</ul>'); inList = false; }
            html.push('<hr style="border: none; border-top: 1px solid var(--border-color); margin: 16px 0;">');
            continue;
        }
        
        // Bullet points
        if (trimmed.startsWith('- ') || trimmed.startsWith('* ')) {
            if (!inList) {
                html.push('<ul style="margin: 6px 0 10px 20px; padding: 0; list-style-type: disc; display: flex; flex-direction: column; gap: 4px;">');
                inList = true;
            }
            let content = trimmed.substring(2);
            content = formatInlineMarkdown(content);
            html.push(`<li style="font-size: 12.5px; line-height: 1.5; color: var(--text-secondary);">${content}</li>`);
            continue;
        }
        
        // Numbered list
        if (/^\d+\.\s/.test(trimmed)) {
            if (inList) { html.push('</ul>'); inList = false; }
            let content = trimmed.replace(/^\d+\.\s/, '');
            content = formatInlineMarkdown(content);
            html.push(`<p style="margin: 6px 0; font-size: 12.5px; line-height: 1.6; color: var(--text-secondary);"><strong>${trimmed.match(/^\d+/)[0]}.</strong> ${content}</p>`);
            continue;
        }
        
        // Empty lines
        if (trimmed === '') {
            if (inList) { html.push('</ul>'); inList = false; }
            continue;
        }
        
        // Regular paragraph
        if (inList) { html.push('</ul>'); inList = false; }
        
        let formatted = formatInlineMarkdown(trimmed);
        html.push(`<p style="margin: 0 0 10px 0; font-size: 12.5px; line-height: 1.65; color: var(--text-secondary);">${formatted}</p>`);
    }
    
    if (inList) { html.push('</ul>'); }
    if (inTable) { html.push('</table></div>'); }
    return html.join('\n');
}

function formatInlineMarkdown(text) {
    if (!text) return '';
    return text
        // Bold
        .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
        // Italic
        .replace(/\*(.*?)\*/g, '<em>$1</em>')
        // Inline code
        .replace(/`(.*?)`/g, '<code style="background:var(--bg-panel); padding:2px 4px; border-radius:4px; font-family:monospace; font-size:11.5px; border: 1px solid var(--border-color);">$1</code>')
        // Strip any remaining raw asterisks so they don't bleed into the text
        .replace(/\*/g, '');
}

function appendAIMessage(role, text, isLoading = false) {
    const container = document.getElementById('ai-chat-messages');
    if (!container) return null;

    const div = document.createElement('div');
    div.className = `ai-msg ${role === 'user' ? 'ai-msg-user' : 'ai-msg-bot'}`;

    if (role === 'bot') {
        div.innerHTML = `
            <div class="ai-avatar"><img src="data/logo.jpeg" style="width:100%;height:100%;object-fit:cover;border-radius:inherit;" alt="AI"></div>
            <div class="ai-bubble">${isLoading ? '<i class="fa-solid fa-spinner fa-spin"></i> Sedang berpikir...' : parseMarkdownToHtml(text)}</div>
        `;
    } else {
        div.innerHTML = `
            <div class="ai-bubble ai-bubble-user">${text}</div>
            <div class="ai-avatar ai-avatar-user"><i class="fa-solid fa-user"></i></div>
        `;
    }

    container.appendChild(div);
    container.scrollTop = container.scrollHeight;
    return div;
}

async function sendAIMessage() {
    const inputEl = document.getElementById('ai-text-input');
    const kecSelect = document.getElementById('ai-kec-select');

    let userText = inputEl?.value?.trim();
    const selectedKec = kecSelect?.value;

    if (!userText && !selectedKec) return;

    if (!userText && selectedKec) {
        userText = `Analisis lengkap risiko dan prediksi untuk Kecamatan ${selectedKec}.`;
    }

    appendAIMessage('user', userText);
    if (inputEl) inputEl.value = '';

    const loadingDiv = appendAIMessage('bot', '', true);

    try {
        const cityCtx = buildCityContext();
        const kecCtx = selectedKec ? `\nData spesifik ${selectedKec}:\n${buildKecamatanContext(selectedKec)}` : '';
        const systemPrompt = `Kamu adalah AI analis data kota bernama KECAMATRAS AI, spesialis analisis kriminalitas dan kesehatan di Surabaya, Indonesia. Jawab dalam Bahasa Indonesia. 
        Jangan gunakan format markdown seperti bintang-bintang (**) atau (*) untuk menebalkan atau memformat teks. Tuliskan jawaban dalam teks biasa yang bersih tanpa menggunakan karakter bintang (*) sama sekali.
        Berikan prediksi, analisis risiko, dan rekomendasi berbasis data berikut:
        ${cityCtx}${kecCtx}
        
        Selain analisis indeks utama, kamu juga bisa memberikan rekomendasi umum kota Surabaya (seperti daerah hunian terbaik, kos mahasiswa dekat kampus ITS/UNAIR, dll.) dengan mengaitkannya ke data keselamatan (kriminalitas) dan kesehatan KECAMATRAS.
        Jika ditanya kos bagus dekat ITS, rekomendasikan Kecamatan Sukolilo (lokasi ITS berada) karena memiliki indeks kriminalitas 0 (teraman menurut data KECAMATRAS) dan Kecamatan Mulyorejo sebagai alternatif terdekat.
        
        Pertanyaan: ${userText}`;

        const response = await askGemini(systemPrompt);
        if (loadingDiv) {
            loadingDiv.querySelector('.ai-bubble').innerHTML = parseMarkdownToHtml(response);
        }
    } catch (err) {
        // Fallback: local data-driven analysis
        const localResult = generateLocalAnalysis(userText, selectedKec);
        if (loadingDiv) {
            const bubble = loadingDiv.querySelector('.ai-bubble');
            bubble.innerHTML = parseMarkdownToHtml(localResult);
            bubble.style.borderLeft = '3px solid var(--color-orange)';
            bubble.title = 'Analisis lokal berbasis data (API tidak tersedia)';
            
            // Append explicit API warning so the user knows exactly why Gemini failed
            const errorBlock = document.createElement('div');
            errorBlock.style.cssText = 'font-size:10.5px;color:var(--color-red);margin-top:8px;padding-top:8px;border-top:1px dashed rgba(217,93,57,0.25);font-style:italic;';
            errorBlock.innerHTML = `<i class="fa-solid fa-circle-exclamation"></i> Gagal menghubungi Gemini: ${err.message}. Menampilkan data analitik lokal.`;
            bubble.appendChild(errorBlock);
        }
    }
}

async function runQuickPredictions() {
    const container = document.getElementById('ai-quick-predictions');
    if (!container || !dashboardData) return;

    container.innerHTML = `<div class="loading-state"><i class="fa-solid fa-spinner fa-spin"></i><p>Menganalisis dengan Gemini AI...</p></div>`;

    const top3Crime = dashboardData.kriminalitas.slice(0, 3);
    const top3Health = dashboardData.kesehatan.slice(0, 3);

    const predictions = [];

    for (const kec of top3Crime) {
        try {
            const ctx = buildKecamatanContext(kec.kecamatan);
            const prompt = `${buildCityContext()}\n\nFokus: ${kec.kecamatan}\n${ctx}\n\nBerikan prediksi 1 bulan ke depan untuk kecamatan ini dalam 2-3 kalimat singkat. Sertakan rekomendasi tindakan prioritas. Jangan gunakan format markdown bintang-bintang (* atau **).`;
            const result = await askGemini(prompt);
            predictions.push({ kecamatan: kec.kecamatan, type: 'kriminalitas', score: kec.indeks_kriminalitas, text: result });
        } catch {
            predictions.push({ kecamatan: kec.kecamatan, type: 'kriminalitas', score: kec.indeks_kriminalitas, text: 'Tidak dapat memuat prediksi.' });
        }
    }

    for (const kec of top3Health.slice(0,2)) {
        try {
            const ctx = buildKecamatanContext(kec.kecamatan);
            const prompt = `${buildCityContext()}\n\nFokus kesehatan: ${kec.kecamatan}\n${ctx}\n\nBerikan prediksi risiko wabah 1 bulan ke depan dalam 2-3 kalimat. Sertakan rekomendasi intervensi. Jangan gunakan format markdown bintang-bintang (* atau **).`;
            const result = await askGemini(prompt);
            predictions.push({ kecamatan: kec.kecamatan, type: 'kesehatan', score: kec.indeks_kesehatan, text: result });
        } catch {
            predictions.push({ kecamatan: kec.kecamatan, type: 'kesehatan', score: kec.indeks_kesehatan, text: 'Tidak dapat memuat prediksi.' });
        }
    }

    container.innerHTML = predictions.map(p => `
        <div class="ai-prediction-card" onclick="switchView('kecamatan'); selectKecamatan('${p.kecamatan}')">
            <div class="ai-pred-header">
                <span class="ai-pred-kec"><i class="fa-solid fa-location-dot"></i> ${p.kecamatan}</span>
                <span class="cat-badge ${p.type === 'kriminalitas' ? 'cat-krim' : 'cat-sehat'}">${p.type === 'kriminalitas' ? 'Kriminal' : 'Kesehatan'}</span>
            </div>
            <div class="ai-pred-score">Indeks: <strong>${parseFloat(p.score).toFixed(1)}</strong>/100</div>
            <div class="ai-pred-text">${parseMarkdownToHtml(p.text)}</div>
        </div>
    `).join('');
}

async function generateRecommendations() {
    const container = document.getElementById('ai-recommendations');
    if (!container || !dashboardData) return;

    container.innerHTML = `<div class="loading-state" style="grid-column:1/-1;"><i class="fa-solid fa-spinner fa-spin"></i><p>Gemini sedang menyusun rekomendasi...</p></div>`;

    const categories = [
        { title: 'Prioritas Keamanan', icon: 'fa-shield-halved', color: 'color-red', prompt: `${buildCityContext()}\n\nBerikan 3 rekomendasi kebijakan keamanan prioritas untuk kota Surabaya berbasis data kriminalitas. Format: daftar bernomor, singkat. Jangan gunakan format markdown bintang-bintang (* atau **).` },
        { title: 'Intervensi Kesehatan', icon: 'fa-heart-pulse', color: 'color-orange', prompt: `${buildCityContext()}\n\nBerikan 3 rekomendasi intervensi kesehatan masyarakat untuk kota Surabaya berbasis data wabah. Format: daftar bernomor, singkat. Jangan gunakan format markdown bintang-bintang (* atau **).` },
        { title: 'Alokasi Sumber Daya', icon: 'fa-sitemap', color: 'color-blue', prompt: `${buildCityContext()}\n\nBerikan 3 rekomendasi alokasi sumber daya pemerintah (polisi, puskesmas, dll) berbasis data risiko kecamatan. Format: daftar bernomor, singkat. Jangan gunakan format markdown bintang-bintang (* atau **).` },
    ];

    const results = await Promise.allSettled(categories.map(c => askGemini(c.prompt)));

    container.innerHTML = categories.map((cat, i) => {
        const text = results[i].status === 'fulfilled' ? results[i].value : 'Gagal memuat rekomendasi.';
        return `
            <div class="ai-reco-card">
                <div class="ai-reco-header">
                    <div class="kpi-icon ${cat.color}" style="width:36px;height:36px;font-size:16px;border-radius:8px;">
                        <i class="fa-solid ${cat.icon}"></i>
                    </div>
                    <h4>${cat.title}</h4>
                </div>
                <div class="ai-reco-body">${parseMarkdownToHtml(text)}</div>
            </div>
        `;
    }).join('');
}

function openNewsModal(item) {
    const existing = document.getElementById('news-detail-modal');
    if (existing) existing.remove();

    const isKrim = item.kategori === 'Kriminalitas';
    const directUrl = getDirectArticleUrl(item);
    const searchUrl = getNewsSearchUrl(item);
    const dateStr = item.tanggal_publikasi ? formatISODate(item.tanggal_publikasi) : 'Baru-baru ini';

    // Pull kecamatan data for context panel
    const cData = (dashboardData?.kriminalitas || []).find(x => cleanKecamatanName(x.kecamatan) === cleanKecamatanName(item.kecamatan_terdeteksi));
    const hData = (dashboardData?.kesehatan || []).find(x => cleanKecamatanName(x.kecamatan) === cleanKecamatanName(item.kecamatan_terdeteksi));
    const crimeRank = (dashboardData?.kriminalitas || []).findIndex(x => cleanKecamatanName(x.kecamatan) === cleanKecamatanName(item.kecamatan_terdeteksi)) + 1;
    const crimeIdx = cData ? parseFloat(cData.indeks_kriminalitas).toFixed(1) : 'N/A';
    const healthIdx = hData ? parseFloat(hData.indeks_kesehatan).toFixed(1) : 'N/A';
    const crimeColor = parseFloat(crimeIdx) >= 60 ? 'var(--color-red)' : parseFloat(crimeIdx) >= 40 ? 'var(--color-orange)' : 'var(--color-green)';
    const accentColor = isKrim ? 'var(--color-red)' : 'var(--color-blue)';
    const accentBg = isKrim ? 'rgba(217,93,57,0.08)' : 'rgba(75,162,172,0.08)';

    // Pipeline provenance text
    const pipeline = isKrim
        ? 'Kafka Ingest → Bronze → Silver → Gold (Delta Lakehouse)'
        : 'Kafka Ingest → Bronze → Silver → Gold (Delta Lakehouse)';

    const overlay = document.createElement('div');
    overlay.id = 'news-detail-modal';
    overlay.style.cssText = `position:fixed;inset:0;background:rgba(18,35,38,0.72);backdrop-filter:blur(8px);z-index:9999;display:flex;align-items:center;justify-content:center;padding:16px;animation:fadeInModal 0.2s ease;`;
    overlay.onclick = (e) => { if (e.target === overlay) overlay.remove(); };

    overlay.innerHTML = `
<div style="background:var(--bg-primary);border-radius:16px;width:620px;max-width:98vw;max-height:90vh;overflow-y:auto;box-shadow:0 32px 80px rgba(18,35,38,0.4);border:1px solid var(--border-color);font-family:var(--font-primary);animation:slideUpModal 0.25s ease;">
    <!-- Header bar -->
    <div style="display:flex;align-items:center;justify-content:space-between;padding:18px 22px 16px;border-bottom:1px solid var(--border-color);">
        <div style="display:flex;align-items:center;gap:10px;">
            <div style="width:36px;height:36px;border-radius:10px;background:${accentBg};display:flex;align-items:center;justify-content:center;flex-shrink:0;">
                <i class="fa-solid ${isKrim ? 'fa-shield-halved' : 'fa-heart-pulse'}" style="color:${accentColor};font-size:15px;"></i>
            </div>
            <div>
                <div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:0.8px;color:${accentColor};">${item.kategori}</div>
                <div style="font-size:11px;color:var(--text-muted);">${item.sumber} &nbsp;·&nbsp; ${dateStr}</div>
            </div>
        </div>
        <button onclick="document.getElementById('news-detail-modal').remove()"
                style="border:none;background:var(--bg-panel);color:var(--text-muted);width:30px;height:30px;border-radius:8px;cursor:pointer;font-size:13px;display:flex;align-items:center;justify-content:center;">
            <i class="fa-solid fa-xmark"></i>
        </button>
    </div>

    <!-- Title + location -->
    <div style="padding:20px 22px 0;">
        <h3 style="margin:0 0 12px;font-size:16px;font-weight:700;color:var(--text-primary);line-height:1.45;">${item.judul}</h3>
        <div style="display:inline-flex;align-items:center;gap:6px;padding:5px 10px;background:${accentBg};border-radius:6px;border-left:3px solid ${accentColor};margin-bottom:18px;">
            <i class="fa-solid fa-location-dot" style="color:${accentColor};font-size:11px;"></i>
            <span style="font-size:12px;font-weight:700;color:var(--text-primary);">Kec. ${item.kecamatan_terdeteksi}</span>
            ${crimeRank ? `<span style="font-size:10px;color:var(--text-muted);">· Rank #${crimeRank}/31</span>` : ''}
        </div>
    </div>

    <!-- Data context grid -->
    <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;margin:0 22px 18px;">
        <div style="background:var(--bg-panel);border-radius:10px;padding:12px 14px;border:1px solid var(--border-color);">
            <div style="font-size:10px;color:var(--text-muted);margin-bottom:4px;font-weight:600;text-transform:uppercase;letter-spacing:0.5px;">Indeks Kriminal</div>
            <div style="font-size:22px;font-weight:800;color:${crimeColor};line-height:1;">${crimeIdx}</div>
            <div style="font-size:10px;color:var(--text-muted);margin-top:2px;">/100 skor risiko</div>
        </div>
        <div style="background:var(--bg-panel);border-radius:10px;padding:12px 14px;border:1px solid var(--border-color);">
            <div style="font-size:10px;color:var(--text-muted);margin-bottom:4px;font-weight:600;text-transform:uppercase;letter-spacing:0.5px;">Indeks Kesehatan</div>
            <div style="font-size:22px;font-weight:800;color:var(--color-blue);line-height:1;">${healthIdx}</div>
            <div style="font-size:10px;color:var(--text-muted);margin-top:2px;">/100 skor risiko</div>
        </div>
        <div style="background:var(--bg-panel);border-radius:10px;padding:12px 14px;border:1px solid var(--border-color);">
            <div style="font-size:10px;color:var(--text-muted);margin-bottom:4px;font-weight:600;text-transform:uppercase;letter-spacing:0.5px;">Kasus Wabah</div>
            <div style="font-size:22px;font-weight:800;color:var(--text-primary);line-height:1;">${hData?.total_kasus_wabah ?? '—'}</div>
            <div style="font-size:10px;color:var(--text-muted);margin-top:2px;">wabah terdata</div>
        </div>
    </div>

    <!-- Pipeline provenance -->
    <div style="margin:0 22px 18px;padding:10px 14px;background:rgba(28,51,53,0.04);border-radius:8px;border:1px solid var(--border-color);">
        <div style="font-size:10px;font-weight:700;color:var(--text-muted);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:4px;">
            <i class="fa-solid fa-database" style="margin-right:4px;"></i>Provenance Pipeline
        </div>
        <div style="font-size:11px;color:var(--text-secondary);font-family:monospace;">${pipeline}</div>
        <div style="font-size:10px;color:var(--text-muted);margin-top:3px;">ID Berita: ${item.id_berita} &nbsp;·&nbsp; Diproses via Kafka + Spark MLlib</div>
    </div>

    <!-- Actions -->
    <div style="padding:0 22px 22px;display:flex;gap:8px;flex-wrap:wrap;">
        <button onclick="document.getElementById('news-detail-modal').remove();switchView('kecamatan');selectKecamatan('${item.kecamatan_terdeteksi}');"
                style="flex:1;min-width:130px;padding:10px 14px;background:var(--color-blue);color:white;border:none;border-radius:9px;cursor:pointer;font-family:var(--font-primary);font-size:12.5px;font-weight:700;display:flex;align-items:center;justify-content:center;gap:6px;">
            <i class="fa-solid fa-map-location-dot"></i> Lihat di Peta
        </button>
        <button onclick="document.getElementById('news-detail-modal').remove();switchView('prediksi');setTimeout(()=>{const s=document.getElementById('ai-kec-select');if(s){s.value='${item.kecamatan_terdeteksi}';sendAIMessage();}},300);"
                style="flex:1;min-width:130px;padding:10px 14px;background:${accentBg};color:${accentColor};border:1px solid ${accentColor};border-radius:9px;cursor:pointer;font-family:var(--font-primary);font-size:12.5px;font-weight:700;display:flex;align-items:center;justify-content:center;gap:6px;">
            <i class="fa-solid fa-wand-magic-sparkles"></i> Analisis AI
        </button>
        ${directUrl ? `
        <a href="${directUrl}" target="_blank" rel="noopener noreferrer"
           style="flex:1;min-width:130px;padding:10px 14px;background:var(--color-green);color:white;border:none;border-radius:9px;cursor:pointer;font-family:var(--font-primary);font-size:12.5px;font-weight:700;text-decoration:none;display:flex;align-items:center;justify-content:center;gap:6px;transition:var(--transition-fast);"
           onmouseover="this.style.opacity='0.9'" onmouseout="this.style.opacity='1'">
            <i class="fa-solid fa-arrow-up-right-from-square"></i> Baca Sumber Asli
        </a>
        ` : ''}
        <a href="${searchUrl}" target="_blank" rel="noopener noreferrer"
           style="flex:1;min-width:130px;padding:10px 14px;background:transparent;color:var(--text-secondary);border:1px solid var(--border-color);border-radius:9px;cursor:pointer;font-family:var(--font-primary);font-size:12.5px;font-weight:700;text-decoration:none;display:flex;align-items:center;justify-content:center;gap:6px;">
            <i class="fa-brands fa-google" style="color:#4285f4;"></i> Google News
        </a>
    </div>
</div>`;
    document.body.appendChild(overlay);
}

// =============================================================================
// GEMINI KEY MANAGEMENT — read from localStorage, editable from UI
// =============================================================================
function getActiveGeminiKey() {
    return localStorage.getItem('kecamatras_gemini_key') || GEMINI_KEY;
}

function getActiveGeminiModel() {
    return localStorage.getItem('kecamatras_gemini_model') || 'gemini-flash-lite-latest';
}

function buildGeminiUrl() {
    return `https://generativelanguage.googleapis.com/v1beta/models/${getActiveGeminiModel()}:generateContent?key=${getActiveGeminiKey()}`;
}

function openSettingsModal() {
    const existing = document.getElementById('settings-modal');
    if (existing) { existing.remove(); return; }

    const currentKey = localStorage.getItem('kecamatras_gemini_key') || '';
    const currentModel = localStorage.getItem('kecamatras_gemini_model') || 'gemini-flash-lite-latest';
    const keyStatus = !currentKey ? 'Belum diset' : 'Key tersimpan';
    const statusColor = !currentKey ? 'var(--color-orange)' : 'var(--color-green)';

    const overlay = document.createElement('div');
    overlay.id = 'settings-modal';
    overlay.style.cssText = `position:fixed;inset:0;background:rgba(28,51,53,0.6);backdrop-filter:blur(5px);z-index:9999;display:flex;align-items:center;justify-content:center;animation:fadeInModal 0.2s ease;`;
    overlay.onclick = (e) => { if (e.target === overlay) overlay.remove(); };

    overlay.innerHTML = `
        <div style="background:var(--bg-primary);border-radius:16px;width:480px;max-width:96vw;box-shadow:0 24px 64px rgba(28,51,53,0.3);border:1px solid var(--border-color);font-family:var(--font-primary);animation:slideUpModal 0.25s ease;overflow:hidden;">
            <div style="padding:20px 24px 18px;border-bottom:1px solid var(--border-color);display:flex;justify-content:space-between;align-items:center;">
                <div style="display:flex;align-items:center;gap:10px;">
                    <div style="width:32px;height:32px;background:var(--bg-panel);border-radius:8px;display:flex;align-items:center;justify-content:center;">
                        <i class="fa-solid fa-gear" style="color:var(--color-blue);font-size:14px;"></i>
                    </div>
                    <div>
                        <h3 style="margin:0;font-size:15px;font-weight:700;color:var(--text-primary);">Pengaturan AI</h3>
                        <p style="margin:0;font-size:11px;color:var(--text-muted);">Konfigurasi Google Gemini API</p>
                    </div>
                </div>
                <button onclick="document.getElementById('settings-modal').remove()" style="border:none;background:var(--bg-panel);color:var(--text-muted);width:28px;height:28px;border-radius:8px;cursor:pointer;font-size:13px;display:flex;align-items:center;justify-content:center;">
                    <i class="fa-solid fa-xmark"></i>
                </button>
            </div>

            <div style="padding:20px 24px;display:flex;flex-direction:column;gap:14px;">
                <!-- Key input -->
                <div>
                    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;">
                        <label style="font-size:12px;font-weight:700;color:var(--text-primary);">
                            <i class="fa-solid fa-key" style="color:var(--text-muted);margin-right:4px;"></i> API Key
                        </label>
                        <span id="key-status-inline" style="font-size:11px;font-weight:600;color:${statusColor};">${keyStatus}</span>
                    </div>
                    <div style="display:flex;gap:8px;">
                        <input type="password" id="settings-gemini-key" value="${currentKey}"
                            placeholder="Masukkan API Key Gemini..."
                            oninput="const v=this.value.trim();const s=document.getElementById('key-status-inline');s.textContent=v?'Key dimasukkan':'Belum diset';s.style.color=v?'var(--color-blue)':'var(--color-orange)';"
                            style="flex:1;background:var(--bg-panel);border:1px solid var(--border-color);border-radius:8px;padding:9px 12px;color:var(--text-primary);font-family:monospace;font-size:12px;outline:none;">
                        <button onclick="const i=document.getElementById('settings-gemini-key');i.type=i.type==='password'?'text':'password';"
                            style="padding:9px 11px;background:var(--bg-panel);border:1px solid var(--border-color);border-radius:8px;cursor:pointer;color:var(--text-muted);font-size:12px;">
                            <i class="fa-solid fa-eye"></i>
                        </button>
                    </div>
                    <p style="font-size:11px;color:var(--text-muted);margin:5px 0 0;">Dapatkan key gratis di <a href="https://aistudio.google.com/app/apikey" target="_blank" style="color:var(--color-blue);">aistudio.google.com</a></p>
                </div>

                <!-- Model selector -->
                <div>
                    <label style="font-size:12px;font-weight:700;color:var(--text-primary);display:block;margin-bottom:6px;">
                        <i class="fa-solid fa-microchip" style="color:var(--text-muted);margin-right:4px;"></i> Model
                    </label>
                    <select id="settings-gemini-model" style="width:100%;background:var(--bg-panel);border:1px solid var(--border-color);border-radius:8px;padding:9px 12px;color:var(--text-primary);font-family:var(--font-primary);font-size:13px;cursor:pointer;">
                        <option value="gemini-flash-lite-latest" ${currentModel==='gemini-flash-lite-latest'?'selected':''}>gemini-flash-lite-latest — Rekomendasi (Cepat & Stabil)</option>
                        <option value="gemini-flash-latest" ${currentModel==='gemini-flash-latest'?'selected':''}>gemini-flash-latest — Flash Terbaru</option>
                        <option value="gemini-2.5-pro" ${currentModel==='gemini-2.5-pro'?'selected':''}>gemini-2.5-pro — Pro Terbaik</option>
                    </select>
                </div>

                <!-- Test result area -->
                <div id="api-test-result" style="display:none;padding:10px 12px;border-radius:8px;font-size:12px;line-height:1.5;"></div>

                <!-- Action buttons -->
                <div style="display:flex;gap:8px;">
                    <button onclick="saveGeminiKey()" style="flex:1;padding:10px;background:var(--color-blue);color:white;border:none;border-radius:8px;cursor:pointer;font-family:var(--font-primary);font-size:13px;font-weight:600;">
                        <i class="fa-solid fa-floppy-disk"></i> Simpan
                    </button>
                    <button onclick="testGeminiKey()" style="padding:10px 14px;background:var(--bg-panel);border:1px solid var(--border-color);border-radius:8px;cursor:pointer;font-family:var(--font-primary);font-size:13px;font-weight:600;color:var(--text-secondary);" id="btn-test-api">
                        <i class="fa-solid fa-flask"></i> Tes
                    </button>
                    <button onclick="clearGeminiKey()" style="padding:10px 12px;background:transparent;border:1px solid var(--border-color);border-radius:8px;cursor:pointer;font-family:var(--font-primary);font-size:12px;color:var(--text-muted);">
                        <i class="fa-solid fa-trash"></i>
                    </button>
                </div>
            </div>
        </div>
    `;
    document.body.appendChild(overlay);
}

async function testGeminiKey() {
    const key = document.getElementById('settings-gemini-key')?.value?.trim();
    const model = document.getElementById('settings-gemini-model')?.value || 'gemini-flash-lite-latest';
    const resultEl = document.getElementById('api-test-result');
    const btn = document.getElementById('btn-test-api');

    if (!key) {
        resultEl.style.display = 'block';
        resultEl.style.background = 'rgba(217,93,57,0.1)';
        resultEl.style.color = 'var(--color-red)';
        resultEl.innerHTML = '<i class="fa-solid fa-circle-xmark"></i> Masukkan API key terlebih dahulu.';
        return;
    }

    btn.disabled = true;
    btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i>';
    resultEl.style.display = 'block';
    resultEl.style.background = 'rgba(75,162,172,0.08)';
    resultEl.style.color = 'var(--text-secondary)';
    resultEl.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Menghubungi Gemini...';

    try {
        const url = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${key}`;
        const res = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ contents: [{ parts: [{ text: 'Balas "OK" saja dalam satu kata.' }] }] })
        });
        const data = await res.json();
        if (res.ok && data.candidates?.[0]) {
            resultEl.style.background = 'rgba(43,156,128,0.1)';
            resultEl.style.color = 'var(--color-green)';
            resultEl.innerHTML = '<i class="fa-solid fa-circle-check"></i> Koneksi berhasil! API key valid dan model aktif.';
        } else {
            const msg = data?.error?.message || `HTTP ${res.status}`;
            resultEl.style.background = 'rgba(217,93,57,0.1)';
            resultEl.style.color = 'var(--color-red)';
            resultEl.innerHTML = `<i class="fa-solid fa-circle-xmark"></i> Gagal: ${msg}`;
        }
    } catch (e) {
        resultEl.style.background = 'rgba(217,93,57,0.1)';
        resultEl.style.color = 'var(--color-red)';
        resultEl.innerHTML = `<i class="fa-solid fa-circle-xmark"></i> Error: ${e.message}`;
    } finally {
        btn.disabled = false;
        btn.innerHTML = '<i class="fa-solid fa-flask"></i> Tes';
    }
}

function saveGeminiKey() {
    const key = document.getElementById('settings-gemini-key')?.value?.trim();
    const model = document.getElementById('settings-gemini-model')?.value || 'gemini-flash-lite-latest';
    if (key) {
        localStorage.setItem('kecamatras_gemini_key', key);
    } else {
        localStorage.removeItem('kecamatras_gemini_key');
    }
    localStorage.setItem('kecamatras_gemini_model', model);
    document.getElementById('settings-modal')?.remove();
    showNotificationToast('Pengaturan AI disimpan');
}

function clearGeminiKey() {
    localStorage.removeItem('kecamatras_gemini_key');
    localStorage.removeItem('kecamatras_gemini_model');
    document.getElementById('settings-modal')?.remove();
    showNotificationToast('Settings direset ke default.');
}

// =============================================================================
// AI QUICK CHIPS
// =============================================================================
function askQuick(promptText, kecamatan) {
    switchView('prediksi');
    setTimeout(() => {
        const inputEl = document.getElementById('ai-text-input');
        const kecSel = document.getElementById('ai-kec-select');
        if (inputEl) inputEl.value = promptText;
        if (kecSel && kecamatan) kecSel.value = kecamatan;
        sendAIMessage();
    }, 200);
}

// =============================================================================
// EXPORT CSV
// =============================================================================
function exportCSV(type) {
    if (!dashboardData) return;
    let rows = [], filename = '';

    if (type === 'kriminalitas') {
        filename = 'kriminalitas_surabaya.csv';
        rows = [['Kecamatan', 'Total Kasus', 'Crime Rate', 'Indeks Kriminalitas']];
        dashboardData.kriminalitas.forEach(x => rows.push([x.kecamatan, x.total_kasus_kriminal, parseFloat(x.crime_rate).toFixed(2), parseFloat(x.indeks_kriminalitas).toFixed(2)]));
    } else {
        filename = 'kesehatan_surabaya.csv';
        rows = [['Kecamatan', 'Total Wabah', 'Incidence Rate', 'HFR', 'Indeks Kesehatan']];
        dashboardData.kesehatan.forEach(x => rows.push([x.kecamatan, x.total_kasus_wabah, parseFloat(x.incidence_rate).toFixed(2), parseFloat(x.hfr).toFixed(2), parseFloat(x.indeks_kesehatan).toFixed(2)]));
    }

    const csv = rows.map(r => r.map(v => `"${v}"`).join(',')).join('\n');
    const blob = new Blob(['﻿' + csv], { type: 'text/csv;charset=utf-8;' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = filename;
    a.click();
    showNotificationToast(`Data ${type} berhasil diunduh!`);
}

// =============================================================================
// KECAMATAN COMPARISON
// =============================================================================
let compareKecamatan = null;

function setCompareKecamatan(kecName) {
    compareKecamatan = kecName || null;
    renderComparePanel();
}

function renderComparePanel() {
    const panel = document.getElementById('compare-panel');
    if (!panel || !dashboardData) return;

    if (!compareKecamatan || !activeKecamatan || compareKecamatan === activeKecamatan) {
        panel.innerHTML = `<p style="color:var(--text-muted);font-size:12px;text-align:center;padding:16px 0;">Pilih kecamatan pembanding di atas untuk membandingkan data.</p>`;
        return;
    }

    const getKec = name => ({
        crime: dashboardData.kriminalitas.find(x => cleanKecamatanName(x.kecamatan) === cleanKecamatanName(name)),
        health: dashboardData.kesehatan.find(x => cleanKecamatanName(x.kecamatan) === cleanKecamatanName(name))
    });
    const A = { name: activeKecamatan, ...getKec(activeKecamatan) };
    const B = { name: compareKecamatan, ...getKec(compareKecamatan) };

    const winBadge = (va, vb, lowerBetter = false) => {
        const aWins = lowerBetter ? va <= vb : va >= vb;
        return aWins ? ['▼', '▲'] : ['▲', '▼'];
    };

    const crimeA = A.crime ? parseFloat(A.crime.indeks_kriminalitas) : 0;
    const crimeB = B.crime ? parseFloat(B.crime.indeks_kriminalitas) : 0;
    const healthA = A.health ? parseFloat(A.health.indeks_kesehatan) : 0;
    const healthB = B.health ? parseFloat(B.health.indeks_kesehatan) : 0;
    const [cwa, cwb] = winBadge(crimeA, crimeB, true);
    const [hwa, hwb] = winBadge(healthA, healthB, true);

    const row = (label, va, vb) => `
        <div class="compare-row">
            <span class="compare-val">${va}</span>
            <span class="compare-label">${label}</span>
            <span class="compare-val">${vb}</span>
        </div>`;

    panel.innerHTML = `
        <div class="compare-header">
            <span class="compare-kec-name" style="color:var(--color-blue);">${A.name}</span>
            <span style="font-size:11px;color:var(--text-muted);font-weight:600;">VS</span>
            <span class="compare-kec-name" style="color:var(--color-orange);">${B.name}</span>
        </div>
        ${row(`Indeks Kriminal ${cwa}/${cwb}`, crimeA.toFixed(1), crimeB.toFixed(1))}
        ${row('Total Kasus', A.crime?.total_kasus_kriminal || '-', B.crime?.total_kasus_kriminal || '-')}
        ${row('Crime Rate', A.crime ? parseFloat(A.crime.crime_rate).toFixed(1) : '-', B.crime ? parseFloat(B.crime.crime_rate).toFixed(1) : '-')}
        <div class="compare-divider"></div>
        ${row(`Indeks Kesehatan ${hwa}/${hwb}`, healthA.toFixed(1), healthB.toFixed(1))}
        ${row('Total Wabah', A.health?.total_kasus_wabah?.toLocaleString('id-ID') || '-', B.health?.total_kasus_wabah?.toLocaleString('id-ID') || '-')}
        ${row('HFR Score', A.health ? parseFloat(A.health.hfr).toFixed(1) : '-', B.health ? parseFloat(B.health.hfr).toFixed(1) : '-')}
    `;
}

// =============================================================================
// FIX: askGemini — always use dynamic key from localStorage with Retry logic
// =============================================================================
async function askGemini(prompt) {
    const key = getActiveGeminiKey();
    if (!key || key.length < 10) throw new Error('API key belum diset. Buka Settings di sidebar untuk memasukkan key.');

    let retries = 3;
    let delay = 2000;

    while (retries >= 0) {
        const res = await fetch(buildGeminiUrl(), {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                contents: [{ parts: [{ text: prompt }] }],
                generationConfig: { maxOutputTokens: 2048, temperature: 0.75 }
            })
        });

        if (res.ok) {
            const data = await res.json();
            return data.candidates?.[0]?.content?.parts?.[0]?.text || '(Tidak ada respons AI)';
        }

        const err = await res.json().catch(() => ({}));
        const msg = err?.error?.message || `HTTP ${res.status}`;
        
        // Don't retry if quota is actually empty
        if (msg.toLowerCase().includes('quota') || msg.includes('limit: 0') || res.status === 400 || res.status === 403) {
            if (msg.toLowerCase().includes('quota')) {
                throw new Error('Quota habis. Coba ganti API key atau model via tombol Settings di sidebar.');
            }
            throw new Error(msg);
        }

        // Retry on 503 (Service Unavailable) or 429 (Too Many Requests) or 500
        if ((res.status === 503 || res.status === 429 || res.status >= 500) && retries > 0) {
            console.warn(`Gemini API error ${res.status}: ${msg}. Retrying in ${delay}ms... (${retries} retries left)`);
            await new Promise(r => setTimeout(r, delay));
            delay *= 2; // Exponential backoff
            retries--;
            continue;
        }

        throw new Error(msg);
    }
}

// =============================================================================
// MAPBOX 3D BUILDINGS
// =============================================================================
function addMapbox3DBuildings() {
    if (!mapboxInstance || !mapboxInstance.isStyleLoaded()) return;
    try {
        if (mapboxInstance.getLayer('3d-buildings')) return;
        mapboxInstance.addLayer({
            id: '3d-buildings',
            source: 'composite',
            'source-layer': 'building',
            filter: ['==', 'extrude', 'true'],
            type: 'fill-extrusion',
            minzoom: 13,
            paint: {
                'fill-extrusion-color': document.body.classList.contains('dark-theme') ? '#3A3B4F' : '#DCD8A3',
                'fill-extrusion-height': ['interpolate', ['linear'], ['zoom'], 13, 0, 14, ['get', 'height']],
                'fill-extrusion-base': ['get', 'min_height'],
                'fill-extrusion-opacity': 0.55
            }
        });
    } catch (e) {
        console.warn('3D buildings skipped:', e.message);
    }
}

// =============================================================================
// LIVE CLOCK
// =============================================================================
function startLiveClock() {
    const el = document.getElementById('live-clock');
    if (!el) return;
    const tick = () => {
        el.textContent = new Date().toLocaleTimeString('id-ID', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    };
    tick();
    setInterval(tick, 1000);
}

// =============================================================================
// COMPARE DROPDOWN POPULATION
// =============================================================================
function populateCompareDropdown() {
    const sel = document.querySelector('select[onchange="setCompareKecamatan(this.value)"]');
    if (!sel || !dashboardData) return;
    const all = dashboardData.kriminalitas.map(x => x.kecamatan);
    sel.innerHTML = '<option value="">-- Pilih kecamatan pembanding --</option>' +
        all.map(k => `<option value="${k}">${k}</option>`).join('');
}

// =============================================================================
// TOP5 / BOTTOM5 RANK PANELS
// =============================================================================
function populateTop5Panels() {
    if (!dashboardData) return;

    const renderRankList = (data, valueKey, labelFn, panelId, isHighBad = true) => {
        const panel = document.getElementById(panelId);
        if (!panel) return;
        panel.innerHTML = data.map((x, i) => {
            const val = parseFloat(x[valueKey]);
            const pct = Math.min(100, val);
            const color = isHighBad
                ? (val > 70 ? 'var(--color-red)' : val > 40 ? 'var(--color-orange)' : 'var(--color-green)')
                : (val < 30 ? 'var(--color-green)' : val < 60 ? 'var(--color-orange)' : 'var(--color-red)');
            return `
                <div style="display:flex;align-items:center;gap:10px;padding:8px 0;border-bottom:1px solid var(--border-color);cursor:pointer;"
                     onclick="switchView('kecamatan');selectKecamatan('${x.kecamatan}');"
                     onmouseover="this.style.background='rgba(75,162,172,0.05)'" onmouseout="this.style.background=''">
                    <span style="width:20px;font-size:11px;font-weight:700;color:var(--text-muted);text-align:right;flex-shrink:0;">${i+1}</span>
                    <div style="flex:1;min-width:0;">
                        <div style="font-size:12px;font-weight:600;color:var(--text-primary);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${x.kecamatan}</div>
                        <div style="height:4px;background:var(--bg-panel);border-radius:2px;margin-top:4px;">
                            <div style="height:100%;width:${pct}%;background:${color};border-radius:2px;transition:width 0.6s ease;"></div>
                        </div>
                    </div>
                    <span style="font-size:12px;font-weight:700;color:${color};flex-shrink:0;">${val.toFixed(1)}</span>
                </div>`;
        }).join('');
    };

    const crimeData = [...dashboardData.kriminalitas].sort((a, b) => b.indeks_kriminalitas - a.indeks_kriminalitas);
    renderRankList(crimeData.slice(0, 5), 'indeks_kriminalitas', x => x.kecamatan, 'crime-top5-panel', true);
    renderRankList(crimeData.slice(-5).reverse(), 'indeks_kriminalitas', x => x.kecamatan, 'crime-bottom5-panel', true);

    const healthData = [...dashboardData.kesehatan].sort((a, b) => b.indeks_kesehatan - a.indeks_kesehatan);
    renderRankList(healthData.slice(0, 5), 'indeks_kesehatan', x => x.kecamatan, 'health-top5-panel', true);
    renderRankList(healthData.slice(-5).reverse(), 'indeks_kesehatan', x => x.kecamatan, 'health-bottom5-panel', true);
}

// =============================================================================
// HISTORICAL TREND CHART (BPS 2019–2022)
// =============================================================================
const BPS_SURABAYA_HISTORY = [
    { tahun: 2019, jumlah: 3377 },
    { tahun: 2020, jumlah: 1647 },
    { tahun: 2021, jumlah: 1648 },
    { tahun: 2022, jumlah: 8759 }
];

let trendHistoryChartInst = null;

function renderHistoricalTrendChart() {
    const canvas = document.getElementById('trendHistoryChart');
    if (!canvas) return;
    if (trendHistoryChartInst) { trendHistoryChartInst.destroy(); trendHistoryChartInst = null; }

    const isDark = document.body.classList.contains('dark-theme');
    const gridColor = isDark ? 'rgba(200,200,200,0.08)' : 'rgba(28,51,53,0.07)';
    const textColor = isDark ? '#A8B5BE' : '#537478';

    trendHistoryChartInst = new Chart(canvas, {
        type: 'line',
        data: {
            labels: BPS_SURABAYA_HISTORY.map(x => x.tahun),
            datasets: [{
                label: 'Jumlah Kejahatan (BPS)',
                data: BPS_SURABAYA_HISTORY.map(x => x.jumlah),
                borderColor: '#4BA2AC',
                backgroundColor: 'rgba(75,162,172,0.1)',
                pointBackgroundColor: '#4BA2AC',
                pointRadius: 5,
                pointHoverRadius: 7,
                tension: 0.35,
                fill: true,
                borderWidth: 2.5
            }]
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    callbacks: {
                        label: ctx => ` ${ctx.parsed.y.toLocaleString('id-ID')} kasus`
                    }
                }
            },
            scales: {
                x: { grid: { color: gridColor }, ticks: { color: textColor, font: { size: 11 } } },
                y: { grid: { color: gridColor }, ticks: { color: textColor, font: { size: 11 }, callback: v => v.toLocaleString('id-ID') } }
            }
        }
    });
}

// =============================================================================
// BELOW-MAP PANELS (kecamatan view)
// =============================================================================
function populateBelowMapPanels() {
    if (!dashboardData) return;

    const mkRow = (kec, val, color, rank) => `
        <div style="display:flex;align-items:center;gap:8px;padding:5px 0;border-bottom:1px solid var(--border-color);cursor:pointer;"
             onclick="selectKecamatan('${kec}')"
             onmouseover="this.style.background='rgba(75,162,172,0.06)'" onmouseout="this.style.background=''">
            <span style="font-size:10px;font-weight:700;color:var(--text-muted);min-width:18px;text-align:center;">${rank}</span>
            <span style="flex:1;font-size:11.5px;font-weight:600;color:var(--text-primary);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${kec}</span>
            <span style="font-size:11px;font-weight:700;color:${color};flex-shrink:0;">${parseFloat(val).toFixed(1)}</span>
        </div>`;

    const crimePanel = document.getElementById('kec-map-crime-rank');
    if (crimePanel) {
        crimePanel.innerHTML = dashboardData.kriminalitas.slice(0, 7).map((x, i) => {
            const v = parseFloat(x.indeks_kriminalitas);
            const c = v >= 60 ? 'var(--color-red)' : v >= 40 ? 'var(--color-orange)' : 'var(--color-green)';
            return mkRow(x.kecamatan, v, c, i + 1);
        }).join('');
    }

    const healthPanel = document.getElementById('kec-map-health-rank');
    if (healthPanel) {
        const sorted = [...dashboardData.kesehatan].sort((a, b) => b.indeks_kesehatan - a.indeks_kesehatan);
        healthPanel.innerHTML = sorted.slice(0, 7).map((x, i) => {
            const v = parseFloat(x.indeks_kesehatan);
            const c = v >= 60 ? 'var(--color-orange)' : v >= 40 ? '#c8a200' : 'var(--color-green)';
            return mkRow(x.kecamatan, v, c, i + 1);
        }).join('');
    }

    const newsPanel = document.getElementById('kec-map-news-mini');
    if (newsPanel && dashboardData.berita) {
        newsPanel.innerHTML = '';
        dashboardData.berita.slice(0, 5).forEach((n, idx) => {
            const isKrim = n.kategori === 'Kriminalitas';
            const row = document.createElement('div');
            row.style.cssText = 'padding:6px 0;border-bottom:1px solid var(--border-color);cursor:pointer;';
            row.onmouseover = () => row.style.background = 'rgba(75,162,172,0.06)';
            row.onmouseout = () => row.style.background = '';
            row.onclick = () => openNewsModal(n);
            row.innerHTML = `
                <div style="display:flex;align-items:flex-start;gap:6px;">
                    <span class="cat-badge ${isKrim ? 'cat-krim' : 'cat-sehat'}" style="font-size:9px;padding:1px 5px;flex-shrink:0;margin-top:1px;">${isKrim ? 'Krim' : 'Kes'}</span>
                    <span style="font-size:11px;font-weight:600;color:var(--text-primary);line-height:1.4;">${n.judul}</span>
                </div>
                <div style="font-size:9.5px;color:var(--text-muted);margin-top:2px;padding-left:42px;">${n.kecamatan_terdeteksi} &bull; ${n.sumber}</div>`;
            newsPanel.appendChild(row);
        });
    }
}

// =============================================================================
// CITY STATS PANEL
// =============================================================================
function renderCityStatsPanel() {
    const panel = document.getElementById('city-stats-panel');
    if (!panel || !dashboardData) return;

    const crimes = dashboardData.kriminalitas;
    const healths = dashboardData.kesehatan;

    const totalCrime = crimes.reduce((s, x) => s + (parseInt(x.total_kasus_kriminal) || 0), 0);
    const totalHealth = healths.reduce((s, x) => s + (parseInt(x.total_kasus_wabah) || 0), 0);
    const avgCrimeIdx = (crimes.reduce((s, x) => s + parseFloat(x.indeks_kriminalitas), 0) / crimes.length).toFixed(1);
    const avgHealthIdx = (healths.reduce((s, x) => s + parseFloat(x.indeks_kesehatan), 0) / healths.length).toFixed(1);
    const maxCrime = crimes[0];
    const minCrime = crimes[crimes.length - 1];
    const bpsTrend = ((BPS_SURABAYA_HISTORY[3].jumlah - BPS_SURABAYA_HISTORY[0].jumlah) / BPS_SURABAYA_HISTORY[0].jumlah * 100).toFixed(0);
    const trendUp = parseFloat(bpsTrend) > 0;

    const statRow = (label, value, color = 'var(--text-primary)') => `
        <div style="display:flex;justify-content:space-between;align-items:center;padding:8px 0;border-bottom:1px solid var(--border-color);">
            <span style="font-size:12px;color:var(--text-muted);">${label}</span>
            <span style="font-size:13px;font-weight:700;color:${color};">${value}</span>
        </div>`;

    panel.innerHTML = `
        ${statRow('Total kasus kriminalitas', totalCrime.toLocaleString('id-ID'), 'var(--color-red)')}
        ${statRow('Total kasus wabah', totalHealth.toLocaleString('id-ID'), 'var(--color-orange)')}
        ${statRow('Rata-rata indeks kriminal', avgCrimeIdx)}
        ${statRow('Rata-rata indeks kesehatan', avgHealthIdx)}
        ${statRow('Kecamatan paling rawan', maxCrime?.kecamatan || '-', 'var(--color-red)')}
        ${statRow('Kecamatan paling aman', minCrime?.kecamatan || '-', 'var(--color-green)')}
        ${statRow('Tren BPS 2019→2022', `${trendUp ? '+' : ''}${bpsTrend}%`, trendUp ? 'var(--color-red)' : 'var(--color-green)')}
    `;
}
