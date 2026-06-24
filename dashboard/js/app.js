// KECAMATRAS Dashboard Core Engine

// State Management
let dashboardData = null;
let currentView = 'dashboard'; // 'dashboard', 'kecamatan', 'kriminalitas', 'kesehatan', 'berita'
let currentMapLayer = 'kriminalitas'; // 'kriminalitas' or 'kesehatan'
let activeKecamatan = null;
let mapInstance = null;
let geojsonLayer = null;

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
    initDashboard();
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
        // Redraw chart to fit dimensions
        if (crimeChart) {
            crimeChart.resize();
        }
    } else if (viewId === 'kesehatan') {
        // Redraw chart to fit dimensions
        if (healthChart) {
            healthChart.resize();
        }
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
        background: var(--bg-primary); border-radius: 16px; padding: 28px 32px;
        width: 540px; max-width: 94vw; max-height: 80vh; overflow-y: auto;
        box-shadow: 0 20px 60px rgba(28,51,53,0.25); border: 1px solid var(--border-subtle);
        font-family: var(--font-primary);
    `;

    const listHTML = allKec.map((kec, i) => `
        <div style="display:flex; align-items:center; gap:12px; padding: 8px 12px; border-radius: 8px;
                    cursor:pointer; transition: background 0.15s;
                    background: ${i % 2 === 0 ? 'rgba(78,184,167,0.06)' : 'transparent'};"
             onmouseover="this.style.background='rgba(78,184,167,0.15)'"
             onmouseout="this.style.background='${i % 2 === 0 ? 'rgba(78,184,167,0.06)' : 'transparent'}'"
             onclick="document.getElementById('kec-list-modal').remove(); switchView('kecamatan'); selectKecamatan('${kec}');">
            <span style="width:24px; height:24px; background:var(--color-blue); color:white;
                         border-radius:50%; display:flex; align-items:center; justify-content:center;
                         font-size:10px; font-weight:700; flex-shrink:0;">${i+1}</span>
            <span style="font-size:13px; font-weight:600; color:var(--text-primary);">${kec}</span>
        </div>
    `).join('');

    modal.innerHTML = `
        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:20px;">
            <div>
                <h3 style="margin:0; font-size:17px; font-weight:700; color:var(--text-primary);">
                    <i class="fa-solid fa-map-location-dot" style="color:var(--color-blue); margin-right:8px;"></i>
                    31 Kecamatan Kota Surabaya
                </h3>
                <p style="margin:4px 0 0; font-size:12px; color:var(--text-secondary);">Klik kecamatan untuk melihat detail</p>
            </div>
            <button onclick="document.getElementById('kec-list-modal').remove()"
                    style="border:none; background:var(--border-subtle); color:var(--text-secondary);
                           width:30px; height:30px; border-radius:50%; cursor:pointer; font-size:14px;
                           display:flex; align-items:center; justify-content:center;">✕</button>
        </div>
        <div style="display:flex; flex-direction:column; gap:2px;">${listHTML}</div>
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

        // Use CartoDB Positron Tile layer for light cream theme
        L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
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
    if (score >= 80) return '#D95D39'; // Extreme (Terracotta Red)
    if (score >= 60) return '#4BA2AC'; // High (Steel Blue)
    if (score >= 40) return '#50C9BA'; // Medium (Teal)
    if (score >= 20) return '#9EE6CF'; // Waspada (Mint)
    return '#C1E6C4'; // Safe/Rendah (Pastel Green)
}

function styleFeature(feature) {
    const name = feature.properties.NAME_3 || feature.properties.kecamatan || feature.properties.name || "";
    const score = getIndexScore(name, currentMapLayer);
    
    return {
        fillColor: getColorByScore(score),
        weight: 1.5,
        opacity: 0.8,
        color: '#2C4F53',
        fillOpacity: 0.65
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
    layer.setStyle({
        weight: 3,
        color: '#1C3335',
        fillOpacity: 0.85
    });
    layer.bringToFront();
    
    const props = layer.feature.properties;
    const name = props.NAME_3 || props.kecamatan || props.name || "Kecamatan";
    const score = getIndexScore(name, currentMapLayer);
    
    const popupContent = `
        <div class="map-popup-detail">
            <h4>${name}</h4>
            <p>Indeks saat ini: <span class="popup-val">${parseFloat(score).toFixed(1)}</span></p>
            <p style="font-size: 9px; color: var(--text-muted);">Klik untuk detail lengkap</p>
        </div>
    `;
    
    layer.bindPopup(popupContent, { closeButton: false, offset: L.point(0, -10) }).openPopup();
}

function resetHighlight(e) {
    if (geojsonLayer) {
        geojsonLayer.resetStyle(e.target);
    }
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
    
    if (geojsonLayer && mapInstance) {
        geojsonLayer.eachLayer(l => {
            l.setStyle(styleFeature(l.feature));
        });
    } else {
        showFallbackGrid();
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
    
    const crimeData = dashboardData.kriminalitas.find(x => cleanKecamatanName(x.kecamatan) === cleanKecamatanName(kecName));
    const healthData = dashboardData.kesehatan.find(x => cleanKecamatanName(x.kecamatan) === cleanKecamatanName(kecName));
    
    if (!crimeData && !healthData) return;

    // Show Details loaded state
    document.getElementById('kecamatan-detail-empty').classList.add('hidden');
    const loadedPanel = document.getElementById('kecamatan-detail-loaded');
    loadedPanel.classList.remove('hidden');
    
    // Kecamatan Name
    document.getElementById('detail-kecamatan-name').innerHTML = `<i class="fa-solid fa-location-dot"></i> Kecamatan ${kecName}`;

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
            const item = document.createElement('a');
            item.className = 'kecamatan-news-mini-item';
            item.href = news.link;
            item.target = '_blank';
            item.innerHTML = `
                <div style="font-weight: 600; margin-bottom: 2px;">${news.judul}</div>
                <div style="font-size: 9px; color: var(--text-muted);">${news.sumber} &bull; ${dateStr}</div>
            `;
            miniNewsList.appendChild(item);
        });
    }

    // Centering Map Polygon if exists
    zoomToPolygon(kecName);
}

function zoomToPolygon(kecName) {
    if (!geojsonLayer || !mapInstance) return;
    const nameNorm = cleanKecamatanName(kecName);
    
    geojsonLayer.eachLayer(layer => {
        const props = layer.feature.properties;
        const polyName = props.NAME_3 || props.kecamatan || props.name || "";
        
        if (cleanKecamatanName(polyName) === nameNorm) {
            // Apply selected styling border
            layer.setStyle({
                weight: 3.5,
                color: '#1C3335',
                fillOpacity: 0.8
            });
            layer.bringToFront();
            
            // Pop up detail
            const score = getIndexScore(polyName, currentMapLayer);
            const popupContent = `
                <div class="map-popup-detail">
                    <h4>${polyName}</h4>
                    <p>Indeks saat ini: <span class="popup-val">${parseFloat(score).toFixed(1)}</span></p>
                </div>
            `;
            layer.bindPopup(popupContent, { closeButton: false, offset: L.point(0, -10) }).openPopup();
            
            // Fit bounds with zoom padding
            mapInstance.fitBounds(layer.getBounds(), { maxZoom: 13, animate: true, padding: [20, 20] });
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
    return {
        responsive: true,
        maintainAspectRatio: false,
        indexAxis: 'y', // Horizontal bars
        plugins: {
            legend: {
                labels: {
                    color: '#2C4F53', // Dark Teal text
                    font: { family: 'Outfit', size: 12, weight: '500' }
                }
            },
            title: {
                display: true,
                text: titleText,
                color: '#1C3335', // Primary Dark text
                font: { family: 'Outfit', size: 14, weight: '700' }
            }
        },
        scales: {
            x: {
                grid: { color: 'rgba(28, 51, 53, 0.1)' },
                ticks: { color: '#2C4F53', font: { family: 'Outfit' } },
                max: 100
            },
            y: {
                grid: { display: false },
                ticks: { color: '#2C4F53', font: { family: 'Outfit', weight: '600' } }
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

// Modal preview berita (mengganti direct link yang bisa 404)
function openNewsModal(item) {
    const existing = document.getElementById('news-detail-modal');
    if (existing) existing.remove();

    const isKrim = item.kategori === 'Kriminalitas';
    const dateStr = item.tanggal_publikasi ? formatISODate(item.tanggal_publikasi) : 'Baru-baru ini';
    const coverImg = isKrim 
        ? CRIME_IMAGES[0]
        : HEALTH_IMAGES[0];

    const overlay = document.createElement('div');
    overlay.id = 'news-detail-modal';
    overlay.style.cssText = `
        position: fixed; inset: 0; background: rgba(28,51,53,0.6); backdrop-filter: blur(5px);
        z-index: 9999; display: flex; align-items: center; justify-content: center;
        animation: fadeInModal 0.2s ease;
    `;
    overlay.onclick = (e) => { if (e.target === overlay) overlay.remove(); };

    const descSnippet = isKrim 
        ? `Insiden ${item.kategori.toLowerCase()} terdeteksi di wilayah Kecamatan ${item.kecamatan_terdeteksi} melalui sistem streaming Kafka KECAMATRAS. Data dianalisis menggunakan Spark medallion pipeline dan disimpan di Delta Lakehouse Gold Table. Anomali ini memengaruhi Indeks Kriminalitas kecamatan tersebut secara real-time.`
        : `Kejadian ${item.kategori.toLowerCase()} terdeteksi di wilayah Kecamatan ${item.kecamatan_terdeteksi} oleh sistem monitoring KECAMATRAS. Laporan diolah melalui Spark MLlib dan disimpan di Gold Layer HDFS. Parameter Indeks Risiko Kesehatan kecamatan ini diperbarui berdasarkan data berita ini.`;

    overlay.innerHTML = `
        <div style="background: var(--bg-primary); border-radius: 16px; overflow: hidden;
                    width: 560px; max-width: 95vw; max-height: 88vh; overflow-y: auto;
                    box-shadow: 0 24px 64px rgba(28,51,53,0.3); border: 1px solid var(--border-subtle);
                    font-family: var(--font-primary); animation: slideUpModal 0.25s ease;">
            <div style="position: relative; height: 180px; overflow: hidden;">
                <img src="${coverImg}" alt="" style="width:100%; height:100%; object-fit:cover;">
                <div style="position:absolute; inset:0; background: linear-gradient(to top, rgba(28,51,53,0.8) 0%, transparent 60%);"></div>
                <span class="cat-badge ${isKrim ? 'cat-krim' : 'cat-sehat'}" 
                      style="position:absolute; top:14px; left:14px;">${item.kategori}</span>
                <button onclick="document.getElementById('news-detail-modal').remove()"
                        style="position:absolute; top:12px; right:12px; border:none; background:rgba(255,255,255,0.9);
                               color:#333; width:28px; height:28px; border-radius:50%; cursor:pointer;
                               font-size:13px; display:flex; align-items:center; justify-content:center;">✕</button>
            </div>
            <div style="padding: 24px 28px 28px;">
                <div style="display:flex; align-items:center; gap:10px; margin-bottom:12px; font-size:12px; color:var(--text-secondary);">
                    <span style="font-weight:600;">${item.sumber}</span>
                    <span>•</span>
                    <span>${dateStr}</span>
                </div>
                <h3 style="margin:0 0 14px; font-size:17px; font-weight:700; color:var(--text-primary); line-height:1.4;">${item.judul}</h3>
                <div style="display:flex; align-items:center; gap:6px; margin-bottom:16px;
                            padding: 8px 12px; background: rgba(78,184,167,0.1); border-radius:8px;
                            border-left: 3px solid var(--color-blue);">
                    <i class="fa-solid fa-location-dot" style="color:var(--color-blue);"></i>
                    <span style="font-size:13px; font-weight:600; color:var(--text-primary);">Kecamatan ${item.kecamatan_terdeteksi}</span>
                </div>
                <p style="margin:0 0 20px; font-size:13.5px; line-height:1.65; color:var(--text-secondary);">${descSnippet}</p>
                <div style="display:flex; gap:10px;">
                    <button onclick="document.getElementById('news-detail-modal').remove(); switchView('kecamatan'); selectKecamatan('${item.kecamatan_terdeteksi}');" 
                            style="flex:1; padding:10px; background:var(--color-blue); color:white; border:none;
                                   border-radius:8px; cursor:pointer; font-family:var(--font-primary);
                                   font-size:13px; font-weight:600;">
                        <i class="fa-solid fa-map-location-dot"></i> Lihat Peta Kecamatan
                    </button>
                    <a href="${item.link}" target="_blank" rel="noopener noreferrer"
                       style="flex:1; padding:10px; background:transparent; color:var(--text-secondary); border:1px solid var(--border-subtle);
                              border-radius:8px; cursor:pointer; font-family:var(--font-primary);
                              font-size:13px; font-weight:600; text-decoration:none; display:flex;
                              align-items:center; justify-content:center; gap:6px;">
                        <i class="fa-brands fa-google-news" style="color:#4285f4;"></i>
                        <i class="fa-solid fa-newspaper"></i> Buka Berita
                    </a>
                </div>
                <p style="margin:12px 0 0; font-size:10px; color:var(--text-muted); text-align:center;">
                    ID: ${item.id_berita} • Dideteksi via Kafka KECAMATRAS Pipeline
                </p>
            </div>
        </div>
    `;

    document.body.appendChild(overlay);
}

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
        
        // Dynamic cover image rotation
        const coverImg = isKrim 
            ? CRIME_IMAGES[index % CRIME_IMAGES.length]
            : HEALTH_IMAGES[index % HEALTH_IMAGES.length];
            
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
            <div class="news-card-img">
                <span class="news-card-category cat-badge ${isKrim ? 'cat-krim' : 'cat-sehat'}">
                    ${item.kategori}
                </span>
                <img src="${coverImg}" alt="${item.judul}">
            </div>
            <div class="news-card-body">
                <div class="news-card-meta">
                    <span class="news-card-source">${item.sumber}</span>
                    <span>${dateStr}</span>
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
        const category = card.querySelector('.news-card-category').textContent.trim();
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
