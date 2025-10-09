// Global variables
let chartInstance = null;
let currentData = null;
let currentLevel = 'district';
let currentParent = null;
let navigationStack = [];
let allRegions = [];
let allDistricts = [];
let currentSort = 'net_cash_desc';
let selectedRegionFilter = '';
let selectedDistrictFilter = '';

// Initialize the application
document.addEventListener('DOMContentLoaded', () => {
    console.log('Reports Dashboard initializing...');
    
    // Set up event listeners
    document.getElementById('level-select').addEventListener('change', handleLevelChange);
    document.getElementById('refresh-btn').addEventListener('click', () => loadData(currentLevel, currentParent));
    document.getElementById('rollup-btn').addEventListener('click', handleRollUp);
    
    // Sidebar filter event listeners
    document.getElementById('sidebar-level-select').addEventListener('change', handleSidebarLevelChange);
    document.getElementById('sort-select').addEventListener('change', handleSortChange);
    document.getElementById('apply-filters-btn').addEventListener('click', applyFilters);
    document.getElementById('reset-filters-btn').addEventListener('click', resetFilters);
    document.getElementById('region-filter').addEventListener('change', handleRegionFilterChange);
    
    // Load initial data and populate filters
    loadData('district', null);
    loadFilterOptions();
});

// Load data from API (will call Python script)
async function loadData(level, parent = null) {
    try {
        // Update current state
        currentLevel = level;
        currentParent = parent;
        
        // Build API URL (this will need to be implemented in your server)
        let url = `/api/reports/district-net-cash?level=${level}`;
        if (parent) {
            url += `&parent=${encodeURIComponent(parent)}`;
        }
        
        console.log(`Fetching data for level: ${level}, parent: ${parent}`);
        
        // For now, use fallback data until server is implemented
        // const response = await fetch(url);
        // if (!response.ok) throw new Error('Failed to load data');
        // const result = await response.json();
        
        // Temporary fallback data
        const result = generateFallbackData(level, parent);
        
        if (result.success) {
            currentData = result.data;
            updateUI(result);
        } else {
            throw new Error(result.error || 'Unknown error');
        }
        
    } catch (error) {
        console.error('Error loading data:', error);
        alert('Failed to load data. Using sample data instead.');
        
        // Generate fallback data
        const fallbackResult = generateFallbackData(level, parent);
        currentData = fallbackResult.data;
        updateUI(fallbackResult);
    }
}

// Generate fallback data for testing
function generateFallbackData(level, parent) {
    const data = [];
    
    if (level === 'region') {
        const regions = ['North', 'South', 'East', 'West', 'Central'];
        regions.forEach(region => {
            data.push({
                label: region,
                total_net_cash: Math.random() * 10000000 + 5000000,
                district_count: Math.floor(Math.random() * 10) + 5,
                account_count: Math.floor(Math.random() * 500) + 100
            });
        });
    } else if (level === 'district') {
        const districts = parent ? 
            [`${parent} District A`, `${parent} District B`, `${parent} District C`] :
            ['Prague', 'Brno', 'Ostrava', 'Pilsen', 'Liberec', 'Olomouc', 'Usti', 'Hradec'];
        
        districts.forEach(district => {
            data.push({
                label: district,
                region: parent || ['North', 'South', 'East', 'West'][Math.floor(Math.random() * 4)],
                total_net_cash: Math.random() * 5000000 + 1000000,
                account_count: Math.floor(Math.random() * 200) + 50
            });
        });
    } else if (level === 'account') {
        for (let i = 0; i < 20; i++) {
            data.push({
                label: 1000 + i,
                district_name: parent || 'Prague',
                region: 'Central',
                total_net_cash: Math.random() * 500000 + 10000,
                transaction_count: Math.floor(Math.random() * 100) + 10
            });
        }
    }
    
    return {
        success: true,
        level: level,
        parent: parent,
        data: data,
        count: data.length
    };
}

// Update all UI components
function updateUI(result) {
    // Sort data before updating UI
    result.data = sortData(result.data);
    
    updateBreadcrumb();
    updateSummaryStats(result.data);
    updateChart(result);
    updateTable(result);
    updateControls();
    updateFilterVisibility();
    
    // Sync sidebar level select
    document.getElementById('sidebar-level-select').value = currentLevel;
}

// Update breadcrumb navigation
function updateBreadcrumb() {
    const breadcrumb = document.getElementById('breadcrumb');
    breadcrumb.innerHTML = '';
    
    // Always show "All Regions" as root
    const rootItem = document.createElement('span');
    rootItem.className = 'breadcrumb-item';
    rootItem.textContent = 'All Regions';
    rootItem.style.cursor = 'pointer';
    rootItem.onclick = () => {
        navigationStack = [];
        loadData('region', null);
    };
    breadcrumb.appendChild(rootItem);
    
    // Add navigation stack items
    navigationStack.forEach((item, index) => {
        const separator = document.createElement('span');
        separator.textContent = ' → ';
        separator.style.margin = '0 0.5rem';
        breadcrumb.appendChild(separator);
        
        const navItem = document.createElement('span');
        navItem.className = 'breadcrumb-item';
        navItem.textContent = item.label;
        navItem.style.cursor = 'pointer';
        navItem.onclick = () => {
            navigationStack = navigationStack.slice(0, index + 1);
            loadData(item.level, item.parent);
        };
        breadcrumb.appendChild(navItem);
    });
    
    // Add current level
    if (currentParent) {
        const separator = document.createElement('span');
        separator.textContent = ' → ';
        separator.style.margin = '0 0.5rem';
        breadcrumb.appendChild(separator);
        
        const currentItem = document.createElement('span');
        currentItem.className = 'breadcrumb-item active';
        currentItem.textContent = currentParent;
        breadcrumb.appendChild(currentItem);
    }
}

// Update summary statistics
function updateSummaryStats(data) {
    const totalCash = data.reduce((sum, item) => sum + item.total_net_cash, 0);
    const avgCash = totalCash / data.length;
    
    document.getElementById('total-cash').textContent = formatCurrency(totalCash);
    document.getElementById('item-count').textContent = data.length;
    document.getElementById('avg-cash').textContent = formatCurrency(avgCash);
    document.getElementById('current-level').textContent = 
        currentLevel.charAt(0).toUpperCase() + currentLevel.slice(1);
    
    const itemLabel = currentLevel === 'region' ? 'Regions' :
                     currentLevel === 'district' ? 'Districts' : 'Accounts';
    document.getElementById('item-label').textContent = itemLabel;
}

// Update chart
function updateChart(result) {
    const ctx = document.getElementById('netCashChart').getContext('2d');
    
    // Destroy existing chart
    if (chartInstance) {
        chartInstance.destroy();
    }
    
    // Prepare data
    const labels = result.data.map(item => item.label);
    const values = result.data.map(item => item.total_net_cash);
    
    // Create gradient
    const gradient = ctx.createLinearGradient(0, 0, 400, 0);
    gradient.addColorStop(0, 'rgba(75, 192, 192, 0.8)');
    gradient.addColorStop(1, 'rgba(54, 162, 235, 0.8)');
    
    // Create chart
    chartInstance = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Net Cash',
                data: values,
                backgroundColor: gradient,
                borderColor: 'rgba(75, 192, 192, 1)',
                borderWidth: 1
            }]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            onClick: (event, elements) => {
                if (elements.length > 0) {
                    const index = elements[0].index;
                    const clickedItem = result.data[index];
                    handleDrillDown(clickedItem);
                }
            },
            plugins: {
                legend: {
                    display: false
                },
                title: {
                    display: false
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return 'Net Cash: ' + formatCurrency(context.parsed.x);
                        }
                    }
                }
            },
            scales: {
                x: {
                    beginAtZero: true,
                    ticks: {
                        callback: function(value) {
                            return formatCurrency(value, true);
                        }
                    }
                }
            }
        }
    });
    
    // Update chart title
    const levelName = currentLevel.charAt(0).toUpperCase() + currentLevel.slice(1);
    const titleText = currentParent ? 
        `Net Cash by ${levelName} in ${currentParent}` :
        `Net Cash by ${levelName}`;
    document.getElementById('chart-title').textContent = titleText;
    
    // Update subtitle
    const canDrillDown = currentLevel !== 'account';
    document.getElementById('chart-subtitle').textContent = canDrillDown ?
        'Click on any bar to drill down' : 'Showing individual accounts';
}

// Update data table
function updateTable(result) {
    const tbody = document.getElementById('table-body');
    const thead = document.getElementById('table-head');
    tbody.innerHTML = '';
    
    // Update table headers based on level
    let headers = [];
    if (result.level === 'region') {
        headers = ['Region', 'Districts', 'Accounts', 'Net Cash'];
    } else if (result.level === 'district') {
        headers = ['District', 'Region', 'Accounts', 'Net Cash'];
    } else {
        headers = ['Account', 'District', 'Region', 'Transactions', 'Net Cash'];
    }
    
    thead.innerHTML = '<tr>' + headers.map(h => `<th>${h}</th>`).join('') + '</tr>';
    
    // Populate table rows
    result.data.forEach(item => {
        const row = document.createElement('tr');
        row.style.cursor = 'pointer';
        row.onclick = () => handleDrillDown(item);
        
        if (result.level === 'region') {
            row.innerHTML = `
                <td style="font-weight: 600;">${item.label}</td>
                <td>${item.district_count || '-'}</td>
                <td>${item.account_count || '-'}</td>
                <td>${formatCurrency(item.total_net_cash)}</td>
            `;
        } else if (result.level === 'district') {
            row.innerHTML = `
                <td style="font-weight: 600;">${item.label}</td>
                <td>${item.region || '-'}</td>
                <td>${item.account_count || '-'}</td>
                <td>${formatCurrency(item.total_net_cash)}</td>
            `;
        } else {
            row.innerHTML = `
                <td style="font-weight: 600;">${item.label}</td>
                <td>${item.district_name || '-'}</td>
                <td>${item.region || '-'}</td>
                <td>${item.transaction_count || '-'}</td>
                <td>${formatCurrency(item.total_net_cash)}</td>
            `;
        }
        
        tbody.appendChild(row);
    });
}

// Update controls
function updateControls() {
    const levelSelect = document.getElementById('level-select');
    const rollupBtn = document.getElementById('rollup-btn');
    
    levelSelect.value = currentLevel;
    
    // Show roll-up button if not at top level
    if (navigationStack.length > 0 || currentLevel !== 'region') {
        rollupBtn.style.display = 'inline-block';
    } else {
        rollupBtn.style.display = 'none';
    }
}

// Handle drill down
function handleDrillDown(item) {
    if (currentLevel === 'account') {
        console.log('Already at lowest level (account)');
        return;
    }
    
    // Add to navigation stack
    navigationStack.push({
        level: currentLevel,
        parent: currentParent,
        label: item.label
    });
    
    // Determine next level
    let nextLevel, nextParent;
    if (currentLevel === 'region') {
        nextLevel = 'district';
        nextParent = item.label;
    } else if (currentLevel === 'district') {
        nextLevel = 'account';
        nextParent = item.label;
    }
    
    loadData(nextLevel, nextParent);
}

// Handle roll up
function handleRollUp() {
    if (navigationStack.length === 0) {
        loadData('region', null);
        return;
    }
    
    const previous = navigationStack.pop();
    loadData(previous.level, previous.parent);
}

// Handle level change from dropdown
function handleLevelChange(event) {
    const newLevel = event.target.value;
    navigationStack = [];
    loadData(newLevel, null);
}

// Load filter options (regions and districts)
async function loadFilterOptions() {
    // Generate sample regions and districts for fallback
    allRegions = ['North', 'South', 'East', 'West', 'Central'];
    allDistricts = {
        'North': ['North District A', 'North District B', 'North District C'],
        'South': ['South District A', 'South District B', 'South District C'],
        'East': ['East District A', 'East District B', 'East District C'],
        'West': ['West District A', 'West District B', 'West District C'],
        'Central': ['Prague', 'Brno', 'Ostrava', 'Pilsen', 'Liberec']
    };
    
    // Populate region filter
    const regionFilter = document.getElementById('region-filter');
    regionFilter.innerHTML = '<option value="">All Regions</option>';
    allRegions.forEach(region => {
        const option = document.createElement('option');
        option.value = region;
        option.textContent = region;
        regionFilter.appendChild(option);
    });
}

// Handle sidebar level change
function handleSidebarLevelChange(event) {
    const newLevel = event.target.value;
    currentLevel = newLevel;
    
    // Update main level select
    document.getElementById('level-select').value = newLevel;
    
    // Show/hide filter sections based on level
    updateFilterVisibility();
}

// Update filter visibility based on current level
function updateFilterVisibility() {
    const regionSection = document.getElementById('region-filter-section');
    const districtSection = document.getElementById('district-filter-section');
    
    if (currentLevel === 'district') {
        regionSection.style.display = 'block';
        districtSection.style.display = 'none';
    } else if (currentLevel === 'account') {
        regionSection.style.display = 'block';
        districtSection.style.display = 'block';
    } else {
        regionSection.style.display = 'none';
        districtSection.style.display = 'none';
    }
}

// Handle region filter change (populate district filter)
function handleRegionFilterChange(event) {
    const selectedRegion = event.target.value;
    const districtFilter = document.getElementById('district-filter');
    
    districtFilter.innerHTML = '<option value="">All Districts</option>';
    
    if (selectedRegion && allDistricts[selectedRegion]) {
        allDistricts[selectedRegion].forEach(district => {
            const option = document.createElement('option');
            option.value = district;
            option.textContent = district;
            districtFilter.appendChild(option);
        });
    }
}

// Handle sort change
function handleSortChange(event) {
    currentSort = event.target.value;
}

// Apply filters
function applyFilters() {
    selectedRegionFilter = document.getElementById('region-filter').value;
    selectedDistrictFilter = document.getElementById('district-filter').value;
    
    // Determine what to load based on filters
    if (currentLevel === 'district' && selectedRegionFilter) {
        loadData('district', selectedRegionFilter);
    } else if (currentLevel === 'account' && selectedDistrictFilter) {
        loadData('account', selectedDistrictFilter);
    } else if (currentLevel === 'account' && selectedRegionFilter) {
        // Load all accounts in the region
        loadData('account', null);
    } else {
        loadData(currentLevel, null);
    }
}

// Reset filters
function resetFilters() {
    document.getElementById('sidebar-level-select').value = 'district';
    document.getElementById('region-filter').value = '';
    document.getElementById('district-filter').value = '';
    document.getElementById('sort-select').value = 'net_cash_desc';
    
    selectedRegionFilter = '';
    selectedDistrictFilter = '';
    currentSort = 'net_cash_desc';
    currentLevel = 'district';
    navigationStack = [];
    
    document.getElementById('level-select').value = 'district';
    
    updateFilterVisibility();
    loadData('district', null);
}

// Sort data based on current sort option
function sortData(data) {
    const sortedData = [...data];
    
    switch (currentSort) {
        case 'net_cash_desc':
            sortedData.sort((a, b) => b.total_net_cash - a.total_net_cash);
            break;
        case 'net_cash_asc':
            sortedData.sort((a, b) => a.total_net_cash - b.total_net_cash);
            break;
        case 'alpha_asc':
            sortedData.sort((a, b) => String(a.label).localeCompare(String(b.label)));
            break;
        case 'alpha_desc':
            sortedData.sort((a, b) => String(b.label).localeCompare(String(a.label)));
            break;
    }
    
    return sortedData;
}

// Format currency
function formatCurrency(value, short = false) {
    if (short && Math.abs(value) >= 1000000) {
        return '$' + (value / 1000000).toFixed(1) + 'M';
    } else if (short && Math.abs(value) >= 1000) {
        return '$' + (value / 1000).toFixed(0) + 'K';
    }
    
    return new Intl.NumberFormat('en-US', {
        style: 'currency',
        currency: 'USD',
        minimumFractionDigits: 0,
        maximumFractionDigits: 0
    }).format(value);
}
