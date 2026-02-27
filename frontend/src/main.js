// Socket.IO is loaded from CDN in index.html
// import { io } from 'socket.io-client'; // Not needed when using CDN
import './style.css';

// Backend URL - use environment variable in production, fallback to localhost for development
const BACKEND_URL = import.meta.env.VITE_BACKEND_URL || 'http://localhost:8000';

// State
let orderCount = 0;
let buyCount = 0;
let sellCount = 0;
let totalVolume = 0;
const maxRows = 4000;
const ordersMap = new Map(); // Track orders by unique ID

// Debounce timers for summary updates
let summaryUpdateTimer = null;
let oppositePartiesUpdateTimer = null;

// Column visibility state - default columns based on user preference
const columnVisibility = {
  'date': true,
  'order-time': false,
  'received-at': true,
  'side': false,
  'league': true,
  'title': false,
  'token-label': true,
  'token-id': false,
  'market-slug': false,
  'condition-id': false,
  'price': true,
  'shares-norm': true,
  'shares-raw': false,
  'value': true,
  'block-number': false,
  'log-index': false,
  'user': false,
  'taker': true,
  'order-hash': false,
  'tx-hash': true,
  'pm-before': true,
  'pm-after': true,
  'pm-sportbook': true,
  'pn-before': true,
  'pn-after': true,
  'pn-sportbook': true
};

// DOM elements
let app;
let socket;

function getOrderIdCandidates(data) {
  const candidates = [];
  const txHash = data?.tx_hash || '';
  const orderHash = data?.order_hash || '';
  const hasLogIndex = data?.log_index !== undefined && data?.log_index !== null && data?.log_index !== '';
  const logIndex = hasLogIndex ? data.log_index : '';
  const hasBlock = data?.block_number !== undefined && data?.block_number !== null && data?.block_number !== '';
  const blockNumber = hasBlock ? data.block_number : '';

  // Legacy/current key formats (kept for backward compatibility with existing rows in DOM/map)
  if (hasLogIndex && txHash) candidates.push(`${txHash}_${logIndex}`);
  if (hasLogIndex && orderHash) candidates.push(`${orderHash}_${logIndex}`);
  if (hasLogIndex && blockNumber !== '') candidates.push(`${blockNumber}_${logIndex}`);
  if (hasLogIndex) candidates.push(`_${logIndex}`); // Existing rows created before tx_hash arrives

  // Fallbacks when log index is not available
  if (txHash) candidates.push(txHash);
  if (orderHash) candidates.push(orderHash);

  // Unique, non-empty
  return [...new Set(candidates.filter(Boolean))];
}

function getPrimaryOrderId(data) {
  const candidates = getOrderIdCandidates(data);
  if (candidates.length > 0) return candidates[0];
  return `temp_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

function findExistingOrderId(data) {
  const candidates = getOrderIdCandidates(data);
  return candidates.find((id) => ordersMap.has(id)) || null;
}

// Format functions
const UTC_PLUS_8_OFFSET_HOURS = 8;

function convertToUtcPlus8(date) {
  // date.getTime() is already UTC epoch milliseconds.
  // Shift by +8h and render with UTC getters for fixed Singapore display.
  return new Date(date.getTime() + (UTC_PLUS_8_OFFSET_HOURS * 60 * 60 * 1000));
}

function formatTimeWithMs(date, useUtcPlus8 = false) {
  if (!date || !(date instanceof Date) || isNaN(date.getTime())) {
    date = new Date();
  }
  const displayDate = useUtcPlus8 ? convertToUtcPlus8(date) : date;
  const hours = String(useUtcPlus8 ? displayDate.getUTCHours() : displayDate.getHours()).padStart(2, '0');
  const minutes = String(useUtcPlus8 ? displayDate.getUTCMinutes() : displayDate.getMinutes()).padStart(2, '0');
  const seconds = String(useUtcPlus8 ? displayDate.getUTCSeconds() : displayDate.getSeconds()).padStart(2, '0');
  const milliseconds = String(useUtcPlus8 ? displayDate.getUTCMilliseconds() : displayDate.getMilliseconds()).padStart(3, '0');
  return `${hours}:${minutes}:${seconds}.${milliseconds}`;
}

function formatAddress(addr) {
  return addr ? `${addr.substring(0, 6)}...${addr.substring(addr.length - 4)}` : '-';
}

function formatHash(hash, start = 10, end = 8) {
  return hash ? `${hash.substring(0, start)}...${hash.substring(hash.length - end)}` : '-';
}

function formatTxHashLink(txHash) {
  if (!txHash) return '-';
  const shortHash = formatHash(txHash, 10, 8);
  const url = `https://polygonscan.com/tx/${txHash}`;
  return `<a href="${url}" target="_blank" rel="noopener noreferrer" class="tx-link">${shortHash}</a>`;
}

function extractLeague(marketSlug) {
  if (!marketSlug) return '-';
  const parts = marketSlug.split('-');
  return parts.length > 0 ? parts[0].toUpperCase() : '-';
}

function formatTimeAgo(dateString) {
  if (!dateString) return '';
  
  try {
    const date = new Date(dateString);
    const now = new Date();
    const diffMs = now - date;
    const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));
    const diffMonths = Math.floor(diffDays / 30);
    const diffYears = Math.floor(diffDays / 365);
    
    if (diffYears > 0) {
      return `${diffYears} year${diffYears > 1 ? 's' : ''} ago`;
    } else if (diffMonths > 0) {
      return `${diffMonths} month${diffMonths > 1 ? 's' : ''} ago`;
    } else if (diffDays > 0) {
      return `${diffDays} day${diffDays > 1 ? 's' : ''} ago`;
    } else {
      const diffHours = Math.floor(diffMs / (1000 * 60 * 60));
      if (diffHours > 0) {
        return `${diffHours} hour${diffHours > 1 ? 's' : ''} ago`;
      } else {
        const diffMinutes = Math.floor(diffMs / (1000 * 60));
        return diffMinutes > 0 ? `${diffMinutes} minute${diffMinutes > 1 ? 's' : ''} ago` : 'just now';
      }
    }
  } catch (e) {
    return '';
  }
}

function parseReceivedAt(receivedAt) {
  if (!receivedAt) return new Date();
  const raw = String(receivedAt);
  // If timestamp is naive (no timezone suffix), treat it as UTC for consistency.
  const hasTimezone = /([zZ]|[+\-]\d{2}:\d{2})$/.test(raw);
  const normalized = hasTimezone ? raw : `${raw}Z`;
  const parsed = new Date(normalized);
  return Number.isNaN(parsed.getTime()) ? new Date() : parsed;
}

function calculatePolymarketPercentage(fillPrice, pmData) {
  if (!pmData || pmData.bbo === undefined || pmData.bbo === null || !fillPrice || fillPrice === 0) {
    return null;
  }
  return ((fillPrice - pmData.bbo) / fillPrice) * 100;
}

function formatPolymarketPercentage(fillPrice, pmData, label, orderId) {
  const percentage = calculatePolymarketPercentage(fillPrice, pmData);
  const hasData = pmData && pmData.bbo !== undefined && pmData.bbo !== null;
  const expandableId = `pm-${label.toLowerCase()}-${orderId}`;
  
  if (!hasData) {
    return `<div class="pm-compact">-</div>`;
  }
  
  const sign = percentage >= 0 ? '+' : '';
  // Green for negative (profitable - paid less than market), Red for positive (paid more than market)
  const percentageClass = percentage < 0 ? 'pm-positive' : 'pm-negative';
  
  return `<div class="pm-compact">
    <div class="pm-percentage ${percentageClass}" onclick="togglePmDetails('${expandableId}')" title="Click to expand details">
      ${sign}${percentage.toFixed(2)}%
      <span class="pm-toggle">▶</span>
    </div>
    <div id="${expandableId}" class="pm-details-expanded" style="display: none;">
      ${formatPolymarketDetails(pmData, label)}
    </div>
  </div>`;
}

function formatPolymarketDetails(pmData, label) {
  if (!pmData || pmData.bbo === undefined || pmData.bbo === null) {
    return `<div class="pm-details">
      <div class="pm-label">${label}</div>
      <div class="pm-value">No data</div>
    </div>`;
  }

  const isPinnacle = pmData.source === 'pinnacle' || label.startsWith('PN_');
  if (isPinnacle) {
    const prettyLabel = label === 'PN_BEFORE'
      ? 'PINNACLE DELTAS (BEFORE)'
      : label === 'PN_AFTER'
        ? 'PINNACLE DELTAS (AFTER)'
        : label;
    const percentageRaw = pmData.percentage;
    const percentageDisplay = percentageRaw === undefined || percentageRaw === null || percentageRaw === ''
      ? '-'
      : String(percentageRaw).includes('%')
        ? String(percentageRaw)
        : `${percentageRaw}%`;
    const oppPercentageRaw = pmData.opponent_percentage;
    const oppPercentageDisplay = oppPercentageRaw === undefined || oppPercentageRaw === null || oppPercentageRaw === ''
      ? '-'
      : String(oppPercentageRaw).includes('%')
        ? String(oppPercentageRaw)
        : `${oppPercentageRaw}%`;

    return `<div class="pm-details">
      <div class="pm-label">${prettyLabel}</div>
      <div class="pm-row">
        <span class="pm-field">TIMESTAMP:</span>
        <span class="pm-value timestamp">${pmData.timestamp || '-'}</span>
      </div>
      <div class="pm-row">
        <span class="pm-field">TEAM:</span>
        <span class="pm-value">${pmData.team || '-'}</span>
      </div>
      <div class="pm-row">
        <span class="pm-field">OPPOSING TEAM:</span>
        <span class="pm-value">${pmData.opposing_team || '-'}</span>
      </div>
      <div class="pm-row">
        <span class="pm-field">BBO PRICE:</span>
        <span class="pm-value">$${Number(pmData.bbo).toFixed(3)}</span>
      </div>
      <div class="pm-row">
        <span class="pm-field">PERCENTAGE ODDS:</span>
        <span class="pm-value">${percentageDisplay}</span>
      </div>
      <div class="pm-row">
        <span class="pm-field">OPP PERCENTAGE ODDS:</span>
        <span class="pm-value">${oppPercentageDisplay}</span>
      </div>
      <div class="pm-row">
        <span class="pm-field">SECONDS FROM FILL:</span>
        <span class="pm-value">${pmData.seconds_from_fill !== undefined ? `${pmData.seconds_from_fill}s` : '-'}</span>
      </div>
    </div>`;
  }
  
  const spread = pmData.spread !== undefined && pmData.spread !== null ? pmData.spread : null;
  
  return `<div class="pm-details">
    <div class="pm-label">${label}</div>
    <div class="pm-row">
      <span class="pm-field">BBO:</span>
      <span class="pm-value">$${pmData.bbo.toFixed(3)}</span>
    </div>
    <div class="pm-row">
      <span class="pm-field">BEST BID:</span>
      <span class="pm-value">$${pmData.best_bid ? pmData.best_bid.toFixed(3) : '-'}</span>
    </div>
    <div class="pm-row">
      <span class="pm-field">BEST ASK:</span>
      <span class="pm-value">$${pmData.best_ask ? pmData.best_ask.toFixed(3) : '-'}</span>
    </div>
    <div class="pm-row">
      <span class="pm-field">SPREAD:</span>
      <span class="pm-value">${spread !== null ? `$${spread.toFixed(4)}` : '-'}</span>
    </div>
    <div class="pm-row">
      <span class="pm-field">SECONDS FROM FILL:</span>
      <span class="pm-value">${pmData.seconds_from_fill !== undefined ? `${pmData.seconds_from_fill}s` : '-'}</span>
    </div>
    ${pmData.timestamp ? `<div class="pm-row">
      <span class="pm-field">TIMESTAMP:</span>
      <span class="pm-value timestamp">${pmData.timestamp}</span>
    </div>` : ''}
  </div>`;
}

// Global function for toggling details
window.togglePmDetails = function(expandableId) {
  const element = document.getElementById(expandableId);
  if (element) {
    const isVisible = element.style.display !== 'none';
    element.style.display = isVisible ? 'none' : 'block';
    const toggle = element.previousElementSibling?.querySelector('.pm-toggle');
    if (toggle) {
      // Show ▶ when collapsed, ▼ when expanded
      toggle.textContent = isVisible ? '▶' : '▼';
    }
  }
};

// Global function for toggling show all parties
window.toggleShowAllParties = function() {
  showAllParties = !showAllParties;
  loadOppositePartiesSummary();
};

// Column visibility functions
window.toggleColumnMenu = function() {
  const menu = document.getElementById('column-menu');
  if (menu) {
    menu.style.display = menu.style.display === 'none' ? 'block' : 'none';
  }
};

window.toggleColumn = function(columnName, visible) {
  columnVisibility[columnName] = visible;
  
  // Update all table headers
  const headers = document.querySelectorAll(`th[data-column="${columnName}"]`);
  headers.forEach(header => {
    header.style.display = visible ? '' : 'none';
  });
  
  // Update all table cells
  const cells = document.querySelectorAll(`td[data-column="${columnName}"]`);
  cells.forEach(cell => {
    cell.style.display = visible ? '' : 'none';
  });
  
  // Update checkbox state
  const checkbox = document.querySelector(`input[data-column="${columnName}"]`);
  if (checkbox) {
    checkbox.checked = visible;
  }
  
  // Apply to all existing rows
  applyColumnVisibilityToAll();
};

function applyColumnVisibility(row) {
  if (!row) return;
  Object.keys(columnVisibility).forEach(columnName => {
    const cells = row.querySelectorAll(`td[data-column="${columnName}"], th[data-column="${columnName}"]`);
    cells.forEach(cell => {
      cell.style.display = columnVisibility[columnName] ? '' : 'none';
    });
  });
}

// Apply visibility to all existing rows
function applyColumnVisibilityToAll() {
  const rows = document.querySelectorAll('#orders-table tr');
  rows.forEach(row => applyColumnVisibility(row));
  const headers = document.querySelectorAll('thead th[data-column]');
  headers.forEach(header => {
    const columnName = header.getAttribute('data-column');
    header.style.display = columnVisibility[columnName] ? '' : 'none';
  });
}

// Close column menu and time filter menu when clicking outside
document.addEventListener('click', function(event) {
  const menu = document.getElementById('column-menu');
  const button = document.querySelector('.column-toggle-btn');
  if (menu && button && !menu.contains(event.target) && !button.contains(event.target)) {
    menu.style.display = 'none';
  }
  const timeMenu = document.getElementById('time-filter-menu');
  const timeButton = document.getElementById('time-filter-btn');
  const timeContainer = document.querySelector('.time-filter-container');
  if (timeMenu && timeContainer && !timeContainer.contains(event.target)) {
    timeMenu.style.display = 'none';
  }
});

// Time filter functions - make them global so they can be called from inline onclick
window.toggleTimeFilterMenu = function(event) {
  if (event) {
    event.stopPropagation();
    event.preventDefault();
  }
  const menu = document.getElementById('time-filter-menu');
  if (menu) {
    const isVisible = menu.style.display !== 'none' && menu.style.display !== '';
    menu.style.display = isVisible ? 'none' : 'block';
  }
};

window.setTimeFilter = function(filter) {
  timeFilter = filter;
  
  // Update label
  const labelMap = {
    '3h': 'Last 3 Hours',
    '8h': 'Last 8 Hours',
    '12h': 'Last 12 Hours',
    '24h': 'Last 24 Hours',
    'all': 'All Time'
  };
  const labelEl = document.getElementById('time-filter-label');
  if (labelEl) {
    labelEl.textContent = labelMap[filter];
  }
  
  // Update checkmarks
  ['3h', '8h', '12h', '24h'].forEach(f => {
    const checkEl = document.getElementById(`time-filter-check-${f}`);
    if (checkEl) {
      checkEl.style.display = f === filter ? 'inline' : 'none';
    }
  });
  
  // Close menu
  const menu = document.getElementById('time-filter-menu');
  if (menu) menu.style.display = 'none';

  // Apply filter to the main transaction table
  applyTimeFilterToMainTable();
  
  // Refresh all summaries
  loadPMSummary();
  loadPinnacleSummary();
  loadGameSummaries();
  loadOppositePartiesSummary();
};

window.setGameSlugFilter = function(value) {
  gameSlugFilter = (value || '').trim().toLowerCase();

  // Apply filter to the main transaction table
  applyTimeFilterToMainTable();

  // Refresh all summaries/cards
  loadPMSummary();
  loadPinnacleSummary();
  loadGameSummaries();
  loadOppositePartiesSummary();
};

window.setCounterpartyFilter = function(value) {
  counterpartyFilter = (value || '').trim().toLowerCase();
  // Apply to main transaction table too
  applyTimeFilterToMainTable();

  // Keep summaries/cards in sync with active filters
  loadPMSummary();
  loadPinnacleSummary();
  loadGameSummaries();
  loadOppositePartiesSummary();
};

window.setUserFilter = function(value) {
  userFilter = (value || '').trim().toLowerCase();
  applyTimeFilterToMainTable();
  loadPMSummary();
  loadPinnacleSummary();
  loadGameSummaries();
  loadOppositePartiesSummary();
};

// Filter orders by time range
function getOrderTimestampMs(order) {
  if (order && order.timestamp) {
    return order.timestamp * 1000;
  }
  if (order && order.received_at) {
    const receivedMs = parseReceivedAt(order.received_at).getTime();
    if (!Number.isNaN(receivedMs)) {
      return receivedMs;
    }
  }
  return null;
}

function isOrderInCurrentTimeFilter(order) {
  if (timeFilter === 'all') {
    return true;
  }
  
  const now = Date.now();
  const hoursMap = {
    '3h': 3,
    '8h': 8,
    '12h': 12,
    '24h': 24
  };
  
  const hours = hoursMap[timeFilter] || 24;
  const cutoffTime = now - (hours * 60 * 60 * 1000);
  const orderTime = getOrderTimestampMs(order);
  if (!orderTime) return false;
  return orderTime >= cutoffTime;
}

function isOrderInCurrentGameSlugFilter(order) {
  if (!gameSlugFilter) return true;
  const slug = (order?.market_slug || '').toLowerCase();
  return slug.includes(gameSlugFilter);
}

function isOrderInCurrentCounterpartyFilter(order) {
  if (!counterpartyFilter) return true;
  const user = (order?.user || '').toLowerCase();
  const taker = (order?.taker || '').toLowerCase();
  return user.includes(counterpartyFilter) || taker.includes(counterpartyFilter);
}

function isOrderInCurrentUserFilter(order) {
  if (!userFilter) return true;
  const user = (order?.user || '').toLowerCase();
  return user.includes(userFilter);
}

function isOrderVisibleByFilters(order) {
  return (
    isOrderInCurrentTimeFilter(order) &&
    isOrderInCurrentGameSlugFilter(order) &&
    isOrderInCurrentCounterpartyFilter(order) &&
    isOrderInCurrentUserFilter(order)
  );
}

function applyTimeFilterToMainTable() {
  const tableBody = document.getElementById('orders-table');
  if (!tableBody) return;

  const rows = tableBody.querySelectorAll('tr[data-order-id]');
  rows.forEach((row) => {
    const orderId = row.getAttribute('data-order-id');
    const order = ordersMap.get(orderId);
    row.style.display = isOrderVisibleByFilters(order) ? '' : 'none';
  });
}

function getFilteredOrders() {
  const allOrders = Array.from(ordersMap.values());
  return allOrders.filter(isOrderVisibleByFilters);
}

function formatSportbook(sportbookData, fillPrice = null) {
  if (!sportbookData || sportbookData.best_bid === undefined || sportbookData.best_bid === null) return '-';
  const bestBid = Number(sportbookData.best_bid);
  const effectiveFill = fillPrice !== null && fillPrice !== undefined
    ? Number(fillPrice)
    : Number(sportbookData.fill_price);
  // Always compute from fill and best bid: (fill - best_bid) * 100
  const diffPct = Number.isFinite(effectiveFill) && Number.isFinite(bestBid)
    ? (effectiveFill - bestBid) * 100
    : Number(sportbookData.price_diff_pct ?? 0);
  const sign = diffPct >= 0 ? '+' : '';
  const color = diffPct < 0 ? 'var(--accent-green)' : diffPct > 0 ? 'var(--accent-red)' : 'var(--text-primary)';
  return `<span style="font-weight: 700; color: ${color};">${sign}${diffPct.toFixed(2)}%</span>`;
}

function parsePercentageToDecimal(value) {
  if (value === null || value === undefined || value === '') return null;
  if (typeof value === 'string') {
    const cleaned = value.replace('%', '').trim();
    const parsed = Number(cleaned);
    if (!Number.isFinite(parsed)) return null;
    return parsed > 1 ? parsed / 100 : parsed;
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return null;
  return numeric > 1 ? numeric / 100 : numeric;
}

function formatPinnacleSportbook(fillPrice, pinnacleAfter, fallbackSportbookData = null) {
  const fill = Number(fillPrice);
  const oppDecimal = parsePercentageToDecimal(pinnacleAfter?.opponent_percentage);
  let diffPct = null;

  // Requested formula:
  // (Fill Price - (1 - Opposite percentage odds)) * 100
  if (Number.isFinite(fill) && oppDecimal !== null) {
    const impliedFromOpp = 1 - oppDecimal;
    diffPct = (fill - impliedFromOpp) * 100;
  } else if (fallbackSportbookData && fallbackSportbookData.price_diff_pct !== undefined) {
    // Safe fallback for older cached rows
    diffPct = Number(fallbackSportbookData.price_diff_pct);
  }

  if (diffPct === null || !Number.isFinite(diffPct)) return '-';

  const sign = diffPct >= 0 ? '+' : '';
  const color = diffPct < 0 ? 'var(--accent-green)' : diffPct > 0 ? 'var(--accent-red)' : 'var(--text-primary)';
  return `<span style="font-weight: 700; color: ${color};">${sign}${diffPct.toFixed(2)}%</span>`;
}

// State for opposite parties table expansion
let showAllParties = false;
const DEFAULT_PARTIES_TO_SHOW = 10;

// Time filter state
let timeFilter = 'all'; // '3h', '8h', '12h', '24h', 'all'
let gameSlugFilter = ''; // free text, case-insensitive
let counterpartyFilter = ''; // free text, case-insensitive
let userFilter = ''; // free text, case-insensitive
let showGameCards = false;

function applyGameCardsVisibility() {
  const gameContainer = document.getElementById('game-summaries-container');
  const toggleBtn = document.getElementById('toggle-game-cards-btn');

  if (gameContainer) {
    gameContainer.style.display = showGameCards ? 'block' : 'none';
  }
  if (toggleBtn) {
    toggleBtn.textContent = showGameCards ? 'Hide Game Cards' : 'Show Game Cards';
  }
}

window.toggleGameCardsSection = function() {
  showGameCards = !showGameCards;
  applyGameCardsVisibility();
};

// Calculate and display game summaries
async function loadGameSummaries() {
  try {
    const orders = getFilteredOrders();
    
    // Group by game slug
    const gameStats = {};
    
    orders.forEach(order => {
      const gameSlug = order.market_slug;
      if (!gameSlug) return;
      
      if (!gameStats[gameSlug]) {
        gameStats[gameSlug] = {
          orders: [],
          totalVolume: 0,
          fillCount: 0,
          beforeSpread: 0,
          afterSpread: 0,
          sportbookSpread: 0,
          pinnacleBeforeSpread: 0,
          pinnacleAfterSpread: 0,
          beforeCount: 0,
          afterCount: 0,
          sportbookCount: 0,
          pinnacleBeforeCount: 0,
          pinnacleAfterCount: 0,
          takers: {} // Track takers for this game
        };
      }
      
      const fillValue = (order.price || 0) * (order.shares_normalized || 0);
      gameStats[gameSlug].orders.push(order);
      gameStats[gameSlug].totalVolume += fillValue;
      gameStats[gameSlug].fillCount += 1;
      
      // Track taker volume
      const taker = order.taker;
      if (taker) {
        if (!gameStats[gameSlug].takers[taker]) {
          gameStats[gameSlug].takers[taker] = {
            volume: 0,
            fills: 0,
            afterPercentages: [], // Track percentage differences instead of raw BBO
            afterCount: 0
          };
        }
        gameStats[gameSlug].takers[taker].volume += fillValue;
        gameStats[gameSlug].takers[taker].fills += 1;
        
        // Track Poly After percentage for this taker (like PM After column)
        if (order.polymarket_after && order.polymarket_after.bbo !== undefined && order.polymarket_after.bbo !== null && order.price && order.price > 0) {
          const percentage = ((order.price - order.polymarket_after.bbo) / order.price) * 100;
          gameStats[gameSlug].takers[taker].afterPercentages.push(percentage);
          gameStats[gameSlug].takers[taker].afterCount += 1;
        }
      }
      
      // PM Before
      if (order.polymarket_before && order.polymarket_before.bbo !== undefined && order.polymarket_before.bbo !== null) {
        const spread = (order.price - order.polymarket_before.bbo) * (order.shares_normalized || 0);
        gameStats[gameSlug].beforeSpread += spread;
        gameStats[gameSlug].beforeCount += 1;
      }
      
      // PM After
      if (order.polymarket_after && order.polymarket_after.bbo !== undefined && order.polymarket_after.bbo !== null) {
        const spread = (order.price - order.polymarket_after.bbo) * (order.shares_normalized || 0);
        gameStats[gameSlug].afterSpread += spread;
        gameStats[gameSlug].afterCount += 1;
      }
      
      // PM Sportbook
      if (order.sportbook && order.sportbook.best_bid !== undefined && order.sportbook.best_bid !== null) {
        const spread = (order.price - order.sportbook.best_bid) * (order.shares_normalized || 0);
        gameStats[gameSlug].sportbookSpread += spread;
        gameStats[gameSlug].sportbookCount += 1;
      }

      // Pinnacle Before
      if (order.pinnacle_before && order.pinnacle_before.bbo !== undefined && order.pinnacle_before.bbo !== null) {
        const spread = (order.price - order.pinnacle_before.bbo) * (order.shares_normalized || 0);
        gameStats[gameSlug].pinnacleBeforeSpread += spread;
        gameStats[gameSlug].pinnacleBeforeCount += 1;
      }

      // Pinnacle After
      if (order.pinnacle_after && order.pinnacle_after.bbo !== undefined && order.pinnacle_after.bbo !== null) {
        const spread = (order.price - order.pinnacle_after.bbo) * (order.shares_normalized || 0);
        gameStats[gameSlug].pinnacleAfterSpread += spread;
        gameStats[gameSlug].pinnacleAfterCount += 1;
      }
    });
    
    // Sort games by total volume (descending)
    const sortedGames = Object.keys(gameStats).sort((a, b) => 
      gameStats[b].totalVolume - gameStats[a].totalVolume
    );
    
    const container = document.getElementById('game-summaries-container');
    if (!container) {
      console.error('❌ Game summaries container not found');
      return;
    }
    
    if (sortedGames.length === 0) {
      container.innerHTML = '<div style="text-align: center; padding: 2rem; color: var(--text-secondary);">No game data available</div>';
      return;
    }
    
    let html = '<div style="display: grid; grid-template-columns: repeat(auto-fill, minmax(500px, 1fr)); gap: 1.5rem; width: 100%;">';
    
    // Process each game and enrich takers
    for (const gameSlug of sortedGames) {
      const stats = gameStats[gameSlug];
      const league = extractLeague(gameSlug);
      
      // Calculate averages
      const avgBeforePct = stats.totalVolume > 0 && stats.beforeCount > 0 
        ? (stats.beforeSpread / stats.totalVolume) * 100 : null;
      const avgAfterPct = stats.totalVolume > 0 && stats.afterCount > 0 
        ? (stats.afterSpread / stats.totalVolume) * 100 : null;
      const avgSportbookPct = stats.totalVolume > 0 && stats.sportbookCount > 0 
        ? (stats.sportbookSpread / stats.totalVolume) * 100 : null;
      const avgPinnacleBeforePct = stats.totalVolume > 0 && stats.pinnacleBeforeCount > 0
        ? (stats.pinnacleBeforeSpread / stats.totalVolume) * 100 : null;
      const avgPinnacleAfterPct = stats.totalVolume > 0 && stats.pinnacleAfterCount > 0
        ? (stats.pinnacleAfterSpread / stats.totalVolume) * 100 : null;
      
      // Get top 3 takers by volume
      const takersArray = Object.entries(stats.takers)
        .map(([address, takerData]) => ({
          address,
          ...takerData,
          avgAfterPct: takerData.afterPercentages.length > 0
            ? takerData.afterPercentages.reduce((a, b) => a + b, 0) / takerData.afterPercentages.length
            : null
        }))
        .sort((a, b) => b.volume - a.volume)
        .slice(0, 3);
      
      // Enrich takers with Polymarket profile info
      let enrichedTakers = takersArray;
      if (takersArray.length > 0) {
        try {
          const addresses = takersArray.map(t => t.address).join(',');
          const response = await fetch(`${BACKEND_URL}/api/takers/enrich?addresses=${encodeURIComponent(addresses)}`);
          if (response.ok) {
            const data = await response.json();
            const enrichedMap = new Map(data.takers.map(t => [t.address.toLowerCase(), t.polymarket]));
            enrichedTakers = takersArray.map(taker => ({
              ...taker,
              polymarket: enrichedMap.get(taker.address.toLowerCase()) || null
            }));
          }
        } catch (error) {
          console.error('Error enriching takers:', error);
        }
      }
      
      // Group orders by token_label (side/team)
      const teamStats = {};
      stats.orders.forEach(order => {
        const team = order.token_label || 'Unknown';
        if (!teamStats[team]) {
          teamStats[team] = {
            volume: 0,
            fills: 0,
            prices: [],
            beforeSpreads: [],
            afterSpreads: [],
            sportbookSpreads: [],
            pinnacleBeforeSpreads: [],
            pinnacleAfterSpreads: [],
            maxFill: 0
          };
        }
        const fillValue = (order.price || 0) * (order.shares_normalized || 0);
        teamStats[team].volume += fillValue;
        teamStats[team].fills += 1;
        teamStats[team].prices.push(order.price || 0);
        teamStats[team].maxFill = Math.max(teamStats[team].maxFill, fillValue);
        
        if (order.polymarket_before && order.polymarket_before.bbo !== undefined && order.polymarket_before.bbo !== null) {
          const spreadPct = ((order.price - order.polymarket_before.bbo) / order.price) * 100;
          teamStats[team].beforeSpreads.push(spreadPct);
        }
        if (order.polymarket_after && order.polymarket_after.bbo !== undefined && order.polymarket_after.bbo !== null) {
          const spreadPct = ((order.price - order.polymarket_after.bbo) / order.price) * 100;
          teamStats[team].afterSpreads.push(spreadPct);
        }
        if (order.sportbook && order.sportbook.best_bid !== undefined && order.sportbook.best_bid !== null) {
          const spreadPct = ((order.price - order.sportbook.best_bid) / order.price) * 100;
          teamStats[team].sportbookSpreads.push(spreadPct);
        }
        if (order.pinnacle_before && order.pinnacle_before.bbo !== undefined && order.pinnacle_before.bbo !== null) {
          const spreadPct = ((order.price - order.pinnacle_before.bbo) / order.price) * 100;
          teamStats[team].pinnacleBeforeSpreads.push(spreadPct);
        }
        if (order.pinnacle_after && order.pinnacle_after.bbo !== undefined && order.pinnacle_after.bbo !== null) {
          const spreadPct = ((order.price - order.pinnacle_after.bbo) / order.price) * 100;
          teamStats[team].pinnacleAfterSpreads.push(spreadPct);
        }
      });
      
      const teams = Object.entries(teamStats).sort((a, b) => b[1].volume - a[1].volume);
      
      html += `
        <div style="background: white; border-radius: 8px; padding: 1.25rem; box-shadow: 0 2px 4px rgba(0,0,0,0.08); border: 1px solid #e5e7eb; margin-bottom: 1.5rem; width: 100%;">
          <!-- Header -->
          <div style="display: flex; align-items: center; gap: 0.5rem; margin-bottom: 0.75rem;">
            <span style="background: #f3f4f6; padding: 0.25rem 0.625rem; border-radius: 4px; font-size: 0.8125rem; font-weight: 600; color: #374151; text-transform: uppercase; letter-spacing: 0.025em;">${league}</span>
            <h3 style="font-size: 0.9375rem; font-weight: 600; color: #111827; margin: 0; line-height: 1.4;">${gameSlug}</h3>
          </div>
          
          <!-- Condition ID -->
          <div style="font-size: 0.75rem; color: #6b7280; margin-bottom: 1rem; font-family: 'Monaco', 'Menlo', 'Courier New', monospace; word-break: break-all;">
            ${stats.orders[0]?.condition_id || '-'}
          </div>
          
          <!-- Overall Stats - Cleaner Grid -->
          <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 0.75rem; margin-bottom: 1.25rem;">
            <div style="padding: 0.75rem; background: #f9fafb; border-radius: 6px; border: 1px solid #e5e7eb;">
              <div style="font-size: 0.6875rem; color: #6b7280; margin-bottom: 0.375rem; font-weight: 500; text-transform: uppercase; letter-spacing: 0.025em;">Total Volume</div>
              <div style="font-size: 1rem; font-weight: 700; color: #111827;">$${stats.totalVolume.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})}</div>
            </div>
            <div style="padding: 0.75rem; background: #f9fafb; border-radius: 6px; border: 1px solid #e5e7eb;">
              <div style="font-size: 0.6875rem; color: #6b7280; margin-bottom: 0.375rem; font-weight: 500; text-transform: uppercase; letter-spacing: 0.025em;">Avg Spread Before (PM)</div>
              <div style="font-size: 1rem; font-weight: 700; color: ${avgBeforePct !== null && avgBeforePct < 0 ? '#10b981' : avgBeforePct !== null ? '#ef4444' : '#9ca3af'};">
                ${avgBeforePct !== null ? `${avgBeforePct >= 0 ? '+' : ''}${avgBeforePct.toFixed(2)}%` : 'N/A'}
              </div>
            </div>
            <div style="padding: 0.75rem; background: #f9fafb; border-radius: 6px; border: 1px solid #e5e7eb;">
              <div style="font-size: 0.6875rem; color: #6b7280; margin-bottom: 0.375rem; font-weight: 500; text-transform: uppercase; letter-spacing: 0.025em;">Avg Spread After (PM)</div>
              <div style="font-size: 1rem; font-weight: 700; color: ${avgAfterPct !== null && avgAfterPct < 0 ? '#10b981' : avgAfterPct !== null ? '#ef4444' : '#9ca3af'};">
                ${avgAfterPct !== null ? `${avgAfterPct >= 0 ? '+' : ''}${avgAfterPct.toFixed(2)}%` : 'N/A'}
              </div>
            </div>
            <div style="padding: 0.75rem; background: #f9fafb; border-radius: 6px; border: 1px solid #e5e7eb;">
              <div style="font-size: 0.6875rem; color: #6b7280; margin-bottom: 0.375rem; font-weight: 500; text-transform: uppercase; letter-spacing: 0.025em;">Avg Sportsbook (PM)</div>
              <div style="font-size: 1rem; font-weight: 700; color: ${avgSportbookPct !== null && avgSportbookPct < 0 ? '#10b981' : avgSportbookPct !== null ? '#ef4444' : '#9ca3af'};">
                ${avgSportbookPct !== null ? `${avgSportbookPct >= 0 ? '+' : ''}${avgSportbookPct.toFixed(2)}%` : 'N/A'}
              </div>
            </div>
            <div style="padding: 0.75rem; background: #f9fafb; border-radius: 6px; border: 1px solid #e5e7eb;">
              <div style="font-size: 0.6875rem; color: #6b7280; margin-bottom: 0.375rem; font-weight: 500; text-transform: uppercase; letter-spacing: 0.025em;">Avg Spread Before (PN)</div>
              <div style="font-size: 1rem; font-weight: 700; color: ${avgPinnacleBeforePct !== null && avgPinnacleBeforePct < 0 ? '#10b981' : avgPinnacleBeforePct !== null ? '#ef4444' : '#9ca3af'};">
                ${avgPinnacleBeforePct !== null ? `${avgPinnacleBeforePct >= 0 ? '+' : ''}${avgPinnacleBeforePct.toFixed(2)}%` : 'N/A'}
              </div>
            </div>
            <div style="padding: 0.75rem; background: #f9fafb; border-radius: 6px; border: 1px solid #e5e7eb;">
              <div style="font-size: 0.6875rem; color: #6b7280; margin-bottom: 0.375rem; font-weight: 500; text-transform: uppercase; letter-spacing: 0.025em;">Avg Spread After (PN)</div>
              <div style="font-size: 1rem; font-weight: 700; color: ${avgPinnacleAfterPct !== null && avgPinnacleAfterPct < 0 ? '#10b981' : avgPinnacleAfterPct !== null ? '#ef4444' : '#9ca3af'};">
                ${avgPinnacleAfterPct !== null ? `${avgPinnacleAfterPct >= 0 ? '+' : ''}${avgPinnacleAfterPct.toFixed(2)}%` : 'N/A'}
              </div>
            </div>
          </div>
          
          <!-- Team Stats - Cleaner Layout -->
          <div style="display: grid; grid-template-columns: repeat(${teams.length}, 1fr); gap: 0.75rem; margin-bottom: 1.25rem;">
            ${teams.map(([team, teamData], idx) => {
              const avgPrice = teamData.prices.length > 0 
                ? teamData.prices.reduce((a, b) => a + b, 0) / teamData.prices.length 
                : 0;
              const avgBefore = teamData.beforeSpreads.length > 0
                ? teamData.beforeSpreads.reduce((a, b) => a + b, 0) / teamData.beforeSpreads.length
                : null;
              const avgAfter = teamData.afterSpreads.length > 0
                ? teamData.afterSpreads.reduce((a, b) => a + b, 0) / teamData.afterSpreads.length
                : null;
              const avgSportbook = teamData.sportbookSpreads.length > 0
                ? teamData.sportbookSpreads.reduce((a, b) => a + b, 0) / teamData.sportbookSpreads.length
                : null;
              const avgPinnacleBefore = teamData.pinnacleBeforeSpreads.length > 0
                ? teamData.pinnacleBeforeSpreads.reduce((a, b) => a + b, 0) / teamData.pinnacleBeforeSpreads.length
                : null;
              const avgPinnacleAfter = teamData.pinnacleAfterSpreads.length > 0
                ? teamData.pinnacleAfterSpreads.reduce((a, b) => a + b, 0) / teamData.pinnacleAfterSpreads.length
                : null;
              
              return `
                <div style="background: ${idx === 0 ? '#fef3c7' : '#ffffff'}; border-radius: 6px; padding: 1rem; border: 1px solid #e5e7eb; box-shadow: ${idx === 0 ? '0 1px 2px rgba(0,0,0,0.05)' : 'none'};">
                  <div style="font-weight: 600; color: #111827; margin-bottom: 0.875rem; font-size: 0.875rem; line-height: 1.3;">${team}</div>
                  <div style="display: flex; flex-direction: column; gap: 0.5rem;">
                    <div style="font-size: 0.75rem; color: #6b7280;">
                      <span style="font-weight: 500;">Fill Volume:</span> 
                      <span style="font-weight: 600; color: #111827; margin-left: 0.25rem;">$${teamData.volume.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})}</span>
                    </div>
                    <div style="font-size: 0.75rem; color: #6b7280;">
                      <span style="font-weight: 500;">Avg Price:</span> 
                      <span style="font-weight: 600; color: #111827; margin-left: 0.25rem;">$${avgPrice.toFixed(3)}</span>
                    </div>
                    <div style="font-size: 0.75rem; color: #6b7280;">
                      <span style="font-weight: 500;">Spread Before:</span> 
                      <span style="font-weight: 600; color: ${avgBefore !== null && avgBefore < 0 ? '#10b981' : avgBefore !== null ? '#ef4444' : '#9ca3af'}; margin-left: 0.25rem;">${avgBefore !== null ? `${avgBefore >= 0 ? '+' : ''}${avgBefore.toFixed(2)}%` : 'N/A'}</span>
                    </div>
                    <div style="font-size: 0.75rem; color: #6b7280;">
                      <span style="font-weight: 500;">Spread After:</span> 
                      <span style="font-weight: 600; color: ${avgAfter !== null && avgAfter < 0 ? '#10b981' : avgAfter !== null ? '#ef4444' : '#9ca3af'}; margin-left: 0.25rem;">${avgAfter !== null ? `${avgAfter >= 0 ? '+' : ''}${avgAfter.toFixed(2)}%` : 'N/A'}</span>
                    </div>
                    <div style="font-size: 0.75rem; color: #6b7280;">
                      <span style="font-weight: 500;">Sportsbook:</span> 
                      <span style="font-weight: 600; color: ${avgSportbook !== null && avgSportbook < 0 ? '#10b981' : avgSportbook !== null ? '#ef4444' : '#9ca3af'}; margin-left: 0.25rem;">${avgSportbook !== null ? `${avgSportbook >= 0 ? '+' : ''}${avgSportbook.toFixed(2)}%` : 'N/A'}</span>
                    </div>
                    <div style="font-size: 0.75rem; color: #6b7280;">
                      <span style="font-weight: 500;">Pinnacle Before:</span> 
                      <span style="font-weight: 600; color: ${avgPinnacleBefore !== null && avgPinnacleBefore < 0 ? '#10b981' : avgPinnacleBefore !== null ? '#ef4444' : '#9ca3af'}; margin-left: 0.25rem;">
                        ${avgPinnacleBefore !== null ? `${avgPinnacleBefore >= 0 ? '+' : ''}${avgPinnacleBefore.toFixed(2)}%` : 'N/A'}
                      </span>
                    </div>
                    <div style="font-size: 0.75rem; color: #6b7280;">
                      <span style="font-weight: 500;">Pinnacle After:</span> 
                      <span style="font-weight: 600; color: ${avgPinnacleAfter !== null && avgPinnacleAfter < 0 ? '#10b981' : avgPinnacleAfter !== null ? '#ef4444' : '#9ca3af'}; margin-left: 0.25rem;">
                        ${avgPinnacleAfter !== null ? `${avgPinnacleAfter >= 0 ? '+' : ''}${avgPinnacleAfter.toFixed(2)}%` : 'N/A'}
                      </span>
                    </div>
                    <div style="font-size: 0.75rem; color: #6b7280;">
                      <span style="font-weight: 500;">Fill Count:</span> 
                      <span style="font-weight: 600; color: #111827; margin-left: 0.25rem;">${teamData.fills}</span>
                    </div>
                    <div style="font-size: 0.75rem; color: #6b7280;">
                      <span style="font-weight: 500;">Max Fill:</span> 
                      <span style="font-weight: 600; color: #111827; margin-left: 0.25rem;">$${teamData.maxFill.toFixed(2)}</span>
                    </div>
                  </div>
                </div>
              `;
            }).join('')}
          </div>
          
          <!-- Top 3 Takers - Cleaner Design -->
          ${enrichedTakers.length > 0 ? `
            <div style="margin-top: 1.25rem; padding-top: 1.25rem; border-top: 1px solid #e5e7eb;">
              <div style="font-weight: 600; color: #111827; margin-bottom: 0.875rem; font-size: 0.8125rem; text-transform: uppercase; letter-spacing: 0.025em;">Top 3 Takers by Volume</div>
              <div style="display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 0.75rem;">
                ${enrichedTakers.map((taker, idx) => {
                  const polymarketUrl = `https://polymarket.com/${taker.address}`;
                  const pm = taker.polymarket || {};
                  const displayName = pm.name || pm.pseudonym || null;
                  const pseudonym = pm.pseudonym && pm.name ? pm.pseudonym : null;
                  const verified = pm.verified_badge;
                  
                  return `
                    <div style="background: #f9fafb; border-radius: 6px; padding: 0.875rem; border: 1px solid #e5e7eb; min-width: 0; overflow: hidden;">
                      <div style="font-weight: 600; color: #111827; margin-bottom: 0.625rem; font-size: 0.8125rem; line-height: 1.35; min-width: 0; overflow-wrap: anywhere; word-break: break-word;">
                        #${idx + 1} ${displayName ? `
                          <a href="${polymarketUrl}" target="_blank" rel="noopener noreferrer" 
                             style="color: #3b82f6; text-decoration: none; font-weight: 600; display: inline-block; max-width: 100%; overflow-wrap: anywhere; word-break: break-word;"
                             onmouseover="this.style.textDecoration='underline'" 
                             onmouseout="this.style.textDecoration='none'">
                            ${displayName}
                          </a>
                          ${verified ? '<span style="color: #10b981; font-size: 0.75rem; margin-left: 0.25rem;">✓</span>' : ''}
                        ` : `
                          <a href="${polymarketUrl}" target="_blank" rel="noopener noreferrer" 
                             style="color: #3b82f6; text-decoration: none; font-family: monospace; display: inline-block; max-width: 100%; overflow-wrap: anywhere; word-break: break-all;"
                             onmouseover="this.style.textDecoration='underline'" 
                             onmouseout="this.style.textDecoration='none'">${formatAddress(taker.address)}</a>
                        `}
                      </div>
                      ${pseudonym ? `
                        <div style="font-size: 0.75rem; color: #6b7280; margin-bottom: 0.375rem; overflow-wrap: anywhere; word-break: break-word;">
                          @${pseudonym}
                        </div>
                      ` : ''}
                      ${!displayName ? `
                        <div style="font-size: 0.7rem; color: #9ca3af; font-family: monospace; margin-bottom: 0.375rem;">
                          ${formatAddress(taker.address)}
                        </div>
                      ` : ''}
                      <div style="display: flex; flex-direction: column; gap: 0.375rem;">
                        <div style="font-size: 0.75rem; color: #6b7280;">
                          <span style="font-weight: 500;">Volume:</span> 
                          <span style="font-weight: 600; color: #111827; margin-left: 0.25rem;">$${taker.volume.toFixed(2)}</span>
                        </div>
                        <div style="font-size: 0.75rem; color: #6b7280;">
                          <span style="font-weight: 500;">Fills:</span> 
                          <span style="font-weight: 600; color: #111827; margin-left: 0.25rem;">${taker.fills}</span>
                        </div>
                        <div style="font-size: 0.75rem; color: #6b7280;">
                          <span style="font-weight: 500;">Poly After Avg:</span> 
                          <span style="font-weight: 600; color: ${taker.avgAfterPct !== null && taker.avgAfterPct < 0 ? '#10b981' : taker.avgAfterPct !== null ? '#ef4444' : '#111827'}; margin-left: 0.25rem;">
                            ${taker.avgAfterPct !== null ? `${taker.avgAfterPct >= 0 ? '+' : ''}${taker.avgAfterPct.toFixed(2)}%` : 'N/A'}
                          </span>
                        </div>
                      </div>
                    </div>
                  `;
                }).join('')}
              </div>
            </div>
          ` : ''}
        </div>
      `;
    }
    
    html += '</div>';
    container.innerHTML = html;
  } catch (error) {
    console.error('❌ Error loading game summaries:', error);
    const container = document.getElementById('game-summaries-container');
    if (container) {
      container.innerHTML = `<div style="text-align: center; padding: 2rem; color: var(--accent-red);">Error loading game summaries</div>`;
    }
  }
}

// Calculate and display PM summary
function loadPMSummary() {
  try {
    const orders = getFilteredOrders();
    
    // Group by league and calculate stats
    const leagueStats = {};
    let totalFillValue = 0;
    let totalBeforeSpread = 0;
    let totalBeforeFillValue = 0;
    let totalAfterSpread = 0;
    let totalAfterFillValue = 0;
    let totalSportbookSpread = 0;
    let totalSportbookFillValue = 0;
    let totalSportbookPointPctWeighted = 0;
    
    orders.forEach(order => {
      const league = extractLeague(order.market_slug);
      if (league === '-') return;
      
      const fillValue = (order.price || 0) * (order.shares_normalized || 0);
      totalFillValue += fillValue;
      
      if (!leagueStats[league]) {
        leagueStats[league] = {
          fillValue: 0,
          fills: 0,
          beforeSpread: 0,
          beforeFillValue: 0,
          afterSpread: 0,
          afterFillValue: 0,
          sportbookSpread: 0,
          sportbookFillValue: 0,
          sportbookPointPctWeighted: 0
        };
      }
      
      leagueStats[league].fillValue += fillValue;
      leagueStats[league].fills += 1;
      
      // PM Before spread
      if (order.polymarket_before && order.polymarket_before.bbo !== undefined && order.polymarket_before.bbo !== null) {
        const spread = (order.price - order.polymarket_before.bbo) * (order.shares_normalized || 0);
        leagueStats[league].beforeSpread += spread;
        leagueStats[league].beforeFillValue += fillValue;
        totalBeforeSpread += spread;
        totalBeforeFillValue += fillValue;
      }
      
      // PM After spread
      if (order.polymarket_after && order.polymarket_after.bbo !== undefined && order.polymarket_after.bbo !== null) {
        const spread = (order.price - order.polymarket_after.bbo) * (order.shares_normalized || 0);
        leagueStats[league].afterSpread += spread;
        leagueStats[league].afterFillValue += fillValue;
        totalAfterSpread += spread;
        totalAfterFillValue += fillValue;
      }
      
      // PM Sportbook spread
      if (order.sportbook && order.sportbook.best_bid !== undefined && order.sportbook.best_bid !== null) {
        const spread = (order.price - order.sportbook.best_bid) * (order.shares_normalized || 0);
        const pointPct = (order.price - order.sportbook.best_bid) * 100;
        leagueStats[league].sportbookSpread += spread;
        leagueStats[league].sportbookFillValue += fillValue;
        leagueStats[league].sportbookPointPctWeighted += (pointPct * fillValue);
        totalSportbookSpread += spread;
        totalSportbookFillValue += fillValue;
        totalSportbookPointPctWeighted += (pointPct * fillValue);
      }
    });
    
    // Sort leagues by fill value (descending)
    const sortedLeagues = Object.keys(leagueStats).sort((a, b) => 
      leagueStats[b].fillValue - leagueStats[a].fillValue
    );
    
    const container = document.getElementById('pm-summary-container');
    if (!container) {
      console.error('❌ PM summary container not found');
      return;
    }
    
    const beforePct = totalBeforeFillValue > 0 ? (totalBeforeSpread / totalBeforeFillValue) * 100 : 0;
    const afterPct = totalAfterFillValue > 0 ? (totalAfterSpread / totalAfterFillValue) * 100 : 0;
    const sportbookRoiPct = totalSportbookFillValue > 0 ? (totalSportbookSpread / totalSportbookFillValue) * 100 : 0;
    const sportbookPointPct = totalSportbookFillValue > 0 ? (totalSportbookPointPctWeighted / totalSportbookFillValue) : 0;
    
    const beforeColor = totalBeforeSpread < 0 ? 'var(--accent-green)' : 'var(--accent-red)';
    const afterColor = totalAfterSpread < 0 ? 'var(--accent-green)' : 'var(--accent-red)';
    const sportbookColor = sportbookRoiPct < 0 ? 'var(--accent-green)' : sportbookRoiPct > 0 ? 'var(--accent-red)' : 'var(--text-primary)';
    
    let html = `
      <div style="margin-bottom: 2rem;">
        <div style="text-align: center; margin-bottom: 1.5rem;">
          <div style="font-size: 0.875rem; color: var(--text-secondary); margin-bottom: 0.5rem;">$ TOTAL FILL VALUE</div>
          <div style="font-size: 2.5rem; font-weight: 700; color: var(--accent-green);">$${totalFillValue.toLocaleString(undefined, {minimumFractionDigits: 0, maximumFractionDigits: 0})}</div>
        </div>
        
        <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 1.5rem;">
          <!-- PM Before Panel -->
          <div style="background: white; border-radius: 12px; padding: 1.5rem; box-shadow: 0 1px 3px rgba(0,0,0,0.1); border: 1px solid var(--border-color);">
            <div style="margin-bottom: 1rem;">
              <button style="background: var(--accent-blue); color: white; border: none; padding: 0.5rem 1rem; border-radius: 6px; font-weight: 600; font-size: 0.875rem; cursor: default;">POLYMARKET BBO</button>
            </div>
            <div style="text-align: center; margin-bottom: 1.5rem; padding-bottom: 1rem; border-bottom: 2px solid var(--border-color);">
              <div style="font-size: 1.5rem; font-weight: 700; color: ${beforeColor}; margin-bottom: 0.25rem;">
                ${totalBeforeSpread >= 0 ? '+' : ''}$${Math.abs(totalBeforeSpread).toLocaleString(undefined, {minimumFractionDigits: 0, maximumFractionDigits: 0})}
              </div>
              <div style="font-size: 0.875rem; color: ${beforeColor};">
                ${beforePct >= 0 ? '+' : ''}${beforePct.toFixed(2)}%
              </div>
            </div>
            <div style="font-size: 0.75rem; font-weight: 600; color: var(--text-secondary); margin-bottom: 0.75rem; text-transform: uppercase; letter-spacing: 0.05em;">TOTAL FILL VALUE x POLYMARKET SPREAD BEFORE</div>
            <div style="space-y: 0.5rem;">
              ${sortedLeagues.map(league => {
                const stats = leagueStats[league];
                const leaguePct = stats.beforeFillValue > 0 ? (stats.beforeSpread / stats.beforeFillValue) * 100 : 0;
                const leagueColor = stats.beforeSpread < 0 ? 'var(--accent-green)' : 'var(--accent-red)';
                return `
                  <div style="display: flex; justify-content: space-between; align-items: center; padding: 0.5rem 0; border-bottom: 1px solid var(--border-color);">
                    <div style="display: flex; align-items: center; gap: 0.75rem;">
                      <span style="background: #f1f3f5; padding: 0.25rem 0.5rem; border-radius: 4px; font-size: 0.75rem; font-weight: 600; color: var(--text-primary);">${league}</span>
                      <span style="font-size: 0.875rem; color: var(--text-primary);">$${stats.fillValue.toLocaleString(undefined, {minimumFractionDigits: 0, maximumFractionDigits: 0})} • ${stats.fills} fills</span>
                    </div>
                    <div style="text-align: right;">
                      <div style="font-size: 0.875rem; font-weight: 600; color: ${leagueColor};">
                        ${stats.beforeSpread >= 0 ? '+' : ''}$${Math.abs(stats.beforeSpread).toLocaleString(undefined, {minimumFractionDigits: 0, maximumFractionDigits: 0})}
                      </div>
                      <div style="font-size: 0.75rem; color: ${leagueColor};">
                        (${leaguePct >= 0 ? '+' : ''}${leaguePct.toFixed(2)}%)
                      </div>
                    </div>
                  </div>
                `;
              }).join('')}
            </div>
          </div>
          
          <!-- PM After Panel -->
          <div style="background: white; border-radius: 12px; padding: 1.5rem; box-shadow: 0 1px 3px rgba(0,0,0,0.1); border: 1px solid var(--border-color);">
            <div style="margin-bottom: 1rem;">
              <button style="background: var(--accent-blue); color: white; border: none; padding: 0.5rem 1rem; border-radius: 6px; font-weight: 600; font-size: 0.875rem; cursor: default;">POLYMARKET BBO</button>
            </div>
            <div style="text-align: center; margin-bottom: 1.5rem; padding-bottom: 1rem; border-bottom: 2px solid var(--border-color);">
              <div style="font-size: 1.5rem; font-weight: 700; color: ${afterColor}; margin-bottom: 0.25rem;">
                ${totalAfterSpread >= 0 ? '+' : ''}$${Math.abs(totalAfterSpread).toLocaleString(undefined, {minimumFractionDigits: 0, maximumFractionDigits: 0})}
              </div>
              <div style="font-size: 0.875rem; color: ${afterColor};">
                ${afterPct >= 0 ? '+' : ''}${afterPct.toFixed(2)}%
              </div>
            </div>
            <div style="font-size: 0.75rem; font-weight: 600; color: var(--text-secondary); margin-bottom: 0.75rem; text-transform: uppercase; letter-spacing: 0.05em;">TOTAL FILL VALUE x POLYMARKET SPREAD AFTER</div>
            <div style="space-y: 0.5rem;">
              ${sortedLeagues.map(league => {
                const stats = leagueStats[league];
                const leaguePct = stats.afterFillValue > 0 ? (stats.afterSpread / stats.afterFillValue) * 100 : 0;
                const leagueColor = stats.afterSpread < 0 ? 'var(--accent-green)' : 'var(--accent-red)';
                return `
                  <div style="display: flex; justify-content: space-between; align-items: center; padding: 0.5rem 0; border-bottom: 1px solid var(--border-color);">
                    <div style="display: flex; align-items: center; gap: 0.75rem;">
                      <span style="background: #f1f3f5; padding: 0.25rem 0.5rem; border-radius: 4px; font-size: 0.75rem; font-weight: 600; color: var(--text-primary);">${league}</span>
                      <span style="font-size: 0.875rem; color: var(--text-primary);">$${stats.fillValue.toLocaleString(undefined, {minimumFractionDigits: 0, maximumFractionDigits: 0})} • ${stats.fills} fills</span>
                    </div>
                    <div style="text-align: right;">
                      <div style="font-size: 0.875rem; font-weight: 600; color: ${leagueColor};">
                        ${stats.afterSpread >= 0 ? '+' : ''}$${Math.abs(stats.afterSpread).toLocaleString(undefined, {minimumFractionDigits: 0, maximumFractionDigits: 0})}
                      </div>
                      <div style="font-size: 0.75rem; color: ${leagueColor};">
                        (${leaguePct >= 0 ? '+' : ''}${leaguePct.toFixed(2)}%)
                      </div>
                    </div>
                  </div>
                `;
              }).join('')}
            </div>
          </div>
          
          <!-- PM Sportbook Panel -->
          <div style="background: white; border-radius: 12px; padding: 1.5rem; box-shadow: 0 1px 3px rgba(0,0,0,0.1); border: 1px solid var(--border-color);">
            <div style="margin-bottom: 1rem;">
              <button style="background: var(--accent-blue); color: white; border: none; padding: 0.5rem 1rem; border-radius: 6px; font-weight: 600; font-size: 0.875rem; cursor: default;">POLYMARKET BBO</button>
            </div>
            <div style="text-align: center; margin-bottom: 1.5rem; padding-bottom: 1rem; border-bottom: 2px solid var(--border-color);">
              <div style="font-size: 1.5rem; font-weight: 700; color: ${sportbookColor}; margin-bottom: 0.25rem;">
                ${totalSportbookSpread >= 0 ? '+' : ''}$${Math.abs(totalSportbookSpread).toLocaleString(undefined, {minimumFractionDigits: 0, maximumFractionDigits: 0})}
              </div>
              <div style="font-size: 0.875rem; color: ${sportbookColor};">
                ROI: ${sportbookRoiPct >= 0 ? '+' : ''}${sportbookRoiPct.toFixed(2)}%
              </div>
              <div style="font-size: 0.75rem; color: ${sportbookColor}; margin-top: 0.2rem;">
                Point: ${sportbookPointPct >= 0 ? '+' : ''}${sportbookPointPct.toFixed(2)}%
              </div>
            </div>
            <div style="font-size: 0.75rem; font-weight: 600; color: var(--text-secondary); margin-bottom: 0.75rem; text-transform: uppercase; letter-spacing: 0.05em;">TOTAL FILL VALUE x POLYMARKET SPORTSBOOK</div>
            <div style="space-y: 0.5rem;">
              ${sortedLeagues.map(league => {
                const stats = leagueStats[league];
                const leagueRoiPct = stats.sportbookFillValue > 0 ? (stats.sportbookSpread / stats.sportbookFillValue) * 100 : 0;
                const leaguePointPct = stats.sportbookFillValue > 0 ? (stats.sportbookPointPctWeighted / stats.sportbookFillValue) : 0;
                const leagueColor = leagueRoiPct < 0 ? 'var(--accent-green)' : leagueRoiPct > 0 ? 'var(--accent-red)' : 'var(--text-primary)';
                return `
                  <div style="display: flex; justify-content: space-between; align-items: center; padding: 0.5rem 0; border-bottom: 1px solid var(--border-color);">
                    <div style="display: flex; align-items: center; gap: 0.75rem;">
                      <span style="background: #f1f3f5; padding: 0.25rem 0.5rem; border-radius: 4px; font-size: 0.75rem; font-weight: 600; color: var(--text-primary);">${league}</span>
                      <span style="font-size: 0.875rem; color: var(--text-primary);">$${stats.fillValue.toLocaleString(undefined, {minimumFractionDigits: 0, maximumFractionDigits: 0})} • ${stats.fills} fills</span>
                    </div>
                    <div style="text-align: right;">
                      <div style="font-size: 0.875rem; font-weight: 600; color: ${leagueColor};">
                        ${stats.sportbookSpread >= 0 ? '+' : ''}$${Math.abs(stats.sportbookSpread).toLocaleString(undefined, {minimumFractionDigits: 0, maximumFractionDigits: 0})}
                      </div>
                      <div style="font-size: 0.75rem; color: ${leagueColor};">
                        (ROI ${leagueRoiPct >= 0 ? '+' : ''}${leagueRoiPct.toFixed(2)}% | Point ${leaguePointPct >= 0 ? '+' : ''}${leaguePointPct.toFixed(2)}%)
                      </div>
                    </div>
                  </div>
                `;
              }).join('')}
            </div>
          </div>
        </div>
      </div>
    `;
    
    container.innerHTML = html;
  } catch (error) {
    console.error('❌ Error loading PM summary:', error);
    const container = document.getElementById('pm-summary-container');
    if (container) {
      container.innerHTML = `<div style="text-align: center; padding: 2rem; color: var(--accent-red);">Error loading PM summary</div>`;
    }
  }
}

// Calculate and display Pinnacle summary
function loadPinnacleSummary() {
  try {
    const orders = getFilteredOrders();

    const leagueStats = {};
    let totalFillValue = 0;
    let totalBeforeSpread = 0;
    let totalBeforeFillValue = 0;
    let totalAfterSpread = 0;
    let totalAfterFillValue = 0;
    let totalSportbookSpread = 0;
    let totalSportbookFillValue = 0;
    let totalSportbookPointPctWeighted = 0;

    orders.forEach(order => {
      const league = extractLeague(order.market_slug);
      if (league === '-') return;

      const fillPrice = Number(order.price);
      const shares = Number(order.shares_normalized || 0);
      if (!Number.isFinite(fillPrice)) return;

      const fillValue = fillPrice * shares;
      totalFillValue += fillValue;

      if (!leagueStats[league]) {
        leagueStats[league] = {
          fillValue: 0,
          fills: 0,
          beforeSpread: 0,
          beforeFillValue: 0,
          afterSpread: 0,
          afterFillValue: 0,
          sportbookSpread: 0,
          sportbookFillValue: 0,
          sportbookPointPctWeighted: 0
        };
      }

      leagueStats[league].fillValue += fillValue;
      leagueStats[league].fills += 1;

      if (order.pinnacle_before && order.pinnacle_before.bbo !== undefined && order.pinnacle_before.bbo !== null) {
        const beforeBbo = Number(order.pinnacle_before.bbo);
        if (Number.isFinite(beforeBbo)) {
          const spread = (fillPrice - beforeBbo) * shares;
          leagueStats[league].beforeSpread += spread;
          leagueStats[league].beforeFillValue += fillValue;
          totalBeforeSpread += spread;
          totalBeforeFillValue += fillValue;
        }
      }

      if (order.pinnacle_after && order.pinnacle_after.bbo !== undefined && order.pinnacle_after.bbo !== null) {
        const afterBbo = Number(order.pinnacle_after.bbo);
        if (Number.isFinite(afterBbo)) {
          const spread = (fillPrice - afterBbo) * shares;
          leagueStats[league].afterSpread += spread;
          leagueStats[league].afterFillValue += fillValue;
          totalAfterSpread += spread;
          totalAfterFillValue += fillValue;
        }
      }

      // PN Sportbook formula:
      // (Fill Price - (1 - Opposite Percentage Odds)) * 100
      const oppDecimal = parsePercentageToDecimal(order?.pinnacle_after?.opponent_percentage);
      if (oppDecimal !== null) {
        const impliedFromOpp = 1 - oppDecimal;
        const sportbookPointPct = (fillPrice - impliedFromOpp) * 100;
        const sportbookSpread = (fillPrice - impliedFromOpp) * shares;
        leagueStats[league].sportbookSpread += sportbookSpread;
        leagueStats[league].sportbookFillValue += fillValue;
        leagueStats[league].sportbookPointPctWeighted += (sportbookPointPct * fillValue);
        totalSportbookSpread += sportbookSpread;
        totalSportbookFillValue += fillValue;
        totalSportbookPointPctWeighted += (sportbookPointPct * fillValue);
      } else if (order?.pinnacle_sportbook?.price_diff_pct !== undefined && order?.pinnacle_sportbook?.price_diff_pct !== null) {
        // Fallback for legacy rows missing opponent_percentage
        const sportbookPointPct = Number(order.pinnacle_sportbook.price_diff_pct);
        if (Number.isFinite(sportbookPointPct)) {
          const sportbookSpread = (sportbookPointPct / 100) * shares;
          leagueStats[league].sportbookSpread += sportbookSpread;
          leagueStats[league].sportbookFillValue += fillValue;
          leagueStats[league].sportbookPointPctWeighted += (sportbookPointPct * fillValue);
          totalSportbookSpread += sportbookSpread;
          totalSportbookFillValue += fillValue;
          totalSportbookPointPctWeighted += (sportbookPointPct * fillValue);
        }
      }
    });

    const sortedLeagues = Object.keys(leagueStats).sort((a, b) =>
      leagueStats[b].fillValue - leagueStats[a].fillValue
    );

    const container = document.getElementById('pn-summary-container');
    if (!container) return;

    const beforePct = totalBeforeFillValue > 0 ? (totalBeforeSpread / totalBeforeFillValue) * 100 : 0;
    const afterPct = totalAfterFillValue > 0 ? (totalAfterSpread / totalAfterFillValue) * 100 : 0;
    const sportbookRoiPct = totalSportbookFillValue > 0 ? (totalSportbookSpread / totalSportbookFillValue) * 100 : 0;
    const sportbookPointPct = totalSportbookFillValue > 0 ? (totalSportbookPointPctWeighted / totalSportbookFillValue) : 0;

    const beforeColor = totalBeforeSpread < 0 ? 'var(--accent-green)' : 'var(--accent-red)';
    const afterColor = totalAfterSpread < 0 ? 'var(--accent-green)' : 'var(--accent-red)';
    const sportbookColor = sportbookRoiPct < 0 ? 'var(--accent-green)' : sportbookRoiPct > 0 ? 'var(--accent-red)' : 'var(--text-primary)';

    let html = `
      <div style="margin-bottom: 2rem;">
        <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 1.5rem;">
          <div style="background: white; border-radius: 12px; padding: 1.5rem; box-shadow: 0 1px 3px rgba(0,0,0,0.1); border: 1px solid var(--border-color);">
            <div style="margin-bottom: 1rem;">
              <button style="background: #facc15; color: #111827; border: none; padding: 0.5rem 1rem; border-radius: 6px; font-weight: 600; font-size: 0.875rem; cursor: default;">PINNACLE DELTAS</button>
            </div>
            <div style="text-align: center; margin-bottom: 1.5rem; padding-bottom: 1rem; border-bottom: 2px solid var(--border-color);">
              <div style="font-size: 1.5rem; font-weight: 700; color: ${beforeColor}; margin-bottom: 0.25rem;">
                ${totalBeforeSpread >= 0 ? '+' : ''}$${Math.abs(totalBeforeSpread).toLocaleString(undefined, {minimumFractionDigits: 0, maximumFractionDigits: 0})}
              </div>
              <div style="font-size: 0.875rem; color: ${beforeColor};">
                ${beforePct >= 0 ? '+' : ''}${beforePct.toFixed(2)}%
              </div>
            </div>
            <div style="font-size: 0.75rem; font-weight: 600; color: var(--text-secondary); margin-bottom: 0.75rem; text-transform: uppercase; letter-spacing: 0.05em;">TOTAL FILL VALUE x PINNACLE SPREAD BEFORE</div>
            <div>
              ${sortedLeagues.map(league => {
                const stats = leagueStats[league];
                const leaguePct = stats.beforeFillValue > 0 ? (stats.beforeSpread / stats.beforeFillValue) * 100 : 0;
                const leagueColor = stats.beforeSpread < 0 ? 'var(--accent-green)' : 'var(--accent-red)';
                return `
                  <div style="display: flex; justify-content: space-between; align-items: center; padding: 0.5rem 0; border-bottom: 1px solid var(--border-color);">
                    <div style="display: flex; align-items: center; gap: 0.75rem;">
                      <span style="background: #f1f3f5; padding: 0.25rem 0.5rem; border-radius: 4px; font-size: 0.75rem; font-weight: 600; color: var(--text-primary);">${league}</span>
                      <span style="font-size: 0.875rem; color: var(--text-primary);">$${stats.fillValue.toLocaleString(undefined, {minimumFractionDigits: 0, maximumFractionDigits: 0})} • ${stats.fills} fills</span>
                    </div>
                    <div style="text-align: right;">
                      <div style="font-size: 0.875rem; font-weight: 600; color: ${leagueColor};">
                        ${stats.beforeSpread >= 0 ? '+' : ''}$${Math.abs(stats.beforeSpread).toLocaleString(undefined, {minimumFractionDigits: 0, maximumFractionDigits: 0})}
                      </div>
                      <div style="font-size: 0.75rem; color: ${leagueColor};">
                        (${leaguePct >= 0 ? '+' : ''}${leaguePct.toFixed(2)}%)
                      </div>
                    </div>
                  </div>
                `;
              }).join('')}
            </div>
          </div>

          <div style="background: white; border-radius: 12px; padding: 1.5rem; box-shadow: 0 1px 3px rgba(0,0,0,0.1); border: 1px solid var(--border-color);">
            <div style="margin-bottom: 1rem;">
              <button style="background: #facc15; color: #111827; border: none; padding: 0.5rem 1rem; border-radius: 6px; font-weight: 600; font-size: 0.875rem; cursor: default;">PINNACLE DELTAS</button>
            </div>
            <div style="text-align: center; margin-bottom: 1.5rem; padding-bottom: 1rem; border-bottom: 2px solid var(--border-color);">
              <div style="font-size: 1.5rem; font-weight: 700; color: ${afterColor}; margin-bottom: 0.25rem;">
                ${totalAfterSpread >= 0 ? '+' : ''}$${Math.abs(totalAfterSpread).toLocaleString(undefined, {minimumFractionDigits: 0, maximumFractionDigits: 0})}
              </div>
              <div style="font-size: 0.875rem; color: ${afterColor};">
                ${afterPct >= 0 ? '+' : ''}${afterPct.toFixed(2)}%
              </div>
            </div>
            <div style="font-size: 0.75rem; font-weight: 600; color: var(--text-secondary); margin-bottom: 0.75rem; text-transform: uppercase; letter-spacing: 0.05em;">TOTAL FILL VALUE x PINNACLE SPREAD AFTER</div>
            <div>
              ${sortedLeagues.map(league => {
                const stats = leagueStats[league];
                const leaguePct = stats.afterFillValue > 0 ? (stats.afterSpread / stats.afterFillValue) * 100 : 0;
                const leagueColor = stats.afterSpread < 0 ? 'var(--accent-green)' : 'var(--accent-red)';
                return `
                  <div style="display: flex; justify-content: space-between; align-items: center; padding: 0.5rem 0; border-bottom: 1px solid var(--border-color);">
                    <div style="display: flex; align-items: center; gap: 0.75rem;">
                      <span style="background: #f1f3f5; padding: 0.25rem 0.5rem; border-radius: 4px; font-size: 0.75rem; font-weight: 600; color: var(--text-primary);">${league}</span>
                      <span style="font-size: 0.875rem; color: var(--text-primary);">$${stats.fillValue.toLocaleString(undefined, {minimumFractionDigits: 0, maximumFractionDigits: 0})} • ${stats.fills} fills</span>
                    </div>
                    <div style="text-align: right;">
                      <div style="font-size: 0.875rem; font-weight: 600; color: ${leagueColor};">
                        ${stats.afterSpread >= 0 ? '+' : ''}$${Math.abs(stats.afterSpread).toLocaleString(undefined, {minimumFractionDigits: 0, maximumFractionDigits: 0})}
                      </div>
                      <div style="font-size: 0.75rem; color: ${leagueColor};">
                        (${leaguePct >= 0 ? '+' : ''}${leaguePct.toFixed(2)}%)
                      </div>
                    </div>
                  </div>
                `;
              }).join('')}
            </div>
          </div>

          <div style="background: white; border-radius: 12px; padding: 1.5rem; box-shadow: 0 1px 3px rgba(0,0,0,0.1); border: 1px solid var(--border-color);">
            <div style="margin-bottom: 1rem;">
              <button style="background: #facc15; color: #111827; border: none; padding: 0.5rem 1rem; border-radius: 6px; font-weight: 600; font-size: 0.875rem; cursor: default;">PINNACLE DELTAS</button>
            </div>
            <div style="text-align: center; margin-bottom: 1.5rem; padding-bottom: 1rem; border-bottom: 2px solid var(--border-color);">
              <div style="font-size: 1.5rem; font-weight: 700; color: ${sportbookColor}; margin-bottom: 0.25rem;">
                ${totalSportbookSpread >= 0 ? '+' : ''}$${Math.abs(totalSportbookSpread).toLocaleString(undefined, {minimumFractionDigits: 0, maximumFractionDigits: 0})}
              </div>
              <div style="font-size: 0.875rem; color: ${sportbookColor};">
                ROI: ${sportbookRoiPct >= 0 ? '+' : ''}${sportbookRoiPct.toFixed(2)}%
              </div>
              <div style="font-size: 0.75rem; color: ${sportbookColor}; margin-top: 0.2rem;">
                Point: ${sportbookPointPct >= 0 ? '+' : ''}${sportbookPointPct.toFixed(2)}%
              </div>
            </div>
            <div style="font-size: 0.75rem; font-weight: 600; color: var(--text-secondary); margin-bottom: 0.75rem; text-transform: uppercase; letter-spacing: 0.05em;">TOTAL FILL VALUE x PINNACLE SPORTSBOOK</div>
            <div>
              ${sortedLeagues.map(league => {
                const stats = leagueStats[league];
                const leagueRoiPct = stats.sportbookFillValue > 0 ? (stats.sportbookSpread / stats.sportbookFillValue) * 100 : 0;
                const leaguePointPct = stats.sportbookFillValue > 0 ? (stats.sportbookPointPctWeighted / stats.sportbookFillValue) : 0;
                const leagueColor = leagueRoiPct < 0 ? 'var(--accent-green)' : leagueRoiPct > 0 ? 'var(--accent-red)' : 'var(--text-primary)';
                return `
                  <div style="display: flex; justify-content: space-between; align-items: center; padding: 0.5rem 0; border-bottom: 1px solid var(--border-color);">
                    <div style="display: flex; align-items: center; gap: 0.75rem;">
                      <span style="background: #f1f3f5; padding: 0.25rem 0.5rem; border-radius: 4px; font-size: 0.75rem; font-weight: 600; color: var(--text-primary);">${league}</span>
                      <span style="font-size: 0.875rem; color: var(--text-primary);">$${stats.fillValue.toLocaleString(undefined, {minimumFractionDigits: 0, maximumFractionDigits: 0})} • ${stats.fills} fills</span>
                    </div>
                    <div style="text-align: right;">
                      <div style="font-size: 0.875rem; font-weight: 600; color: ${leagueColor};">
                        ${stats.sportbookSpread >= 0 ? '+' : ''}$${Math.abs(stats.sportbookSpread).toLocaleString(undefined, {minimumFractionDigits: 0, maximumFractionDigits: 0})}
                      </div>
                      <div style="font-size: 0.75rem; color: ${leagueColor};">
                        (ROI ${leagueRoiPct >= 0 ? '+' : ''}${leagueRoiPct.toFixed(2)}% | Point ${leaguePointPct >= 0 ? '+' : ''}${leaguePointPct.toFixed(2)}%)
                      </div>
                    </div>
                  </div>
                `;
              }).join('')}
            </div>
          </div>
        </div>
      </div>
    `;

    container.innerHTML = html;
  } catch (error) {
    console.error('❌ Error loading Pinnacle summary:', error);
    const container = document.getElementById('pn-summary-container');
    if (container) {
      container.innerHTML = `<div style="text-align: center; padding: 2rem; color: var(--accent-red);">Error loading Pinnacle summary</div>`;
    }
  }
}

// Load opposite parties summary from backend
async function loadOppositePartiesSummary() {
  try {
    console.log('📊 Loading opposite parties summary...');
    const url = new URL(`${BACKEND_URL}/api/orders/opposite-parties`);
    if (userFilter) {
      url.searchParams.set('user_filter', userFilter);
    }
    const response = await fetch(url.toString());
    if (!response.ok) {
      console.error('❌ Failed to load opposite parties summary:', response.statusText);
      return;
    }

    const data = await response.json();
    const parties = data.parties || [];
    
    console.log(`✅ Loaded ${parties.length} opposite parties`);

    const container = document.getElementById('opposite-parties-table-container');
    if (!container) {
      console.error('❌ Opposite parties container not found');
      return;
    }

    if (parties.length === 0) {
      container.innerHTML = '<div style="text-align: center; padding: 2rem; color: var(--text-secondary);">No opposite parties data available</div>';
      return;
    }

    // Apply counterparty contains-filter (address / name / pseudonym)
    const filteredParties = !counterpartyFilter
      ? parties
      : parties.filter((party) => {
          const address = (party.address || '').toLowerCase();
          const pm = party.polymarket || {};
          const name = (pm.name || '').toLowerCase();
          const pseudonym = (pm.pseudonym || '').toLowerCase();
          return (
            address.includes(counterpartyFilter) ||
            name.includes(counterpartyFilter) ||
            pseudonym.includes(counterpartyFilter)
          );
        });

    if (filteredParties.length === 0) {
      container.innerHTML = `<div style="text-align: center; padding: 2rem; color: var(--text-secondary);">No counterparties match "${counterpartyFilter}"</div>`;
      return;
    }
    
    // Determine how many parties to show
    const partiesToShow = showAllParties ? filteredParties.length : Math.min(DEFAULT_PARTIES_TO_SHOW, filteredParties.length);
    const hasMore = filteredParties.length > DEFAULT_PARTIES_TO_SHOW;

    // Create table with header - improved design
    let tableHTML = `
      <div style="margin-bottom: 1.5rem; padding: 1rem 1.5rem; background: white; border-radius: 12px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); display: flex; justify-content: space-between; align-items: center; border: 1px solid var(--border-color);">
        <div style="display: flex; align-items: center; gap: 0.75rem;">
          <span style="font-size: 1.25rem;">🏆</span>
          <div>
            <div style="font-size: 1.125rem; font-weight: 600; color: var(--text-primary); margin-bottom: 0.25rem;">
              Top ${filteredParties.length} Counterparties
            </div>
            <div style="font-size: 0.875rem; color: var(--text-secondary);">
              Showing ${partiesToShow} of ${filteredParties.length} counterparties${counterpartyFilter ? ` (filtered from ${parties.length})` : ''}
            </div>
          </div>
        </div>
        ${hasMore ? `
          <button onclick="toggleShowAllParties()" 
                  style="padding: 0.625rem 1.25rem; background: var(--accent-blue); color: white; border: none; border-radius: 8px; cursor: pointer; font-weight: 600; font-size: 0.875rem; transition: all 0.2s; box-shadow: 0 2px 4px rgba(59, 130, 246, 0.3);"
                  onmouseover="this.style.background='#2563eb'; this.style.transform='translateY(-1px)'; this.style.boxShadow='0 4px 6px rgba(59, 130, 246, 0.4)'" 
                  onmouseout="this.style.background='var(--accent-blue)'; this.style.transform='translateY(0)'; this.style.boxShadow='0 2px 4px rgba(59, 130, 246, 0.3)'">
            ${showAllParties ? '▼ Show Less' : '▶ Show More'}
          </button>
        ` : ''}
      </div>
      <div style="background: white; border-radius: 12px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1); border: 1px solid var(--border-color);">
        <table style="width: 100%; border-collapse: collapse;">
        <thead>
          <tr style="background: linear-gradient(to bottom, #f8f9fa, #f1f3f5); border-bottom: 2px solid var(--border-color);">
            <th style="padding: 0.75rem 1rem; text-align: left; font-weight: 600; color: var(--text-primary); font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.02em; white-space: nowrap;">Counterparty</th>
            <th style="padding: 0.75rem 1rem; text-align: right; font-weight: 600; color: var(--text-primary); font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.02em; white-space: nowrap;">Score</th>
            <th style="padding: 0.75rem 1rem; text-align: right; font-weight: 600; color: var(--text-primary); font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.02em; white-space: nowrap;">Orders</th>
            <th style="padding: 0.75rem 1rem; text-align: right; font-weight: 600; color: var(--text-primary); font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.02em; white-space: nowrap;">Volume</th>
            <th style="padding: 0.75rem 1rem; text-align: right; font-weight: 600; color: var(--text-primary); font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.02em; white-space: nowrap;">P/L</th>
            <th style="padding: 0.75rem 1rem; text-align: right; font-weight: 600; color: var(--text-primary); font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.02em; white-space: nowrap;">PNL %</th>
            <th style="padding: 0.75rem 1rem; text-align: right; font-weight: 600; color: var(--text-primary); font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.02em; white-space: nowrap;">Profitable</th>
            <th style="padding: 0.75rem 1rem; text-align: right; font-weight: 600; color: var(--text-primary); font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.02em; white-space: nowrap;">Unprofitable</th>
            <th style="padding: 0.75rem 1rem; text-align: right; font-weight: 600; color: var(--text-primary); font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.02em; white-space: nowrap;">PM Trades</th>
            <th style="padding: 0.75rem 1rem; text-align: right; font-weight: 600; color: var(--text-primary); font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.02em; white-space: nowrap;">PM Vol</th>
            <th style="padding: 0.75rem 1rem; text-align: right; font-weight: 600; color: var(--text-primary); font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.02em; white-space: nowrap;">PM PNL</th>
            <th style="padding: 0.75rem 1rem; text-align: right; font-weight: 600; color: var(--text-primary); font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.02em; white-space: nowrap;">PM Rank</th>
          </tr>
        </thead>
        <tbody>
    `;

    filteredParties.slice(0, partiesToShow).forEach((party, index) => {
      const profitSign = party.profit >= 0 ? '+' : '';
      const pm = party.polymarket || {};
      
      // Format address/name display with card-like UI
      const name = pm.name || pm.pseudonym || null;
      const pseudonym = pm.pseudonym && pm.name ? pm.pseudonym : null;
      const verified = pm.verified_badge;
      const polymarketUrl = `https://polymarket.com/${party.address}`;
      
      // Color for rank badge (similar to image)
      const rankColors = ['#fbbf24', '#9ca3af', '#fb923c', '#3b82f6', '#3b82f6', '#3b82f6', '#3b82f6', '#3b82f6', '#3b82f6', '#3b82f6'];
      const badgeColor = rankColors[index % rankColors.length] || '#3b82f6';
      
      // Format display - prioritize username like the image
      const displayName = name || pseudonym || null;
      let addressDisplay = '';
      
      if (displayName) {
        // Show username prominently with badge, similar to image
        addressDisplay = `
          <div style="display: flex; align-items: flex-start; gap: 0.75rem; padding: 0.5rem 0;">
            <div style="width: 36px; height: 36px; border-radius: 50%; background: ${badgeColor}; display: flex; align-items: center; justify-content: center; color: white; font-weight: 700; font-size: 0.875rem; flex-shrink: 0;">
              ${index + 1}
            </div>
            <div style="flex: 1; min-width: 0; line-height: 1.5;">
              <div style="margin-bottom: 0.25rem;">
                <a href="${polymarketUrl}" target="_blank" rel="noopener noreferrer" 
                   style="font-weight: 600; color: #3b82f6; text-decoration: none; font-size: 0.9375rem; cursor: pointer; display: inline-block;"
                   onmouseover="this.style.textDecoration='underline'" 
                   onmouseout="this.style.textDecoration='none'">
                  ${displayName}
                </a>
                ${verified ? '<span style="color: #10b981; font-size: 0.875rem; margin-left: 0.25rem;">✓</span>' : ''}
              </div>
              ${pseudonym && name ? `
                <div style="font-size: 0.8125rem; color: #6b7280; margin-bottom: 0.25rem;">
                  @${pseudonym}
                </div>
              ` : ''}
              <div style="font-size: 0.75rem; color: #9ca3af; font-family: monospace; margin-bottom: 0.25rem;">
                ${formatAddress(party.address)}
              </div>
              ${pm.created_at ? `
                <div style="font-size: 0.75rem; color: #9ca3af; display: flex; align-items: center; gap: 0.25rem; margin-top: 0.125rem;">
                  <span style="font-size: 0.7rem;">🕐</span>
                  <span>Created ${formatTimeAgo(pm.created_at)}</span>
                </div>
              ` : ''}
              ${pm.global_rank !== null && pm.global_rank !== undefined ? `
                <div style="font-size: 0.75rem; color: #9ca3af; display: flex; align-items: center; gap: 0.25rem; margin-top: 0.125rem;">
                  <span style="color: #fbbf24; font-size: 0.7rem;">⭐</span>
                  <span>Global Rank: #${pm.global_rank.toLocaleString()}</span>
                </div>
              ` : ''}
            </div>
          </div>
        `;
      } else {
        // Fallback if no name/pseudonym - show address as clickable
        addressDisplay = `
          <div style="display: flex; align-items: center; gap: 0.75rem; padding: 0.5rem 0;">
            <div style="width: 36px; height: 36px; border-radius: 50%; background: ${badgeColor}; display: flex; align-items: center; justify-content: center; color: white; font-weight: 700; font-size: 0.875rem; flex-shrink: 0;">
              ${index + 1}
            </div>
            <div style="flex: 1;">
              <a href="${polymarketUrl}" target="_blank" rel="noopener noreferrer" 
                 style="font-family: monospace; font-size: 0.875rem; color: #3b82f6; text-decoration: none; cursor: pointer; font-weight: 500;"
                 onmouseover="this.style.textDecoration='underline'" 
                 onmouseout="this.style.textDecoration='none'">
                ${formatAddress(party.address)}
              </a>
            </div>
          </div>
        `;
      }
      
      // Format PM Global PNL - Green for positive, Red for negative
      const pmPnl = pm.global_pnl !== null && pm.global_pnl !== undefined ? pm.global_pnl : null;
      const pmPnlSign = pmPnl !== null && pmPnl >= 0 ? '+' : '';
      const pmPnlColor = pmPnl !== null ? (pmPnl > 0 ? 'var(--accent-green)' : pmPnl < 0 ? 'var(--accent-red)' : 'var(--text-primary)') : 'var(--text-secondary)';
      
      tableHTML += `
        <tr style="border-bottom: 1px solid var(--border-color); transition: background 0.2s;" 
            onmouseover="this.style.background='#f8f9fa'" 
            onmouseout="this.style.background='white'">
          <td style="padding: 0.875rem 1rem; color: var(--text-primary); font-size: 0.875rem; min-width: 240px; max-width: 280px;">
            ${addressDisplay}
          </td>
          <td style="padding: 0.875rem 1rem; text-align: right; color: ${party.score !== null && party.score !== undefined ? (party.score >= 0 ? '#10b981' : '#ef4444') : 'var(--text-secondary)'}; font-weight: 600; font-size: 0.875rem; white-space: nowrap;">
            ${party.score !== null && party.score !== undefined ? party.score.toFixed(2) : '-'}
          </td>
          <td style="padding: 0.875rem 1rem; text-align: right; color: var(--text-primary); font-weight: 500; font-size: 0.875rem; white-space: nowrap;">
            ${party.orders.toLocaleString()}
          </td>
          <td style="padding: 0.875rem 1rem; text-align: right; color: var(--text-primary); font-weight: 600; font-size: 0.875rem; white-space: nowrap;">
            $${party.volume.toFixed(2)}
          </td>
          <td style="padding: 0.875rem 1rem; text-align: right; color: ${party.profit < 0 ? 'var(--accent-green)' : party.profit > 0 ? 'var(--accent-red)' : 'var(--text-primary)'}; font-weight: 600; font-size: 0.875rem; white-space: nowrap;">
            ${profitSign}$${Math.abs(party.profit).toFixed(2)}
          </td>
          <td style="padding: 0.875rem 1rem; text-align: right; color: ${party.pnl_percentage !== null && party.pnl_percentage !== undefined ? (party.pnl_percentage < 0 ? 'var(--accent-green)' : party.pnl_percentage > 0 ? 'var(--accent-red)' : 'var(--text-primary)') : 'var(--text-secondary)'}; font-weight: 600; font-size: 0.875rem; white-space: nowrap;">
            ${party.pnl_percentage !== null && party.pnl_percentage !== undefined ? `${party.pnl_percentage >= 0 ? '+' : ''}${party.pnl_percentage.toFixed(2)}%` : '-'}
          </td>
          <td style="padding: 0.875rem 1rem; text-align: right; color: var(--accent-green); font-weight: 600; font-size: 0.875rem; white-space: nowrap;">
            $${(party.profitable_volume || 0).toFixed(2)}
          </td>
          <td style="padding: 0.875rem 1rem; text-align: right; color: var(--accent-red); font-weight: 600; font-size: 0.875rem; white-space: nowrap;">
            $${(party.unprofitable_volume || 0).toFixed(2)}
          </td>
          <td style="padding: 0.875rem 1rem; text-align: right; color: var(--text-primary); font-weight: 500; font-size: 0.875rem; white-space: nowrap;">
            ${pm.total_trades !== null && pm.total_trades !== undefined ? pm.total_trades.toLocaleString() : '-'}
          </td>
          <td style="padding: 0.875rem 1rem; text-align: right; color: var(--text-primary); font-weight: 600; font-size: 0.875rem; white-space: nowrap;">
            ${pm.global_volume !== null && pm.global_volume !== undefined ? `$${pm.global_volume.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})}` : '-'}
          </td>
          <td style="padding: 0.875rem 1rem; text-align: right; color: ${pmPnlColor}; font-weight: 600; font-size: 0.875rem; white-space: nowrap;">
            ${pmPnl !== null ? `${pmPnlSign}$${Math.abs(pmPnl).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})}` : '-'}
          </td>
          <td style="padding: 0.875rem 1rem; text-align: right; color: var(--text-primary); font-weight: 500; font-size: 0.875rem; white-space: nowrap;">
            ${pm.global_rank !== null && pm.global_rank !== undefined ? `#${pm.global_rank.toLocaleString()}` : '-'}
          </td>
        </tr>
      `;
    });

    tableHTML += `
        </tbody>
      </table>
      </div>
    `;

    container.innerHTML = tableHTML;
  } catch (error) {
    console.error('❌ Error loading opposite parties summary:', error);
    const container = document.getElementById('opposite-parties-table-container');
    if (container) {
      container.innerHTML = `<div style="text-align: center; padding: 2rem; color: var(--accent-red);">Error loading opposite parties data</div>`;
    }
  }
}

// Load orders from backend
async function loadOrdersFromBackend() {
  try {
    console.log('📦 Loading order history from backend...');
    const response = await fetch(`${BACKEND_URL}/api/orders/history`);
    if (!response.ok) {
      console.error('❌ Failed to load order history:', response.statusText);
      return;
    }

    const data = await response.json();
    const orders = data.orders || [];
    
    console.log(`✅ Loaded ${orders.length} orders from backend`);

    // Reset stats
    orderCount = 0;
    buyCount = 0;
    sellCount = 0;
    totalVolume = 0;

    // Backend sends newest first, but we need to process oldest first
    // so that insertBefore puts newest at top
    const ordersReversed = [...orders].reverse();

    // Process each order (oldest first, so insertBefore puts newest at top)
    ordersReversed.forEach(order => {
      const orderId = getPrimaryOrderId(order);
      if (!ordersMap.has(orderId)) {
        // Debug: Check if PM data is present
        const hasBefore = order.polymarket_before !== undefined && order.polymarket_before !== null;
        const hasAfter = order.polymarket_after !== undefined && order.polymarket_after !== null;
        const hasSportbook = order.sportbook !== undefined && order.sportbook !== null;
        if (hasBefore || hasAfter || hasSportbook) {
          console.log(`📦 Loading order ${orderId} with PM data - Before: ${hasBefore}, After: ${hasAfter}, Sportbook: ${hasSportbook}`);
        }
        
        // Update stats
        orderCount++;
        if (order.side === 'BUY') {
          buyCount++;
        } else {
          sellCount++;
        }
        totalVolume += (order.shares_normalized || 0) * (order.price || 0);
        
        // Add to table using existing handler (but skip stats update)
        handleOrderUpdate(order, true); // true = skip stats update
      }
    });

    // Update stats display once at the end
    const totalOrdersEl = document.getElementById('total-orders');
    const buyOrdersEl = document.getElementById('buy-orders');
    const sellOrdersEl = document.getElementById('sell-orders');
    const totalVolumeEl = document.getElementById('total-volume');

    if (totalOrdersEl) totalOrdersEl.textContent = orderCount.toLocaleString();
    if (buyOrdersEl) buyOrdersEl.textContent = buyCount.toLocaleString();
    if (sellOrdersEl) sellOrdersEl.textContent = sellCount.toLocaleString();
    if (totalVolumeEl) totalVolumeEl.textContent = `$${totalVolume.toFixed(2)}`;
    
    console.log(`📊 Restored ${orders.length} orders, ${orderCount} total`);
  } catch (error) {
    console.error('❌ Error loading orders from backend:', error);
  }
}

// Handle order updates
function handleOrderUpdate(data, skipStats = false) {
  try {
    // Create stable order ID (supports tx_hash appearing later)
    const nextOrderId = getPrimaryOrderId(data);
    const existingOrderId = findExistingOrderId(data);
    const orderId = existingOrderId || nextOrderId;
    
    // Check if this is an update to existing order
    const isUpdate = !!existingOrderId;
    
    if (isUpdate) {
      // Update existing order in the table
      const existingRow = document.querySelector(`[data-order-id="${existingOrderId}"]`);
      if (existingRow) {
        // If we now have a stronger ID (e.g. tx_hash arrived), migrate row ID
        if (nextOrderId !== existingOrderId) {
          existingRow.setAttribute('data-order-id', nextOrderId);
        }
        // Always target by data-column (safer than fixed indexes)
        const txHashCell = existingRow.querySelector('td[data-column="tx-hash"]');
        const pmBeforeCell = existingRow.querySelector('td[data-column="pm-before"]');
        const pmAfterCell = existingRow.querySelector('td[data-column="pm-after"]');
        const pmSportbookCell = existingRow.querySelector('td[data-column="pm-sportbook"]');
        const pnBeforeCell = existingRow.querySelector('td[data-column="pn-before"]');
        const pnAfterCell = existingRow.querySelector('td[data-column="pn-after"]');
        const pnSportbookCell = existingRow.querySelector('td[data-column="pn-sportbook"]');

        if (txHashCell) {
          txHashCell.innerHTML = formatTxHashLink(data.tx_hash);
        }
        
        if (pmBeforeCell) {
          pmBeforeCell.innerHTML = formatPolymarketPercentage(data.price, data.polymarket_before, 'BEFORE', orderId);
          // Ensure details are collapsed (minimized) by default
          const expandedEl = document.getElementById(`pm-before-${orderId}`);
          if (expandedEl) expandedEl.style.display = 'none';
          // Reset toggle icon to collapsed state
          const toggleEl = pmBeforeCell.querySelector('.pm-toggle');
          if (toggleEl) toggleEl.textContent = '▶';
        }
        if (pmAfterCell) {
          pmAfterCell.innerHTML = formatPolymarketPercentage(data.price, data.polymarket_after, 'AFTER', orderId);
          // Ensure details are collapsed (minimized) by default
          const expandedEl = document.getElementById(`pm-after-${orderId}`);
          if (expandedEl) expandedEl.style.display = 'none';
          // Reset toggle icon to collapsed state
          const toggleEl = pmAfterCell.querySelector('.pm-toggle');
          if (toggleEl) toggleEl.textContent = '▶';
        }
        if (pmSportbookCell) {
          pmSportbookCell.innerHTML = formatSportbook(data.sportbook, data.price);
        }
        if (pnBeforeCell) {
          pnBeforeCell.innerHTML = formatPolymarketPercentage(data.price, data.pinnacle_before, 'PN_BEFORE', orderId);
          const expandedEl = document.getElementById(`pm-pn_before-${orderId}`);
          if (expandedEl) expandedEl.style.display = 'none';
          const toggleEl = pnBeforeCell.querySelector('.pm-toggle');
          if (toggleEl) toggleEl.textContent = '▶';
        }
        if (pnAfterCell) {
          pnAfterCell.innerHTML = formatPolymarketPercentage(data.price, data.pinnacle_after, 'PN_AFTER', orderId);
          const expandedEl = document.getElementById(`pm-pn_after-${orderId}`);
          if (expandedEl) expandedEl.style.display = 'none';
          const toggleEl = pnAfterCell.querySelector('.pm-toggle');
          if (toggleEl) toggleEl.textContent = '▶';
        }
        if (pnSportbookCell) {
          pnSportbookCell.innerHTML = formatPinnacleSportbook(data.price, data.pinnacle_after, data.pinnacle_sportbook);
        }
        
        // Highlight the row to show it was updated
        existingRow.classList.add('new-row');
        setTimeout(() => existingRow.classList.remove('new-row'), 2000);
      }
      
      // Update the order data in map (backend handles persistence)
      if (nextOrderId !== existingOrderId) {
        ordersMap.delete(existingOrderId);
      }
      ordersMap.set(nextOrderId, data);
      
      // Keep main table row visibility in sync with selected time filter
      if (existingRow) {
        existingRow.style.display = isOrderVisibleByFilters(data) ? '' : 'none';
      }
      
      // Debounce summary updates to avoid too many refreshes
      if (summaryUpdateTimer) clearTimeout(summaryUpdateTimer);
      summaryUpdateTimer = setTimeout(() => {
        loadPMSummary();
        loadPinnacleSummary();
        loadGameSummaries();
      }, 300);
      
      if (oppositePartiesUpdateTimer) clearTimeout(oppositePartiesUpdateTimer);
      oppositePartiesUpdateTimer = setTimeout(() => {
        loadOppositePartiesSummary();
      }, 300);
      
      return;
    }
    
    // New order - add to stats and table (unless loading from backend)
    if (!skipStats) {
      orderCount++;
      if (data.side === 'BUY') {
        buyCount++;
      } else {
        sellCount++;
      }
      totalVolume += (data.shares_normalized || 0) * (data.price || 0);

      // Update stats
      const totalOrdersEl = document.getElementById('total-orders');
      const buyOrdersEl = document.getElementById('buy-orders');
      const sellOrdersEl = document.getElementById('sell-orders');
      const totalVolumeEl = document.getElementById('total-volume');

      if (totalOrdersEl) totalOrdersEl.textContent = orderCount.toLocaleString();
      if (buyOrdersEl) buyOrdersEl.textContent = buyCount.toLocaleString();
      if (sellOrdersEl) sellOrdersEl.textContent = sellCount.toLocaleString();
      if (totalVolumeEl) totalVolumeEl.textContent = `$${totalVolume.toFixed(2)}`;
    }

    // Format timestamps
    let orderDate;
    if (data.timestamp) {
      orderDate = new Date(data.timestamp * 1000);
    } else {
      orderDate = new Date();
    }
    const orderDateStr = orderDate.toLocaleDateString();
    const orderTimeStr = formatTimeWithMs(orderDate);

    let receivedDate;
    if (data.received_at) {
      receivedDate = parseReceivedAt(data.received_at);
    } else {
      receivedDate = new Date();
    }
    const receivedTimeStr = formatTimeWithMs(receivedDate, true);

    // Calculate value
    const value = ((data.shares_normalized || 0) * (data.price || 0)).toFixed(2);

    // Create row
    const row = document.createElement('tr');
    row.className = 'new-row';
    row.setAttribute('data-order-id', nextOrderId);
    row.innerHTML = `
      <td data-column="date" class="timestamp">${orderDateStr}</td>
      <td data-column="order-time" class="timestamp">${orderTimeStr}</td>
      <td data-column="received-at" class="timestamp">${receivedTimeStr}</td>
      <td data-column="side"><span class="badge ${data.side?.toLowerCase() || 'buy'}">${data.side || '-'}</span></td>
      <td data-column="league"><span class="badge league">${extractLeague(data.market_slug)}</span></td>
      <td data-column="title">${data.title || '-'}</td>
      <td data-column="token-label">${data.token_label || '-'}</td>
      <td data-column="token-id" class="address">${formatHash(data.token_id, 10, 8)}</td>
      <td data-column="market-slug">${data.market_slug || '-'}</td>
      <td data-column="condition-id" class="address">${formatHash(data.condition_id, 10, 8)}</td>
      <td data-column="price" class="number price">$${(data.price || 0).toFixed(4)}</td>
      <td data-column="shares-norm" class="number shares">${data.shares_normalized ? data.shares_normalized.toFixed(2) : '-'}</td>
      <td data-column="shares-raw" class="number">${data.shares ? data.shares.toLocaleString() : '-'}</td>
      <td data-column="value" class="number">$${value}</td>
      <td data-column="block-number" class="number">${data.block_number || '-'}</td>
      <td data-column="log-index" class="number">${data.log_index || '-'}</td>
      <td data-column="user" class="address">${formatAddress(data.user)}</td>
      <td data-column="taker" class="address">${formatAddress(data.taker)}</td>
      <td data-column="order-hash" class="address">${formatHash(data.order_hash, 10, 8)}</td>
      <td data-column="tx-hash" class="address">${formatTxHashLink(data.tx_hash)}</td>
      <td data-column="pm-before" class="pm-cell">${formatPolymarketPercentage(data.price, data.polymarket_before, 'BEFORE', orderId)}</td>
      <td data-column="pm-after" class="pm-cell">${formatPolymarketPercentage(data.price, data.polymarket_after, 'AFTER', orderId)}</td>
      <td data-column="pm-sportbook" class="number">${formatSportbook(data.sportbook, data.price)}</td>
      <td data-column="pn-before" class="pm-cell">${formatPolymarketPercentage(data.price, data.pinnacle_before, 'PN_BEFORE', orderId)}</td>
      <td data-column="pn-after" class="pm-cell">${formatPolymarketPercentage(data.price, data.pinnacle_after, 'PN_AFTER', orderId)}</td>
      <td data-column="pn-sportbook" class="number">${formatPinnacleSportbook(data.price, data.pinnacle_after, data.pinnacle_sportbook)}</td>
    `;
    
    // Apply column visibility
    applyColumnVisibility(row);
    // Apply current time filter visibility for the new row
    row.style.display = isOrderVisibleByFilters(data) ? '' : 'none';

    // Add to table - ensure DOM is ready
    const tableBody = document.getElementById('orders-table');
    if (!tableBody) {
      console.error('❌ Table body not found! Retrying in 100ms...');
      // Retry if DOM not ready
      setTimeout(() => handleOrderUpdate(data), 100);
      return;
    }
    
    // Remove loading message if present
    if (tableBody.children.length === 1) {
      const firstChild = tableBody.children[0];
      if (firstChild && (firstChild.classList.contains('loading') || firstChild.querySelector('.loading'))) {
        tableBody.innerHTML = '';
      }
    }
    
    // Store in map FIRST (before DOM update) - backend handles persistence
    ordersMap.set(nextOrderId, data);

    // Use requestAnimationFrame for smooth DOM updates
    requestAnimationFrame(() => {
      if (tableBody && row) {
        // Ensure we're inserting at the top (newest first)
        if (tableBody.firstChild) {
          tableBody.insertBefore(row, tableBody.firstChild);
        } else {
          tableBody.appendChild(row);
        }
      }
    });

    // Remove old rows
    while (tableBody.children.length > maxRows) {
      const lastRow = tableBody.lastChild;
      const lastOrderId = lastRow.getAttribute('data-order-id');
      if (lastOrderId) ordersMap.delete(lastOrderId);
      tableBody.removeChild(lastRow);
    }

    // Debounce summary updates to avoid too many refreshes and ensure data is in map
    // Use a small delay to ensure DOM is updated first
    if (summaryUpdateTimer) clearTimeout(summaryUpdateTimer);
    summaryUpdateTimer = setTimeout(() => {
      loadPMSummary();
      loadPinnacleSummary();
      loadGameSummaries();
    }, 100);
    
    if (oppositePartiesUpdateTimer) clearTimeout(oppositePartiesUpdateTimer);
    oppositePartiesUpdateTimer = setTimeout(() => {
      loadOppositePartiesSummary();
    }, 100);

    // Remove highlight after animation
    setTimeout(() => {
      row.classList.remove('new-row');
    }, 2000);
  } catch (error) {
    console.error('❌ Error in handleOrderUpdate:', error);
  }
}

// Create UI
function createUI() {
  app = document.getElementById('app');
  if (!app) {
    console.error('❌ App element not found!');
    return;
  }

  app.innerHTML = `
    <div class="container">
      <div class="header">
        <div style="display: flex; align-items: flex-start; margin-bottom: 1.5rem; gap: 2rem;">
          <div style="flex: 1;">
            <h1 style="margin: 0 0 1rem 0; display: flex; align-items: center; gap: 1rem;">
              TRADES HISTORY and CounterParties
              <span id="status" class="status disconnected">Disconnected</span>
            </h1>
          </div>
        </div>
      </div>
      
      <!-- Order: 1. Counterparties, 2. PM Summary, 3. PN Summary, 4. Game Summaries, 5. Transaction Table -->
      <div class="opposite-parties-summary" id="opposite-parties-summary" style="margin-bottom: 2rem; width: 100%; clear: both; display: block;">
        <div id="opposite-parties-table-container">
          <div class="loading">Loading opposite parties data...</div>
        </div>
        <div style="display: flex; align-items: center; gap: 0.75rem; flex-wrap: wrap; margin-top: 1rem;">
          <div class="time-filter-container" style="position: relative; z-index: 100;">
            <label style="font-weight: 500; color: var(--text-primary); margin-right: 0.5rem; font-size: 0.875rem;">Time Filter:</label>
            <span class="timezone-chip">Received time shown in UTC+8 (Singapore)</span>
            <button id="time-filter-btn" class="time-filter-btn" onclick="toggleTimeFilterMenu(event)" style="background: white; border: 1px solid var(--border-color); border-radius: 6px; padding: 0.5rem 1rem; cursor: pointer; font-size: 0.875rem; color: var(--text-primary); font-weight: 500; min-width: 140px; text-align: left; position: relative; display: flex; align-items: center; justify-content: space-between; z-index: 101;">
              <span id="time-filter-label">All Time</span>
              <span style="margin-left: 0.5rem;">▼</span>
            </button>
            <div id="time-filter-menu" class="time-filter-menu" style="display: none; position: absolute; top: 100%; left: 0; margin-top: 0.25rem; background: #ffffff; border-radius: 8px; box-shadow: 0 10px 30px rgba(15, 23, 42, 0.15); z-index: 1002; min-width: 180px; padding: 0.5rem 0; border: 1px solid #dbe6f3;">
              <div class="time-filter-option" onclick="(function(e){e.stopPropagation(); setTimeFilter('3h');})(event)" style="padding: 0.625rem 1rem; cursor: pointer; color: #334155; font-size: 0.875rem; display: flex; align-items: center; gap: 0.5rem; transition: background 0.2s; pointer-events: auto;" onmouseover="this.style.background='#f1f5f9'" onmouseout="this.style.background='transparent'">
                <span id="time-filter-check-3h" style="display: none; color: white;">✓</span>
                <span>Last 3 Hours</span>
              </div>
              <div class="time-filter-option" onclick="(function(e){e.stopPropagation(); setTimeFilter('8h');})(event)" style="padding: 0.625rem 1rem; cursor: pointer; color: #334155; font-size: 0.875rem; display: flex; align-items: center; gap: 0.5rem; transition: background 0.2s; pointer-events: auto;" onmouseover="this.style.background='#f1f5f9'" onmouseout="this.style.background='transparent'">
                <span id="time-filter-check-8h" style="display: none; color: white;">✓</span>
                <span>Last 8 Hours</span>
              </div>
              <div class="time-filter-option" onclick="(function(e){e.stopPropagation(); setTimeFilter('12h');})(event)" style="padding: 0.625rem 1rem; cursor: pointer; color: #334155; font-size: 0.875rem; display: flex; align-items: center; gap: 0.5rem; transition: background 0.2s; pointer-events: auto;" onmouseover="this.style.background='#f1f5f9'" onmouseout="this.style.background='transparent'">
                <span id="time-filter-check-12h" style="display: none; color: white;">✓</span>
                <span>Last 12 Hours</span>
              </div>
              <div class="time-filter-option" onclick="(function(e){e.stopPropagation(); setTimeFilter('24h');})(event)" style="padding: 0.625rem 1rem; cursor: pointer; color: #334155; font-size: 0.875rem; display: flex; align-items: center; gap: 0.5rem; transition: background 0.2s; pointer-events: auto;" onmouseover="this.style.background='#f1f5f9'" onmouseout="this.style.background='transparent'">
                <span id="time-filter-check-24h" style="display: none; color: white;">✓</span>
                <span>Last 24 Hours</span>
              </div>
              <div class="time-filter-option" onclick="(function(e){e.stopPropagation(); setTimeFilter('all');})(event)" style="padding: 0.625rem 1rem; cursor: pointer; background: #e8f1ff; color: #1d4ed8; font-size: 0.875rem; font-weight: 600; margin-top: 0.25rem; border-top: 1px solid #dbe6f3; border-radius: 0 0 8px 8px; pointer-events: auto;">
                <span>All Time</span>
              </div>
            </div>
          </div>
          <div style="display: flex; align-items: center; gap: 0.5rem;">
            <label for="game-slug-filter-input" style="font-weight: 500; color: var(--text-primary); font-size: 0.875rem;">Game Slug:</label>
            <input
              id="game-slug-filter-input"
              type="text"
              placeholder="e.g. nba-uta"
              oninput="setGameSlugFilter(this.value)"
              style="height: 38px; min-width: 200px; padding: 0.5rem 0.75rem; border: 1px solid var(--border-color); border-radius: 8px; font-size: 0.875rem; color: var(--text-primary); background: white; outline: none;"
            />
          </div>
          <div style="display: flex; align-items: center; gap: 0.5rem;">
            <label for="counterparty-filter-input" style="font-weight: 500; color: var(--text-primary); font-size: 0.875rem;">Counterparty:</label>
            <input
              id="counterparty-filter-input"
              type="text"
              placeholder="e.g. 0xcb or 5f7"
              oninput="setCounterpartyFilter(this.value)"
              style="height: 38px; min-width: 220px; padding: 0.5rem 0.75rem; border: 1px solid var(--border-color); border-radius: 8px; font-size: 0.875rem; color: var(--text-primary); background: white; outline: none;"
            />
          </div>
          <div style="display: flex; align-items: center; gap: 0.5rem;">
            <label for="user-filter-input" style="font-weight: 500; color: var(--text-primary); font-size: 0.875rem;">User:</label>
            <input
              id="user-filter-input"
              type="text"
              placeholder="e.g. 0x9d94"
              oninput="setUserFilter(this.value)"
              style="height: 38px; min-width: 220px; padding: 0.5rem 0.75rem; border: 1px solid var(--border-color); border-radius: 8px; font-size: 0.875rem; color: var(--text-primary); background: white; outline: none;"
            />
          </div>
          <div class="column-toggle-container">
            <button class="column-toggle-btn" onclick="toggleColumnMenu()">
              Show/Hide Columns
            </button>
            <div id="column-menu" class="column-menu" style="display: none;">
              <div class="column-menu-header">SHOW/HIDE COLUMNS</div>
              <div class="column-menu-content">
                <label><input type="checkbox" data-column="date" ${columnVisibility['date'] ? 'checked' : ''} onchange="toggleColumn('date', this.checked)"> Date</label>
                <label><input type="checkbox" data-column="order-time" ${columnVisibility['order-time'] ? 'checked' : ''} onchange="toggleColumn('order-time', this.checked)"> Order Time</label>
                <label><input type="checkbox" data-column="received-at" ${columnVisibility['received-at'] ? 'checked' : ''} onchange="toggleColumn('received-at', this.checked)"> Received At (UTC+8)</label>
                <label><input type="checkbox" data-column="side" ${columnVisibility['side'] ? 'checked' : ''} onchange="toggleColumn('side', this.checked)"> Side</label>
                <label><input type="checkbox" data-column="league" ${columnVisibility['league'] ? 'checked' : ''} onchange="toggleColumn('league', this.checked)"> League</label>
                <label><input type="checkbox" data-column="title" ${columnVisibility['title'] ? 'checked' : ''} onchange="toggleColumn('title', this.checked)"> Title</label>
                <label><input type="checkbox" data-column="token-label" ${columnVisibility['token-label'] ? 'checked' : ''} onchange="toggleColumn('token-label', this.checked)"> Token Label</label>
                <label><input type="checkbox" data-column="token-id" ${columnVisibility['token-id'] ? 'checked' : ''} onchange="toggleColumn('token-id', this.checked)"> Token ID</label>
                <label><input type="checkbox" data-column="market-slug" ${columnVisibility['market-slug'] ? 'checked' : ''} onchange="toggleColumn('market-slug', this.checked)"> Market Slug</label>
                <label><input type="checkbox" data-column="condition-id" ${columnVisibility['condition-id'] ? 'checked' : ''} onchange="toggleColumn('condition-id', this.checked)"> Condition ID</label>
                <label><input type="checkbox" data-column="price" ${columnVisibility['price'] ? 'checked' : ''} onchange="toggleColumn('price', this.checked)"> Price</label>
                <label><input type="checkbox" data-column="shares-norm" ${columnVisibility['shares-norm'] ? 'checked' : ''} onchange="toggleColumn('shares-norm', this.checked)"> Shares (Norm)</label>
                <label><input type="checkbox" data-column="shares-raw" ${columnVisibility['shares-raw'] ? 'checked' : ''} onchange="toggleColumn('shares-raw', this.checked)"> Shares (Raw)</label>
                <label><input type="checkbox" data-column="value" ${columnVisibility['value'] ? 'checked' : ''} onchange="toggleColumn('value', this.checked)"> Value</label>
                <label><input type="checkbox" data-column="block-number" ${columnVisibility['block-number'] ? 'checked' : ''} onchange="toggleColumn('block-number', this.checked)"> Block #</label>
                <label><input type="checkbox" data-column="log-index" ${columnVisibility['log-index'] ? 'checked' : ''} onchange="toggleColumn('log-index', this.checked)"> Log Index</label>
                <label><input type="checkbox" data-column="user" ${columnVisibility['user'] ? 'checked' : ''} onchange="toggleColumn('user', this.checked)"> User</label>
                <label><input type="checkbox" data-column="taker" ${columnVisibility['taker'] ? 'checked' : ''} onchange="toggleColumn('taker', this.checked)"> Taker</label>
                <label><input type="checkbox" data-column="order-hash" ${columnVisibility['order-hash'] ? 'checked' : ''} onchange="toggleColumn('order-hash', this.checked)"> Order Hash</label>
                <label><input type="checkbox" data-column="tx-hash" ${columnVisibility['tx-hash'] ? 'checked' : ''} onchange="toggleColumn('tx-hash', this.checked)"> TX Hash</label>
                <label><input type="checkbox" data-column="pm-before" ${columnVisibility['pm-before'] ? 'checked' : ''} onchange="toggleColumn('pm-before', this.checked)"> PM Before</label>
                <label><input type="checkbox" data-column="pm-after" ${columnVisibility['pm-after'] ? 'checked' : ''} onchange="toggleColumn('pm-after', this.checked)"> PM After</label>
                <label><input type="checkbox" data-column="pm-sportbook" ${columnVisibility['pm-sportbook'] ? 'checked' : ''} onchange="toggleColumn('pm-sportbook', this.checked)"> PM Sportbook</label>
                <label><input type="checkbox" data-column="pn-before" ${columnVisibility['pn-before'] ? 'checked' : ''} onchange="toggleColumn('pn-before', this.checked)"> PN Before</label>
                <label><input type="checkbox" data-column="pn-after" ${columnVisibility['pn-after'] ? 'checked' : ''} onchange="toggleColumn('pn-after', this.checked)"> PN After</label>
                <label><input type="checkbox" data-column="pn-sportbook" ${columnVisibility['pn-sportbook'] ? 'checked' : ''} onchange="toggleColumn('pn-sportbook', this.checked)"> PN Sportbook</label>
              </div>
            </div>
          </div>
          <div class="column-toggle-container">
            <button id="toggle-game-cards-btn" class="column-toggle-btn" onclick="toggleGameCardsSection()">
              Hide Game Cards
            </button>
          </div>
        </div>
      </div>
      
      <div id="pm-summary-container" style="margin-bottom: 2rem; width: 100%; clear: both; display: block;">
        <div class="loading">Loading PM summary...</div>
      </div>

      <div id="pn-summary-container" style="margin-bottom: 2rem; width: 100%; clear: both; display: block;">
        <div class="loading">Loading Pinnacle summary...</div>
      </div>
      
      <div id="game-summaries-container" style="margin-bottom: 2rem; width: 100%; clear: both; display: block;">
        <div class="loading">Loading game summaries...</div>
      </div>

      <div class="table-container" style="width: 100%; clear: both; display: block;">
        <table>
          <thead>
            <tr>
              <th data-column="date">Date</th>
              <th data-column="order-time">Order Time</th>
              <th data-column="received-at">Received At (UTC+8)</th>
              <th data-column="side">Side</th>
              <th data-column="league">League</th>
              <th data-column="title">Title</th>
              <th data-column="token-label">Token Label</th>
              <th data-column="token-id">Token ID</th>
              <th data-column="market-slug">Market Slug</th>
              <th data-column="condition-id">Condition ID</th>
              <th data-column="price">Price</th>
              <th data-column="shares-norm">Shares (Norm)</th>
              <th data-column="shares-raw">Shares (Raw)</th>
              <th data-column="value">Value</th>
              <th data-column="block-number">Block #</th>
              <th data-column="log-index">Log Index</th>
              <th data-column="user">User</th>
              <th data-column="taker">Taker</th>
              <th data-column="order-hash">Order Hash</th>
              <th data-column="tx-hash">TX Hash</th>
              <th data-column="pm-before">PM Before</th>
              <th data-column="pm-after">PM After</th>
              <th data-column="pm-sportbook">PM Sportbook</th>
              <th data-column="pn-before">PN Before</th>
              <th data-column="pn-after">PN After</th>
              <th data-column="pn-sportbook">PN Sportbook</th>
            </tr>
          </thead>
          <tbody id="orders-table">
            <tr>
              <td colspan="26" class="loading">Waiting for order updates...</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  `;

  // Initialize socket after UI is created
  initSocket();
  
  // Load order history from backend
  loadOrdersFromBackend();
  
  // Load opposite parties summary
  loadOppositePartiesSummary();
  
  // Load PM summary and game summaries after a short delay to ensure orders are loaded
  setTimeout(() => {
    loadPMSummary();
    loadPinnacleSummary();
    loadGameSummaries();
  }, 500);
  
  // Initialize column checkboxes and apply visibility
  setTimeout(() => {
    Object.keys(columnVisibility).forEach(columnName => {
      const checkbox = document.querySelector(`input[data-column="${columnName}"]`);
      if (checkbox) {
        checkbox.checked = columnVisibility[columnName];
      }
      // Apply visibility to headers and cells
      toggleColumn(columnName, columnVisibility[columnName]);
    });
    
    // Initialize time filter UI
    setTimeFilter('all');
    applyGameCardsVisibility();
  }, 100);
}

// Initialize Socket.IO connection
function initSocket() {
  // Use global io from CDN
  if (typeof io === 'undefined') {
    console.error('❌ Socket.IO not loaded. Make sure the CDN script is in index.html');
    return;
  }
  
  socket = io(BACKEND_URL, {
    reconnection: true,
    reconnectionDelay: 1000,
    reconnectionAttempts: 5,
    transports: ['websocket', 'polling']
  });

  socket.on('connect', () => {
    console.log('✅ Connected to backend');
    const statusEl = document.getElementById('status');
    if (statusEl) {
      statusEl.textContent = 'Connected';
      statusEl.className = 'status connected';
    }
  });

  socket.on('disconnect', () => {
    console.log('❌ Disconnected from backend');
    const statusEl = document.getElementById('status');
    if (statusEl) {
      statusEl.textContent = 'Disconnected';
      statusEl.className = 'status disconnected';
    }
  });

  socket.on('status', (data) => {
    const statusEl = document.getElementById('status');
    if (statusEl) {
      statusEl.textContent = data.connected ? 'Connected' : 'Disconnected';
      statusEl.className = `status ${data.connected ? 'connected' : 'disconnected'}`;
    }
  });

  socket.on('order-update', (data) => {
    console.log('📊 Received order update:', data);
    try {
      handleOrderUpdate(data);
    } catch (error) {
      console.error('❌ Error handling order update:', error, data);
    }
  });

  // Ensure socket is ready
  if (socket.connected) {
    console.log('✅ Socket already connected');
  } else {
    console.log('⏳ Waiting for socket connection...');
    socket.once('connect', () => {
      console.log('✅ Socket connected, ready for updates');
    });
  }

  socket.on('connect_error', (error) => {
    console.error('❌ Connection error:', error);
  });
}

// Prevent page refresh on errors
window.addEventListener('error', (e) => {
  // Ignore connection refused errors from Vite HMR
  if (e.message && e.message.includes('ERR_CONNECTION_REFUSED')) {
    return false;
  }
  console.error('Error caught:', e.error);
  e.preventDefault();
  return false;
});

window.addEventListener('unhandledrejection', (e) => {
  // Ignore connection errors
  if (e.reason && (e.reason.message && e.reason.message.includes('ERR_CONNECTION_REFUSED'))) {
    return false;
  }
  console.error('Unhandled promise rejection:', e.reason);
  e.preventDefault();
});

// Suppress Vite HMR connection errors in console
const originalError = console.error;
console.error = function(...args) {
  const message = args.join(' ');
  // Filter out connection refused errors
  if (message.includes('ERR_CONNECTION_REFUSED') || message.includes('net::ERR_CONNECTION_REFUSED')) {
    return;
  }
  originalError.apply(console, args);
};

// Initialize when DOM is ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', createUI);
} else {
  createUI();
}
