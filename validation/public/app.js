// Global state
let votings = [];
let municipalities = [];
let currentComparison = null;

// DOM elements
const votingSelect = document.getElementById('voting-select');
const municipalitySelect = document.getElementById('municipality-select');
const compareBtn = document.getElementById('compare-btn');
const loading = document.getElementById('loading');
const results = document.getElementById('results');
const comparison = document.getElementById('comparison');

// Initialize app
async function init() {
  try {
    // Load votings
    const votingsResponse = await fetch('/api/votings');
    votings = await votingsResponse.json();
    populateVotingSelect();

    // Load municipalities with changes
    const municipalitiesResponse = await fetch('/api/municipalities');
    municipalities = await municipalitiesResponse.json();
    populateMunicipalitySelect();

    // Enable compare button when both are selected
    votingSelect.addEventListener('change', checkSelections);
    municipalitySelect.addEventListener('change', checkSelections);
    compareBtn.addEventListener('click', compareResults);
  } catch (error) {
    console.error('Error initializing app:', error);
    alert('Error loading data. Please check the server.');
  }
}

function populateVotingSelect() {
  votings.forEach(voting => {
    const option = document.createElement('option');
    option.value = voting.voting_date;
    const date = formatDate(voting.voting_date);
    option.textContent = `${date} (${voting.proposal_count} proposals)`;
    votingSelect.appendChild(option);
  });
}

function populateMunicipalitySelect() {
  municipalities.forEach(muni => {
    const option = document.createElement('option');
    option.value = muni.municipality_id;
    option.textContent = `${muni.municipality_name} (${muni.municipality_id}) - ${muni.change_count} mergers`;
    municipalitySelect.appendChild(option);
  });
}

function checkSelections() {
  const hasVoting = votingSelect.value !== '';
  const hasMunicipality = municipalitySelect.value !== '';
  compareBtn.disabled = !(hasVoting && hasMunicipality);
}

async function compareResults() {
  const votingDate = votingSelect.value;
  const municipalityId = municipalitySelect.value;

  if (!votingDate || !municipalityId) return;

  // Show loading
  loading.style.display = 'block';
  results.style.display = 'none';
  comparison.style.display = 'none';

  try {
    const response = await fetch(`/api/compare/${votingDate}/${municipalityId}`);
    currentComparison = await response.json();

    displayResults(currentComparison);
    displayComparison(currentComparison);
  } catch (error) {
    console.error('Error comparing results:', error);
    alert('Error loading comparison data.');
  } finally {
    loading.style.display = 'none';
  }
}

function displayResults(data) {
  // Display aggregated results (left panel)
  const aggContent = document.getElementById('agg-content');
  const aggMunicipality = document.getElementById('agg-municipality');

  if (data.aggregated.length > 0) {
    const firstResult = data.aggregated[0];
    aggMunicipality.textContent = `${firstResult.municipality_name} (${firstResult.municipality_id})`;

    // Add source information if this is an aggregated municipality
    if (firstResult.source_municipality_count > 1) {
      aggMunicipality.innerHTML += `<br><small style="color: #666;">Aggregated from ${firstResult.source_municipality_count} municipalities: ${firstResult.source_bfs_numbers}</small>`;
    }

    let html = '<table class="results-table">';
    html += '<thead><tr><th>Proposal</th><th>Ja</th><th>Nein</th><th>Gültig</th></tr></thead>';
    html += '<tbody>';

    data.aggregated.forEach(result => {
      html += `<tr>
        <td class="proposal-title">${truncateText(result.title_de, 60)}</td>
        <td class="number">${formatNumber(result.ja_stimmen_absolut)}</td>
        <td class="number">${formatNumber(result.nein_stimmen_absolut)}</td>
        <td class="number">${formatNumber(result.gueltige_stimmen)}</td>
      </tr>`;
    });

    html += '</tbody></table>';
    aggContent.innerHTML = html;
  } else {
    aggContent.innerHTML = '<p class="warning">⚠️ No aggregated results found in database.</p>';
  }

  // Display merger information (middle panel)
  const mergerContent = document.getElementById('merger-content');
  const changes = data.original.changes || [];

  if (changes.length > 0) {
    let html = '<div class="merger-timeline">';
    html += `<h3>All Merger Changes (${changes.length} total):</h3>`;
    html += '<ul>';

    changes.forEach(change => {
      html += `<li>
        <strong>${change.predecessor_name}</strong> (${change.predecessor_id})
        <br>
        <span class="merger-date">→ Merged on ${formatDate(change.mutation_date)}</span>
        <br>
        <span class="merger-type">${change.mutation_type}</span>
      </li>`;
    });

    html += '</ul></div>';
    mergerContent.innerHTML = html;
  } else {
    mergerContent.innerHTML = '<p class="info">ℹ️ No merger changes found for this municipality.</p>';
  }

  // Display original results (right panel)
  const origContent = document.getElementById('orig-content');
  const originalResults = data.original.results || [];

  if (originalResults.length > 0) {
    let html = '';

    originalResults.forEach(proposal => {
      html += `<div class="proposal-section">`;
      html += `<h3>${truncateText(proposal.title_de, 60)}</h3>`;

      if (proposal.municipalities.length > 0) {
        html += '<table class="results-table">';
        html += '<thead><tr><th>Municipality</th><th>Ja</th><th>Nein</th><th>Gültig</th></tr></thead>';
        html += '<tbody>';

        // Calculate totals
        let totalJa = 0;
        let totalNein = 0;
        let totalGueltig = 0;

        proposal.municipalities.forEach(muni => {
          html += `<tr>
            <td><strong>${muni.municipality_name}</strong> (${muni.municipality_id})</td>
            <td class="number">${formatNumber(muni.ja_stimmen_absolut)}</td>
            <td class="number">${formatNumber(muni.nein_stimmen_absolut)}</td>
            <td class="number">${formatNumber(muni.gueltige_stimmen)}</td>
          </tr>`;

          totalJa += muni.ja_stimmen_absolut;
          totalNein += muni.nein_stimmen_absolut;
          totalGueltig += muni.gueltige_stimmen;
        });

        // Add total row if multiple municipalities
        if (proposal.municipalities.length > 1) {
          html += `<tr class="total-row">
            <td><strong>TOTAL</strong></td>
            <td class="number"><strong>${formatNumber(totalJa)}</strong></td>
            <td class="number"><strong>${formatNumber(totalNein)}</strong></td>
            <td class="number"><strong>${formatNumber(totalGueltig)}</strong></td>
          </tr>`;
        }

        html += '</tbody></table>';
      } else {
        html += '<p class="warning">⚠️ No data found in JSON for this municipality.</p>';
      }

      html += '</div>';
    });

    origContent.innerHTML = html;
  } else {
    origContent.innerHTML = '<p class="warning">⚠️ No original results found in JSON files.</p>';
  }

  results.style.display = 'grid';
}

function displayComparison(data) {
  const comparisonContent = document.getElementById('comparison-content');
  let html = '<div class="validation-results">';

  let allMatch = true;
  const mismatches = [];

  // Compare each proposal
  data.aggregated.forEach((aggResult, idx) => {
    const origProposal = data.original.results[idx];

    if (origProposal && origProposal.municipalities.length > 0) {
      // Calculate original totals
      const origTotalJa = origProposal.municipalities.reduce((sum, m) => sum + m.ja_stimmen_absolut, 0);
      const origTotalNein = origProposal.municipalities.reduce((sum, m) => sum + m.nein_stimmen_absolut, 0);
      const origTotalGueltig = origProposal.municipalities.reduce((sum, m) => sum + m.gueltige_stimmen, 0);

      const jaMatch = aggResult.ja_stimmen_absolut === origTotalJa;
      const neinMatch = aggResult.nein_stimmen_absolut === origTotalNein;
      const gueltigMatch = aggResult.gueltige_stimmen === origTotalGueltig;

      const proposalMatch = jaMatch && neinMatch && gueltigMatch;

      if (!proposalMatch) {
        allMatch = false;
        mismatches.push({
          title: aggResult.title_de,
          aggJa: aggResult.ja_stimmen_absolut,
          origJa: origTotalJa,
          aggNein: aggResult.nein_stimmen_absolut,
          origNein: origTotalNein,
          aggGueltig: aggResult.gueltige_stimmen,
          origGueltig: origTotalGueltig
        });
      }
    }
  });

  if (allMatch && data.aggregated.length > 0 && data.original.results.length > 0) {
    html += '<div class="success-message">';
    html += '<h3>✅ Perfect Match!</h3>';
    html += `<p>All ${data.aggregated.length} proposals match exactly between aggregated and original data.</p>`;
    html += '</div>';
  } else if (mismatches.length > 0) {
    html += '<div class="error-message">';
    html += '<h3>❌ Mismatches Found</h3>';
    html += '<p>The following proposals have different values:</p>';

    mismatches.forEach(mm => {
      html += `<div class="mismatch-detail">`;
      html += `<strong>${truncateText(mm.title, 60)}</strong><br>`;
      html += `<table class="comparison-table">`;
      html += `<tr><th></th><th>Aggregated (DB)</th><th>Original (JSON)</th><th>Difference</th></tr>`;
      html += `<tr><td>Ja</td><td>${formatNumber(mm.aggJa)}</td><td>${formatNumber(mm.origJa)}</td><td>${mm.aggJa - mm.origJa}</td></tr>`;
      html += `<tr><td>Nein</td><td>${formatNumber(mm.aggNein)}</td><td>${formatNumber(mm.origNein)}</td><td>${mm.aggNein - mm.origNein}</td></tr>`;
      html += `<tr><td>Gültig</td><td>${formatNumber(mm.aggGueltig)}</td><td>${formatNumber(mm.origGueltig)}</td><td>${mm.aggGueltig - mm.origGueltig}</td></tr>`;
      html += `</table>`;
      html += `</div>`;
    });

    html += '</div>';
  } else {
    html += '<div class="info-message">';
    html += '<p>ℹ️ No comparison data available. Check if both aggregated and original results exist.</p>';
    html += '</div>';
  }

  html += '</div>';
  comparisonContent.innerHTML = html;
  comparison.style.display = 'block';
}

// Utility functions
function formatDate(dateStr) {
  // Format: 20190519 -> 2019-05-19
  if (!dateStr || dateStr.length !== 8) return dateStr;
  return `${dateStr.substr(0, 4)}-${dateStr.substr(4, 2)}-${dateStr.substr(6, 2)}`;
}

function formatNumber(num) {
  if (num === null || num === undefined) return '-';
  return num.toLocaleString('de-CH');
}

function truncateText(text, maxLength) {
  if (!text || text.length <= maxLength) return text;
  return text.substr(0, maxLength) + '...';
}

// Initialize when DOM is ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', init);
} else {
  init();
}
