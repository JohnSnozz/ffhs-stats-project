import { Database } from "bun:sqlite";
import { join } from "path";
import { writeFileSync, appendFileSync, existsSync, mkdirSync } from "fs";

const db = new Database("../data/swiss_votings.db", { readonly: true });
const VOTES_DIR = "../data/votes";
const LOGS_DIR = join(import.meta.dir, "logs");

// Ensure logs directory exists
if (!existsSync(LOGS_DIR)) {
  mkdirSync(LOGS_DIR, { recursive: true });
}

// Get current timestamp for log file
const logTimestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, -5);
const mainLogFile = join(LOGS_DIR, `validation_server_${logTimestamp}.log`);
const requestLogFile = join(LOGS_DIR, `requests_${logTimestamp}.log`);
const comparisonLogFile = join(LOGS_DIR, `comparisons_${logTimestamp}.log`);

// Logger utility
function log(message: string, logFile: string = mainLogFile) {
  const timestamp = new Date().toISOString();
  const logMessage = `[${timestamp}] ${message}\n`;
  console.log(logMessage.trim());
  appendFileSync(logFile, logMessage);
}

function logComparison(votingDate: string, municipalityId: string, result: any) {
  const timestamp = new Date().toISOString();
  const logEntry = {
    timestamp,
    voting_date: votingDate,
    municipality_id: municipalityId,
    municipality_name: result.aggregated[0]?.municipality_name || 'Unknown',
    aggregated_count: result.aggregated.length,
    original_count: result.original.results.length,
    changes_count: result.original.changes.length,
    source_municipalities: result.aggregated[0]?.source_bfs_numbers || 'N/A',
    validation: validateResults(result)
  };
  appendFileSync(comparisonLogFile, JSON.stringify(logEntry, null, 2) + '\n');
}

function validateResults(result: any) {
  if (!result.aggregated.length || !result.original.results.length) {
    return { status: 'NO_DATA', matches: 0, mismatches: 0 };
  }

  let matches = 0;
  let mismatches = 0;

  result.aggregated.forEach((agg: any, idx: number) => {
    const orig = result.original.results[idx];
    if (orig) {
      const origJa = orig.municipalities.reduce((sum: number, m: any) => sum + m.ja_stimmen_absolut, 0);
      const origNein = orig.municipalities.reduce((sum: number, m: any) => sum + m.nein_stimmen_absolut, 0);
      const origGueltig = orig.municipalities.reduce((sum: number, m: any) => sum + m.gueltige_stimmen, 0);

      if (agg.ja_stimmen_absolut === origJa &&
          agg.nein_stimmen_absolut === origNein &&
          agg.gueltige_stimmen === origGueltig) {
        matches++;
      } else {
        mismatches++;
      }
    }
  });

  return {
    status: mismatches === 0 ? 'PERFECT_MATCH' : 'MISMATCH',
    matches,
    mismatches
  };
}

// Helper to get voting JSON file path from date
function getVotingJsonPath(votingDate: string): string {
  // Format: sd-t-17-02-20190519-eidgAbstimmung.json
  return join(import.meta.dir, VOTES_DIR, `sd-t-17-02-${votingDate}-eidgAbstimmung.json`);
}

// API: Get all votings
function getVotings() {
  log('Fetching all votings from database');
  const query = db.query(`
    SELECT
      v.voting_date,
      COUNT(DISTINCT p.proposal_id) as proposal_count,
      GROUP_CONCAT(p.title_de, ' | ') as proposals
    FROM votings v
    JOIN proposals p ON v.voting_id = p.voting_id
    GROUP BY v.voting_date
    ORDER BY v.voting_date DESC
  `);
  const results = query.all();
  log(`Retrieved ${results.length} votings`);
  return results;
}

// API: Get municipalities that had changes
function getMunicipalitiesWithChanges() {
  log('Fetching municipalities with merger changes');
  const query = db.query(`
    SELECT
      mc.new_bfs_number as municipality_id,
      mc.new_name as municipality_name,
      COUNT(*) as change_count
    FROM municipal_changes mc
    WHERE mc.is_merger = 1
    GROUP BY mc.new_bfs_number, mc.new_name
    ORDER BY change_count DESC, mc.new_name ASC
  `);
  const results = query.all();
  log(`Retrieved ${results.length} municipalities with mergers`);
  return results;
}

// API: Get aggregated results from database (using analysis view)
function getAggregatedResults(votingDate: string, municipalityId: string) {
  log(`Fetching aggregated results: voting=${votingDate}, municipality=${municipalityId}`);
  const query = db.query(`
    SELECT
      municipality_id,
      municipality_name,
      proposal_id,
      title_de,
      ja_stimmen_absolut,
      nein_stimmen_absolut,
      gueltige_stimmen,
      stimmbeteiligung,
      source_municipality_count,
      source_bfs_numbers
    FROM v_voting_results_analysis
    WHERE voting_date = ?
      AND municipality_id = ?
    ORDER BY proposal_id
  `);
  const results = query.all(votingDate, municipalityId);
  log(`Retrieved ${results.length} aggregated proposals for ${municipalityId}`);
  if (results.length > 0) {
    log(`  Source municipalities: ${results[0].source_bfs_numbers}`);
    log(`  Source count: ${results[0].source_municipality_count}`);
  }
  return results;
}

// API: Get original results from JSON file
async function getOriginalResults(votingDate: string, municipalityId: string) {
  try {
    const filePath = getVotingJsonPath(votingDate);
    const file = Bun.file(filePath);
    const data = await file.json();

    const results: any[] = [];

    // Navigate through the JSON structure: schweiz -> vorlagen -> kantone -> gemeinden
    if (!data.schweiz?.vorlagen) return results;

    for (const vorlage of data.schweiz.vorlagen) {
      // Search all cantons for the municipality
      let foundResults = null;

      for (const kanton of vorlage.kantone || []) {
        for (const gemeinde of kanton.gemeinden || []) {
          if (gemeinde.geoLevelnummer === municipalityId) {
            foundResults = {
              municipality_id: gemeinde.geoLevelnummer,
              municipality_name: gemeinde.geoLevelname,
              vorlage_id: vorlage.vorlagenId,
              title_de: vorlage.vorlagenTitel.find((t: any) => t.langKey === 'de')?.text || '',
              ja_stimmen_absolut: gemeinde.resultat?.jaStimmenAbsolut || 0,
              nein_stimmen_absolut: gemeinde.resultat?.neinStimmenAbsolut || 0,
              gueltige_stimmen: gemeinde.resultat?.gueltigeStimmen || 0,
              stimmbeteiligung_prozent: gemeinde.resultat?.stimmbeteiligungInProzent || 0
            };
            break;
          }
        }
        if (foundResults) break;
      }

      if (foundResults) {
        results.push(foundResults);
      }
    }

    return results;
  } catch (error) {
    console.error('Error reading JSON file:', error);
    return [];
  }
}

// API: Get ALL changes for a municipality (not filtered by date)
function getAllMunicipalityChanges(municipalityId: string) {
  log(`Fetching all changes for municipality ${municipalityId}`);
  const query = db.query(`
    SELECT
      mc.old_bfs_number as predecessor_id,
      mc.old_name as predecessor_name,
      mc.new_bfs_number as current_id,
      mc.mutation_date,
      mc.mutation_type
    FROM municipal_changes mc
    WHERE mc.new_bfs_number = ?
      AND mc.is_merger = 1
    ORDER BY mc.mutation_date ASC
  `);
  const results = query.all(municipalityId);
  log(`Found ${results.length} merger changes for ${municipalityId}`);
  return results;
}

// API: Get source municipalities from analysis view for this voting
function getSourceMunicipalities(votingDate: string, municipalityId: string) {
  log(`Getting source municipalities for ${municipalityId} on ${votingDate}`);
  const query = db.query(`
    SELECT DISTINCT source_bfs_numbers
    FROM v_voting_results_analysis
    WHERE voting_date = ?
      AND municipality_id = ?
    LIMIT 1
  `);
  const result = query.get(votingDate, municipalityId) as any;
  if (result && result.source_bfs_numbers) {
    const sources = result.source_bfs_numbers.split(',');
    log(`  Source municipalities: ${sources.join(', ')}`);
    return sources;
  }
  log(`  No sources found, using municipality itself: ${municipalityId}`);
  return [municipalityId]; // Fallback to just the municipality itself
}

// API: Get all predecessor results from JSON
async function getAllPredecessorResults(votingDate: string, municipalityId: string) {
  try {
    const filePath = getVotingJsonPath(votingDate);
    log(`Reading original JSON file: ${filePath}`);
    const file = Bun.file(filePath);
    const data = await file.json();

    // Get ALL changes for this municipality (for middle panel display)
    const allChanges = getAllMunicipalityChanges(municipalityId);

    // Get the actual source municipalities from the analysis view for this voting
    const sourceMunicipalityIds = getSourceMunicipalities(votingDate, municipalityId);

    const results: any[] = [];

    if (!data.schweiz?.vorlagen) {
      log('ERROR: No voting data found in JSON');
      return { changes: allChanges, results };
    }

    log(`Processing ${data.schweiz.vorlagen.length} proposals from JSON`);

    for (const vorlage of data.schweiz.vorlagen) {
      const proposalResults: any = {
        vorlage_id: vorlage.vorlagenId,
        title_de: vorlage.vorlagenTitel.find((t: any) => t.langKey === 'de')?.text || '',
        municipalities: []
      };

      // Search all cantons for source municipalities
      for (const kanton of vorlage.kantone || []) {
        for (const gemeinde of kanton.gemeinden || []) {
          if (sourceMunicipalityIds.includes(gemeinde.geoLevelnummer)) {
            const muniData = {
              municipality_id: gemeinde.geoLevelnummer,
              municipality_name: gemeinde.geoLevelname,
              ja_stimmen_absolut: gemeinde.resultat?.jaStimmenAbsolut || 0,
              nein_stimmen_absolut: gemeinde.resultat?.neinStimmenAbsolut || 0,
              gueltige_stimmen: gemeinde.resultat?.gueltigeStimmen || 0,
              stimmbeteiligung_prozent: gemeinde.resultat?.stimmbeteiligungInProzent || 0
            };
            proposalResults.municipalities.push(muniData);
            log(`  Found ${gemeinde.geoLevelname} (${gemeinde.geoLevelnummer}): ${muniData.ja_stimmen_absolut} Ja, ${muniData.nein_stimmen_absolut} Nein`);
          }
        }
      }

      results.push(proposalResults);
    }

    log(`Retrieved ${results.length} proposals with original data`);
    return { changes: allChanges, results };
  } catch (error) {
    log(`ERROR reading JSON file: ${error}`);
    console.error('Error reading JSON file:', error);
    return { changes: [], results: [] };
  }
}

// Start the server
const server = Bun.serve({
  port: 3000,
  async fetch(req) {
    const url = new URL(req.url);
    const logMsg = `${req.method} ${url.pathname}`;
    log(logMsg, requestLogFile);

    // Serve static files
    if (url.pathname === "/" || url.pathname === "/index.html") {
      return new Response(Bun.file(join(import.meta.dir, "public", "index.html")));
    }
    if (url.pathname === "/app.js") {
      return new Response(Bun.file(join(import.meta.dir, "public", "app.js")));
    }
    if (url.pathname === "/style.css") {
      return new Response(Bun.file(join(import.meta.dir, "public", "style.css")));
    }

    // API endpoints
    if (url.pathname === "/api/votings") {
      log('API: /api/votings called', requestLogFile);
      const votings = getVotings();
      return Response.json(votings);
    }

    if (url.pathname === "/api/municipalities") {
      log('API: /api/municipalities called', requestLogFile);
      const municipalities = getMunicipalitiesWithChanges();
      return Response.json(municipalities);
    }

    if (url.pathname.startsWith("/api/compare/")) {
      // Format: /api/compare/{votingDate}/{municipalityId}
      const parts = url.pathname.split("/");
      const votingDate = parts[3];
      const municipalityId = parts[4];

      if (!votingDate || !municipalityId) {
        log(`API ERROR: Missing parameters for /api/compare/`, requestLogFile);
        return Response.json({ error: "Missing parameters" }, { status: 400 });
      }

      log(`API: /api/compare/${votingDate}/${municipalityId} called`, requestLogFile);
      log(`\n=== COMPARISON REQUEST: ${votingDate} / ${municipalityId} ===`);

      const aggregated = getAggregatedResults(votingDate, municipalityId);
      const original = await getAllPredecessorResults(votingDate, municipalityId);

      const result = {
        voting_date: votingDate,
        municipality_id: municipalityId,
        aggregated,
        original
      };

      // Log detailed comparison
      logComparison(votingDate, municipalityId, result);

      log(`=== COMPARISON COMPLETE ===\n`);
      return Response.json(result);
    }

    log(`404: ${url.pathname}`, requestLogFile);
    return new Response("Not found", { status: 404 });
  },
});

// Startup logging
log('==========================================================');
log('VALIDATION SERVER STARTING');
log('==========================================================');
log(`Server URL: http://localhost:${server.port}`);
log(`Database: ${db.filename}`);
log(`JSON files directory: ${join(import.meta.dir, VOTES_DIR)}`);
log(`Log files directory: ${LOGS_DIR}`);
log(`  - Main log: ${mainLogFile}`);
log(`  - Request log: ${requestLogFile}`);
log(`  - Comparison log: ${comparisonLogFile}`);
log('==========================================================');

console.log(`🚀 Validation server running at http://localhost:${server.port}`);
console.log(`📊 Database: ${db.filename}`);
console.log(`📁 JSON files: ${join(import.meta.dir, VOTES_DIR)}`);
console.log(`📝 Logs: ${LOGS_DIR}`);
