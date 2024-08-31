// Helper function to perform HTTP requests to the SQLite-like cloud database
async function queryDatabase(query, params = []) {
  const response = await fetch('https://your-sqlite-cloud-db-endpoint', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': 'Bearer your-api-key' // Replace with your actual API key
    },
    body: JSON.stringify({ query, params })
  });
  
  if (!response.ok) throw new Error(`Database query failed: ${response.statusText}`);
  return response.json();
}

// Define functions
async function downloadCSV(url) {
  const response = await fetch(url);
  if (!response.ok) throw new Error('Failed to fetch CSV');
  return response.text();
}

async function insertData(csvText) {
  const lines = csvText.split('\n').slice(1); // Skip header
  const rows = lines.map(line => line.split(','));
  
  const data = rows.map(row => ({
    rank: parseInt(row[0]),
    domain: row[1]
  }));
  
  const updateDate = new Date().toISOString().split('T')[0];
  const version = `v${updateDate.replace(/-/g, '')}`;
  
  // Insert into UpdateHistory
  await queryDatabase('INSERT INTO update_history (update_date, version, description) VALUES (?, ?, ?)', [updateDate, version, 'Monthly import']);
  const result = await queryDatabase('SELECT id FROM update_history WHERE version = ?', [version]);
  const updateId = result[0].id;
  
  // Insert into TrancoDomain
  for (const item of data) {
    await queryDatabase('INSERT INTO tranco_domains (rank, domain, update_id) VALUES (?, ?, ?)', [item.rank, item.domain, updateId]);
  }
}

async function generateReports() {
  const today = new Date().toISOString().split('T')[0];
  
  const periods = {
    '1 month': [new Date(new Date().setDate(new Date().getDate() - 30)).toISOString().split('T')[0], today],
    '3 months': [new Date(new Date().setDate(new Date().getDate() - 90)).toISOString().split('T')[0], today],
    '6 months': [new Date(new Date().setDate(new Date().getDate() - 180)).toISOString().split('T')[0], today]
  };
  
  for (const [period, [startDate, endDate]] of Object.entries(periods)) {
    for (const topN of [100, 10000]) {
      const result = await queryDatabase(`
        SELECT domain, current_rank, previous_rank, (previous_rank - current_rank) AS rank_difference, current_version, previous_version
        FROM (
          SELECT domain, current_rank, current_version, latest_update_date
          FROM tranco_domains
          JOIN update_history ON tranco_domains.update_id = update_history.id
          WHERE rank <= ? AND update_date BETWEEN ? AND ?
        ) AS current
        LEFT JOIN (
          SELECT domain, current_rank AS previous_rank, current_version AS previous_version
          FROM tranco_domains
          JOIN update_history ON tranco_domains.update_id = update_history.id
          WHERE rank <= ? AND update_date < ?
        ) AS previous ON current.domain = previous.domain
        ORDER BY domain, current_version;
      `, [topN, startDate, endDate, topN, startDate]);
      
      const reportData = JSON.stringify(result);
      await queryDatabase('INSERT INTO rank_reports (period, report_date, data) VALUES (?, ?, ?)', [period, today, reportData]);
    }
  }
}

async function handleRequest() {
  try {
    const csvUrl = 'https://example.com/cloudflare_data.csv';
    const csvText = await downloadCSV(csvUrl);
    await insertData(csvText);
    await generateReports();
    
    return new Response('Reports generated successfully', { status: 200 });
  } catch (error) {
    return new Response(`Error: ${error.message}`, { status: 500 });
  }
}

// Cloudflare Worker Event Listener
addEventListener('fetch', event => {
  event.respondWith(handleRequest());
});
