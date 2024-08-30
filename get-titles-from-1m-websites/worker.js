// worker.js

const DB_TABLE = 'domains';
const D1_DATABASE_ID = 'your_database_id';
const API_TOKEN = 'your_cloudflare_api_token';

const proxy = 'https://your-proxy-url.com'; // Set your proxy if needed

async function fetchFromD1(query) {
    const url = `https://api.cloudflare.com/client/v4/accounts/your_account_id/d1/databases/${D1_DATABASE_ID}/query`;
    const response = await fetch(url, {
        method: 'POST',
        headers: {
            'Authorization': `Bearer ${API_TOKEN}`,
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(query),
    });
    return response.json();
}

async function fetchDomainContent(domain) {
    const url = `https://${domain}`;
    const response = await fetch(url, {
        headers: {
            'Proxy': proxy, // Use proxy if needed
        }
    });

    if (!response.ok) throw new Error(`Failed to fetch ${domain}`);
    const html = await response.text();
    return {
        title: getTitleFromHtml(html),
        description: getDescriptionFromHtml(html),
    };
}

function getTitleFromHtml(html) {
    const match = /<title>(.*?)<\/title>/i.exec(html);
    return match ? match[1].trim() : 'not content!';
}

function getDescriptionFromHtml(html) {
    const match = /<meta name="description" content="(.*?)"/i.exec(html);
    return match ? match[1].trim() : 'No description found';
}

async function updateDomain(domain, title, description) {
    const query = {
        query: `UPDATE ${DB_TABLE} SET title = ?, description = ? WHERE url = ?`,
        values: [title, description, domain],
    };
    await fetchFromD1(query);
}

async function handleRequest(request) {
    // Query to get domains where title is empty
    const query = {
        query: `SELECT url FROM ${DB_TABLE} WHERE title = '' LIMIT 100`,
    };
    const data = await fetchFromD1(query);
    const domains = data.results;

    for (const domain of domains) {
        const domainUrl = domain.url;
        try {
            const { title, description } = await fetchDomainContent(domainUrl);
            await updateDomain(domainUrl, title, description);
        } catch (error) {
            console.error(`Failed to process ${domainUrl}:`, error);
        }
    }
    return new Response('Done processing domains.', { status: 200 });
}

addEventListener('fetch', event => {
    event.respondWith(handleRequest(event.request));
});
