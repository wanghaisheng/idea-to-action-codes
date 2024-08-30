// worker.js

const API_ENDPOINT = 'https://your-serverless-function-url'; // URL of your serverless function

async function fetchFromDatabase(query) {
    const response = await fetch(`${API_ENDPOINT}/query`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({ query }),
    });
    return response.json();
}

async function updateDatabase(domain, title, description) {
    const response = await fetch(`${API_ENDPOINT}/update`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({
            domain,
            title,
            description,
        }),
    });
    return response.json();
}

async function fetchDomainContent(domain) {
    const url = `https://${domain}`;
    const response = await fetch(url);
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

async function handleRequest(request) {
    // Query to get domains where title is empty
    const query = `SELECT url FROM domains WHERE title = '' LIMIT 100`;
    const { results } = await fetchFromDatabase(query);

    for (const domain of results) {
        const domainUrl = domain.url;
        try {
            const { title, description } = await fetchDomainContent(domainUrl);
            await updateDatabase(domainUrl, title, description);
        } catch (error) {
            console.error(`Failed to process ${domainUrl}:`, error);
        }
    }
    return new Response('Done processing domains.', { status: 200 });
}

addEventListener('fetch', event => {
    event.respondWith(handleRequest(event.request));
});
