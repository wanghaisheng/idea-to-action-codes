import { JSDOM } from 'jsdom'; // Install jsdom using npm install jsdom
import { getUndoneDomains, insertOrUpdateData } from './dbhelper.js';

// Configuration for Cloudflare Worker environment
const database = 'd1'; // or 'mysql'
const apiUrl = 'https://www.google.com/search?q=About+';

function parseDomainData(html, domain) {
    try {
        const dom = new JSDOM(html);
        const document = dom.window.document;

        // Extracting the index date
        const indexdateElement = Array.from(document.querySelectorAll('div')).find(el => el.textContent.includes("Site first indexed by Google"));
        let indexdate = "unk";
        if (indexdateElement) {
            const indexdateText = indexdateElement.textContent;
            const indexdateMatch = indexdateText.split("Site first indexed by Google").pop();
            if (indexdateMatch && !indexdateMatch.endsWith("ago")) {
                indexdate = indexdateMatch.split("ago")[0] + "ago";
            }
        }

        // Extracting About the Source and In Their Own Words
        let aboutthesource = "";
        let intheirownwords = "";
        const aboutSourceElements = Array.from(document.querySelectorAll('div')).filter(el => el.textContent.includes("Web results about the source"));
        if (aboutSourceElements.length > 0) {
            const aboutSourceText = aboutSourceElements[0].textContent;
            const [aboutText, inTheirOwnWordsText] = aboutSourceText.split("In their own words");
            if (aboutText) {
                aboutthesource = aboutText.split("About the source").pop().trim();
            }
            if (inTheirOwnWordsText) {
                intheirownwords = inTheirOwnWordsText.replace(/\r|\n/g, "").trim();
            }
        }

        return {
            domain,
            indexdate,
            Aboutthesource: aboutthesource,
            Intheirownwords: intheirownwords
        };
    } catch (error) {
        console.error(`Error parsing data for domain ${domain}:`, error);
        return null;
    }
}

async function fetchData(url) {
    const response = await fetch(url);
    if (response.ok) {
        return await response.text();
    } else {
        console.error(`Failed to fetch data from ${url}. Status: ${response.status}`);
        return null;
    }
}

async function processDomains() {
    const domains = await getUndoneDomains(database);
    for (const domainRecord of domains) {
        const domain = domainRecord.domain;
        const url = `${apiUrl}${domain}&tbm=ilp&sa=X&ved=2ahUKEwj30IH2gZuHAxUPAkQIHb8sA_sQv5AHegQIABAD`;

        const html = await fetchData(url);
        if (html) {
            const domainData = parseDomainData(html, domain);
            if (domainData) {
                await insertOrUpdateData(database, domainData);
            }
        }
    }
}

// Main handler for the Cloudflare Worker
addEventListener('fetch', event => {
    event.respondWith(handleRequest(event.request));
});

async function handleRequest(request) {
    try {
        await processDomains();
        return new Response('Domains processed successfully.', { status: 200 });
    } catch (error) {
        console.error('Error processing domains:', error);
        return new Response('Failed to process domains.', { status: 500 });
    }
}
