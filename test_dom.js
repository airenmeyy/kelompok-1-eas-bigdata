const fs = require('fs');
const jsdom = require('jsdom');
const { JSDOM } = jsdom;

const html = fs.readFileSync('dashboard/index.html', 'utf8');

const virtualConsole = new jsdom.VirtualConsole();
virtualConsole.on('error', (err) => {
    console.log('--- ERROR ---');
    console.log(err);
});
virtualConsole.on('warn', (warn) => {
    console.log('--- WARN ---');
    console.log(warn);
});
virtualConsole.on('log', (log) => {
    console.log('--- LOG ---');
    console.log(log);
});

const dom = new JSDOM(html, {
    url: 'http://localhost/dashboard/',
    runScripts: 'dangerously',
    resources: 'usable',
    virtualConsole
});

setTimeout(() => {
    console.log('Timeout reached, exiting');
    process.exit(0);
}, 3000);
