const metascraper = require('metascraper')([
    require('metascraper-author')(),
    require('metascraper-date')(),
    //require('metascraper-description')(),
    //require('metascraper-image')(),
    //require('metascraper-logo')(),
    //require('metascraper-clearbit')(),
    //require('metascraper-publisher')(),
    require('metascraper-title')()
    //require('metascraper-url')()
    ])
    
    const got = require('got')
    
    const targetUrl = process.argv[2];
    ;(async () => {
    const { body: html, url } = await got(targetUrl)
    const metadata = await metascraper({ html, url })
    console.log(metadata['date'])
    console.log(metadata['author'])
    console.log(metadata['title'])
    })()
