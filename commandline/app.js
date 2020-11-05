const fs = require('fs');
const csvParser = require('csv-parser');
const childProcess = require('child_process');

const PATH_SCOPE_PARSER='../../mediacat-frontend/scope_parser/main.py';
const PATH_INPUT_CSV='../../mediacat-frontend/scope_parser/csv/test_demo.csv';

const PATH_TWITTER_CRAWLER='../../mediacat-twitter-crawler/twitter_crawler.py';
const PATH_DOMAIN_CRAWLER='../../mediacat-domain-crawler/newCrawler/crawl.js';

const FAILED_DOMAIN_LINKS='./failed_links_list.json';
const VALID_DOMAIN_LINKS='./link_title_list.json';

const domaincsvFile = './domain.csv';
const twittercsvFile = './twitter.csv';

/**
 * calls the scope parser, then the two crawlers
 */
function app() {
  stepOne(stepTwo);
}

/**
 * calls the scope parser, then the twitter crawler
 */
function appTwitter() {
  stepOne(stepTwoTwitter);
}

/**
 * calls the scope parser, then the app crawler
 */
function appDomain() {
  stepOne(stepTwoDomain);
}

/**************************************************/

function stepOne(next) {
  /**
   * Step one: feed in the input files into scope parser
   */

  try {
    const pythonProcess = childProcess.spawnSync('python3',[PATH_SCOPE_PARSER, PATH_INPUT_CSV]);
    try {
      console.log(pythonProcess.stderr.toString())
      console.log(pythonProcess.stdout.toString())
    } catch (err) {
        console.error(err)
    }
    next()
  } catch (err) {
      console.error(err)
      process.exit(1);
    }
}

/**
 * Step Two: feed to the crawlers
 */

 /**
 * helper function that calls the two crawlers
 * is used by stepOne
 */

function stepTwo() {
  // twitter crawler
  stepTwoTwitter()

  // domain crawler
  stepTwoDomain()
}

/*******************Twitter*******************************/

/**
 * calls the twitter crawler
 */

function stepTwoTwitter() {
  try {
      if (fs.existsSync(twittercsvFile)) {
          console.log('File %s exists!', twittercsvFile)
        //file exists
      }
    } catch(err) {
      console.error(err)
      process.exit(1);
  }

  /**
   * Twitter crawler:
   * install twint on the correct branch version
   * : pip3 install --user --upgrade git+https://github.com/twintproject/twint.git@origin/master#egg=twint
   */
  
  try {
      const pythonProcess2 = childProcess.spawn( `python3 ${PATH_TWITTER_CRAWLER} ${twittercsvFile}`, {
        shell: true
      })

      pythonProcess2.on('close', () => {
        callbackAfterTwitterCrawler()
      })
    
      pythonProcess2.stderr.on('data', function (data) {
        console.error("STDERR:", data.toString());
      });

      pythonProcess2.stdout.on('data', function (data) {
        console.log("STDOUT:", data.toString());
      });

      pythonProcess2.on('exit', function (exitCode) {
        console.log("Child exited with code: " + exitCode);
      });

  } catch (err) {
      console.error(err)
  }
}

function callbackAfterTwitterCrawler() {
  console.log("finished crawling twitter")
  console.log("all twitter csv 's should be ready")

};

/*******************Twitter*******************************/

/*******************Domain*******************************/

function stepTwoDomain() {

  try {
    if (fs.existsSync(domaincsvFile)) {
      console.log('File %s exists!', domaincsvFile)
    }
  } catch(err) {
    console.error(err)
    process.exit(1);
  }

  readDomainInput(domaincsvFile, callDomainCrawler, callbackAfterDomainCrawler)
}

/**
 * Read csv of domains and call the domain crawler
 * @param {*} file 
 */
function readDomainInput(file, callback, callback2) {
  const results = [];
  try {
    fs.createReadStream(file)
    .pipe(csvParser())
    .on('data', (data) => results.push(data))
    .on('end', () => {
      return (callback(results, callback2));
    });
  } catch (err) {
    console.log(err)
  }
}

/**
 * calls the domain crawler
 * feed the urls to the domain crawler,
 * make sure to `npm install`
 * @param {*} domainlist 
 */

function callDomainCrawler(domainlist, callback) {
  //let crawler = require(PATH_DOMAIN_CRAWLER)
  let i;
  let list = ['-l']
  for (i = 0; i < domainlist.length; i++) {
    let domain = domainlist[i];
    let source = domain.Source
    list.push(source)
  }

  console.log(list)
  const command = PATH_DOMAIN_CRAWLER

  try {
    const domainCrawlProcess = childProcess.fork(command, list)

    // listen for errors as they may prevent the exit event from firing
    domainCrawlProcess.on('error', function (err) {
        console.log(err);
    });

    // execute the callback once the process has finished running
    domainCrawlProcess.on('exit', function (code) {
        var err = code === 0 ? null : new Error('exit code ' + code);
        if (err) {
          console.log(err);
        }
        callback();
    });
  } catch (err) {
    console.log(err);
  }

}

function callbackAfterDomainCrawler() {
  console.log("finished crawling domains")
  console.log("all domain json 's should be ready")
  console.log("domain crawl is complete")

  try {
    if (fs.existsSync(FAILED_DOMAIN_LINKS)) {
       console.log('File %s exists!', FAILED_DOMAIN_LINKS)
    }
  } catch(err) {
    console.error(err)
  }

  try {
      if (fs.existsSync(VALID_DOMAIN_LINKS)) {
          console.log('File %s exists!', VALID_DOMAIN_LINKS)
        //file exists
      }
    } catch(err) {
      console.error(err)
  }

};

/*******************Domain*******************************/
/*******************Post*******************************/


/**
 * Take the output of the crawlers and 
 * stuff it into date processor
 */


/**
 * Step Four: Take the output of the date processor and 
 * stuff it into post processor
 */



/**
 * Call back to post processing after crawler is done
 * 
 */
function callToPostProcessing(){
  console.log("call to post processing")
 }

/*******************Post*******************************/

/**
 * uncomment commands as needed
 */
if (require.main === module) {
  // app();
  // appDomain();
  appTwitter();
}
