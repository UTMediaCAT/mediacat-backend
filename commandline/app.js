/**
 * To use this, 
 * npm install first
 * do node app.js
 * Also make sure that the crawlers are install correctly
 * twint has to be on the right branch with pip
 * npm install for the domain crawler
 * needs to have directory ./csv/
 * input the path to the scope parser up top
 * input the path to the scope input csv up top as well
 */


/**
 * Step Two: feed to the crawlers
 */

const utf8 = require('utf8'); 
const fs = require('fs');
const csvParser = require('csv-parser');



const PATH_SCOPE_PARSER='../../mediacat-frontend/scope_parser/main.py'
const PATH_INPUT_CSV='../../mediacat-frontend/scope_parser/csv/test_demo.csv'

const PATH_TWITTER_CRAWLER='../../mediacat-twitter-crawler/twitter_crawler.py'
const PATH_DOMAIN_CRAWLER='../../mediacat-domain-crawler/newCrawler/crawl.js'


const FAILED_DOMAIN_LINKS='./failed_links_list.json'
const VALID_DOMAIN_LINKS='./link_title_list.json'

const domaincsvFile = './domain.csv'
const twittercsvFile = './twitter.csv'

const childProcess = require("child_process");


function app() {

  stepOne(stepTwo)

}

function stepTwo() {
  stepTwoTwitter()
  stepTwoDomain()
}

function stepOne(next) {
  /**
   * Step one: feed in the input files into scope parser
   */

  const pythonProcess = childProcess.spawnSync('python3',[PATH_SCOPE_PARSER, PATH_INPUT_CSV]);

  // pythonProcess.stderr.on('err', (err)=> {
  //   console.log("error!");
  //   console.log(err);
  // });

  // pythonProcess.stdout.on('data', (data) => {
  //   console.log("finished scope parsing");
  //   // two files would be generated called domain.csv and twitter.csv
  // });

  console.log(pythonProcess.stderr.toString())
  console.log(pythonProcess.stdout.toString())

  next()
}

/**
 * Step Two: feed to the crawlers
 * fs.open( filename, flags, mode, callback )
 */

function stepTwoTwitter() {




  try {
      if (fs.existsSync(domaincsvFile)) {
        console.log('File %s exists!', domaincsvFile)
      }
    } catch(err) {
      console.error(err)
  }


  try {
      if (fs.existsSync(twittercsvFile)) {
          console.log('File %s exists!', twittercsvFile)
        //file exists
      }
    } catch(err) {
      console.error(err)
  }

  /**
   * Twitter crawler:
   * install twint on the correct branch version
   * : pip3 install --user --upgrade git+https://github.com/twintproject/twint.git@origin/master#egg=twint
   */

  // const pythonProcess2 = childProcess.spawn('python3',[PATH_TWITTER_CRAWLER, twittercsvFile]);
  // const pythonProcess3 = childProcess.exec( `python3 ${PATH_TWITTER_CRAWLER} ${twittercsvFile}`, callbackAfterDomainCrawler)
  
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

}


/**
 * Read csv of domains and call the domain crawler
 * @param {*} file 
 */
function readDomainInput(file, callback, callback2) {
  const results = [];
  fs.createReadStream(file)
  .pipe(csvParser())
  .on('data', (data) => results.push(data))
  .on('end', () => {
     return (callback(results, callback2));
  });
}


/**
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

}

function callbackAfterTwitterCrawler() {
  console.log("finished crawling twitter")
  console.log("all twitter csv 's should be ready")

};


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

function stepTwoDomain() {
  readDomainInput(domaincsvFile, callDomainCrawler, callbackAfterDomainCrawler)
}

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





if (require.main === module) {
  app();
}