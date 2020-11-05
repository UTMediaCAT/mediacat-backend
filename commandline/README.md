## Prerequisite

Make sure you have the following:

* npm
* Python 3
* Node.js
* pip3

Clone the following repos

```
https://github.com/UTMediaCAT/mediacat-backend

https://github.com/UTMediaCAT/mediacat-twitter-crawler

https://github.com/UTMediaCAT/mediacat-domain-crawler
```

* in the same directory as app.js make sure there is a folder named ```csv```


## Execute

1. 
Go to the directory location of the [domain crawler script](https://github.com/UTMediaCAT/mediacat-domain-crawler/tree/master/newCrawler)

run the command `npm install` to install the dependencies

2.
Go to the directory location of the [twitter crawler script](https://github.com/UTMediaCAT/mediacat-twitter-crawler)

- run the command `pip3 install --user --upgrade git+https://github.com/twintproject/twint.git@origin/master#egg=twint` to install twint

- run the command `pip3 install pandas` to install pandas

3. Go to the `mediacat-domain-crawler` and `mediacat-twitter-crawler` repos and follow any additional instructions that may need to be done. This will probably be more updated frequently.


In the app.js file notice the following constants:


```
PATH_SCOPE_PARSER
PATH_INPUT_CSV
PATH_TWITTER_CRAWLER
PATH_DOMAIN_CRAWLER
FAILED_DOMAIN_LINKS
VALID_DOMAIN_LINKS
domaincsvFile
twittercsvFile
```

- `PATH_SCOPE_PARSER` set to the path of the script that calls the main parser
- `PATH_INPUT_CSV` set to the path of the input scope csv
- `PATH_TWITTER_CRAWLER` set to the path of the script that calls the twitter crawler
- `PATH_DOMAIN_CRAWLER` set to the path of the script that calls the domain crawler
- `FAILED_DOMAIN_LINKS`set to the path of the failed links outputted by the domain crawler
- `VALID_DOMAIN_LINKS` set to the path of the valid links outputted by the domain crawler
- `domaincsvFile` set to the path of the domain csv outputted by the scope parser
- `twittercsvFile` set to the path of the twitter csv outputted by the scope parser

4. run the command `npm install` in this folder `mediacat-backend/commandline`

5. run the commmand `screen -S crawl` to get on a screen session

6. run the command `node app.js` in this folder `mediacat-backend/commandline`

7. run the command `control + a + d` to detach from the screen

If you wanted to retach from the screen, you can do `screen -r crawl` or to see the number of screens you can do `screen -ls` . 

To learn more [read here](https://linuxize.com/post/how-to-use-linux-screen/)
