# Metscraper
This tool can be used to populate the `date` fields of JSONs produced by the domain crawler.

## Usage
First install the required packages if not already installed, `npm i metascraper metascraper-author metascraper-date metascraper-title got`
To run, use `python3 getDates.py /path/to/jsons/` where `/path/to/jsons/` is the path to the directory of JSONs whose dates will be populated. The resulting updated JSONs will be located in the `DatedOutput` directory with the same original name. 
