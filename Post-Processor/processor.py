import json, os
import re
import csv 
import ast
import itertools
import uuid
from timeit import default_timer as timer
import logging
import sys, traceback
logging.basicConfig(filename='./logs/processor.log', level=logging.DEBUG, filemode='w')

"""Loads the domain output json into a dictionary"""
def load_json():
    # used to parse domain files in a folder called results
    logging.info("Loading domain files")
    path_to_json = './DomainOutput/'
    all_data = {}
    pairings = {}
    for file_name in [file for file in os.listdir(path_to_json) if file.endswith('.json')]:
        with open(path_to_json + file_name) as json_file:
            data = json.load(json_file)
            data['id'] = os.path.splitext(file_name)[0]
            data['completed'] = False
            data["type"] = "article"
            data["language"] = ""
            all_data[data['url']] = data
            pairings[data['id']] = {'url': data['url'], 'domain': data['domain']}
    return all_data, pairings

"""Loads the twitter output csv into a dictionary"""
def load_twitter_csv():
    logging.info("Loading twitter files")
    data = {}
    pairings = {}
    path = './TwitterOutput/'
    for file_name in [file for file in os.listdir(path) if file.endswith('.csv')]:
        with open(path + file_name, mode='r', encoding='utf-8-sig') as csv_file:
            for line in csv.DictReader(csv_file):
                if not(line['Found URL'] and line['Hashtags'] and line['Mentions']):
                    print("Line format not properly formed :" + line)
                    logging.warning("Line format not properly formed :" + line)
                    continue
                else:
                    try:
                        lst = ast.literal_eval(line['Found URL'])
                        hashtags = ast.literal_eval(line['Hashtags'])
                        mentions = ast.literal_eval(line['Mentions']) 
                    except:
                        logging.warning("Line format not properly formed :" + line)
                pairings[line['Hit Record Unique ID']] = {'url': line['URL to article/Tweet'], 'domain': line['Source']}
                data[line['URL to article/Tweet']] = {'url': line['URL to article/Tweet'], 'id': line['Hit Record Unique ID'], 'domain': line['Source'], 
                'Location': line['Location'], 'type': "twitter", 'Tags': line['Passed through tags'], "language": "",
                'Associated Publisher' : line['Associated Publisher'], 'author_metadata': line['Authors'], 'article_text': line['Plain Text of Article or Tweet'],
                'date': line['Date'], 'Mentions': mentions, 'Hashtags': hashtags, 'found_urls': lst, 'completed': False}
    return data, pairings

"""Loads the scope csv into a dictionary."""
def load_scope(file):
    logging.info("Loading scope")
    # parse all the text aliases from each source using the scope file
    scope = {}
    #format: {source: {aliases: [], twitter_handles:[]}}
    with open(file) as csv_file:
        for line in csv.DictReader(csv_file): 
            aliases,twitter,tags = [], [], []
            if line['Text Aliases'] != " " and line['Text Aliases']:
                aliases = line['Text Aliases'].split('|')
            if line['Associated Twitter Handle'] != " " and line['Associated Twitter Handle']:
                twitter = line['Associated Twitter Handle'].split('|')
            if line['Tags'] != " " and line['Tags']:
                tags = line['Tags'].split('|')
            scope[line['Source']] = {'Name': line['Name'],
                                    'RSS': line['RSS feed URLs (where available)'],
                                    'Type': line['Type'],
                                    'Publisher': line['Associated Publisher'],
                                    'Tags': tags,
                                    'aliases': aliases, 
                                    'twitter_handles': twitter} 
    write_to_file(scope, "scope.json")
    return scope 

"""
Finds all the text aliases and twitter handles in this node's text.
Returns a list of sources in scope that have been mentioned in this node's text, as well as
a list of twitter handles found that are not in the scope.
Parameters:
    data: the data dictionary
    node: the node in the dictionary that we are searching on
    scope: the scope dictionary
Returns:
    Returns 2 lists in a tuple:
    found aliases is a list of all the sources that this article node refers to
    random tweets is a list of all twitter handles found that are not in the scope
"""
# finds all the aliases that are in this node's text
# returns a list of urls that it is referring to
def find_aliases(data, node, scope):
    found_aliases = []
    twitters = []
    sequence = data[node]['article_text']
    for source, info in scope.items():
        if data[node]['domain'] == source:
            continue
        src = [source]
        pattern = r"(\W|^)(%s)(\W|$)" % "|".join(src)
        if re.search(pattern, sequence, re.IGNORECASE):
            found_aliases.append(source)

        if info['aliases']:
            aliases = info['aliases'] 
            pattern = r"(?!@)(\W|^)(%s)(\W|$)" % "|".join(aliases)
            if re.search(pattern, sequence, re.IGNORECASE):  
                found_aliases.append(str(aliases))

        if info['twitter_handles']:
            handles = info['twitter_handles']
            for handle in handles:
                pattern = r"(\W|^)(%s)(\W|$)" % "|".join(handle)
                if re.search(pattern, sequence, re.IGNORECASE):
                    found_aliases += handle

    # find all twitter handles in the text 
    pattern = r"(?<=^|(?<=[^a-zA-Z0-9-_\.]))(@[A-Za-z]+[A-Za-z0-9-_]+)"
    twitters = re.findall(pattern, sequence, re.IGNORECASE)
    # check these twitter handles arent in the scope, and save them to random_tweets
    t = [item for item in twitters if item not in found_aliases]
    random_tweets = [item for item in t if item not in scope.keys()]

    return found_aliases, random_tweets

"""
Processes the twitter data by finding all the articles that are referring to it
and mutating the output dictionary. 
Parameters: 
    data: the twitter output dictionary
    scope: the scope dictionary
    output: the output dictionary to add to 
    interest_output: the of interest dictionary of articles not in the scope
"""
def process_twitter(data, scope):
    logging.info("Processing Twitter")
    try:
        start = timer()
        referrals = {}
        num_processed = 0
        number_of_articles = len(data)
        for node in data:
            if data[node]['completed']:
                continue
            found_aliases, twitter_handles = find_aliases(data, node, scope)
            # each key in links is an article url, and it has a list of article ids that are talking about it
            for link in data[node]['found_urls']:
                if link in referrals:
                    referrals[link].append(data[node]['id'])
                else:
                    referrals[link] = [data[node]['id']] 
            # looks for sources in found aliases, and adds it to the linking
            for source in found_aliases:
                if source in referrals:
                    referrals[source].append(data[node]['id'])
                else:
                    referrals[source] = [data[node]['id']]

            data[node]['completed'] = True
            num_processed += 1
            logging.info("Processed "+ str(num_processed)+ "/"+ str(number_of_articles)+ " twitter articles")
        end = timer()
        logging.info("Finished processing twitter - Took " + str(end - start) + " seconds")
    except:
        logging.warning('Exception at Processing Twitter, data written to Saved/')
        exc_type, exc_value, exc_traceback = sys.exc_info()
        logging.error(exc_value)
        logging.error(exc_type)
        write_to_file(referrals, "Saved/twitter_referrals.json")
        write_to_file(data, "Saved/twitter_data.json")
        raise
    return data, referrals

"""
Processes the domain data by finding all the articles that it is referring to and 
articles that are referring to it and mutating the output dictionary. 
Parameters: 
    data: the domain output dictionary
    scope: the scope dictionary
    output: the output dictionary to add to 
    interest_output: the of interest dictionary of articles not in the scope
"""
def process_domain(data, scope):
    logging.info("Processing Domain")
    try:
        start = timer()
        referrals = {}
        num_processed = 0
        number_of_articles = len(data)
        for node in data:
            if data[node]['completed']:
                continue
            found_aliases, twitter_handles = find_aliases(data, node, scope)
            # each key in links is an article url, and it has a list of article ids that are talking about it
            for link in data[node]['found_urls']:
                # save all referrals where the key is each link in 'found_urls' and the value is this article's id
                if link['url'] in referrals:
                    referrals[link['url']].append(data[node]['id'])
                else:
                    referrals[link['url']] = [data[node]['id']] 

            # looks for sources in found aliases, and adds it to the linking
            for source in found_aliases:
                if source in referrals:
                    referrals[source].append(data[node]['id'])
                else:
                    referrals[source] = [data[node]['id']]
            data[node]['completed'] = True
            num_processed += 1

            logging.info("Processed "+ str(num_processed)+ "/"+ str(number_of_articles)+ " domain articles")
        end = timer()
        logging.info("Finished Processing Domain - Took " + str(end - start) + " seconds")
    except:
        logging.warning('Exception at Processing Domain, data written to Saved/')
        exc_type, exc_value, exc_traceback = sys.exc_info()
        logging.error(exc_value)
        logging.error(exc_type)
        write_to_file(referrals, "Saved/domain_referrals.json")
        write_to_file(data, "Saved/domain_data.json")
        raise
    return data, referrals
"""
Creates the data format for the output json
"""
def create_output(article, referrals, scope, output, interest_output, domain_pairs, twitter_pairs):
    
    if article["domain"] in scope.keys():
        # article is in scope

        if article['type'] in ["domain", "twitter handle", "text aliases"] and len(referrals) == 0:
            # does not include these static nodes in output if referral count is 0.
            pass
        output[article['id']] = {'id': article['id'], 
                    'url or alias text':article['url'], 
                    'referring record id':referrals,
                    'number of referrals': len(referrals),
                    'type': article['type'],
                    'associated publisher':scope[article["domain"]]['Publisher'], 
                    'tags':scope[article["domain"]]['Tags'], 
                    'name/title': scope[article["domain"]]['Name'], 
                    'language':article["language"],
                    'authors': article['author_metadata'], 
                    'date of publication':article['date'], 
                    'plain text':article['article_text'], 
                    'image reference':'', 
                    'anchor text':'', 
                }
    else: 
        # article is not in scope
        cited = {}
        for ref_id in referrals:
            if ref_id in domain_pairs:
                try:
                    cited[domain_pairs[ref_id]['domain']] += 1
                except:
                    cited[domain_pairs[ref_id]['domain']] = 1
            else:
                try:
                    cited[twitter_pairs[ref_id]['domain']] += 1
                except:
                    cited[twitter_pairs[ref_id]['domain']] = 1
        # sort top referring domains by number of hits descending
        top = dict(sorted(cited.items(), key=lambda item: item[1], reverse=True))
        
        interest_output[article['id']] = {'id': article['id'], 
                    'hit count': len(referrals), 
                    'url or alias text':article['url'], 
                    'source': article['domain'], 
                    'name': '', 
                    'tags':[], 
                    'publisher':'', 
                    'referring record id':referrals, 
                    'authors': article['author_metadata'], 
                    'plain text':article['article_text'], 
                    'type': article['type'],
                    'date of publication':article['date'], 
                    'top referrals': dict(itertools.islice(top.items(), 5))} 
                    # top referrals gets the top 5 domains that referred to this article the most
"""
Cross-match the referrals for domain referrals and twitter referrals.
Returns a list of all sources that are referring to this specific article.
Params:
    article: an article in the data
    domain_referrals: a dictionary of all the referrals in the domain data
    twitter_referrals: a dictionary of all the referrals in the twitter data
"""
def parse_referrals(article, domain_referrals, twitter_referrals):

    referring_articles = []
    # get all referrals for this url (who is referring to me)
    if article['url'] in domain_referrals:
        referring_articles += domain_referrals[article['url']]

    if str(article['url']) in twitter_referrals:
        referring_articles += twitter_referrals[str(article['url'])]

    # remove duplicates from list
    referring_articles = list(dict.fromkeys(referring_articles))
    # remove itself from list
    if article['id'] in referring_articles: referring_articles.remove(article['id'])
    return referring_articles

"""
The main point of entry for the processor. Calls the domain and twitter 
processor seperately.
Parameters:
    domain_data: domain dictionary
    twitter_data: twitter dictionary
    scope: scope dictionary

Outputs the post processing data into output.json
"""
def process_crawler(domain_data, twitter_data, scope, domain_pairs, twitter_pairs, saved_domain_referrals = {}, saved_twitter_referrals = {}):
    output = {}
    interest_output = {}
    # get a dictionary of all the referrals for each source
    domain_data, domain_referrals = (process_domain(domain_data, scope))
    # save referrals to file in case of break
    write_to_file(domain_data, "Saved/domain_data.json")
    if saved_domain_referrals != {}:
        new = {key: value + saved_domain_referrals[key] for key, value in domain_referrals.items()}
        saved_domain_referrals.update(new)
        domain_referrals = saved_domain_referrals

    write_to_file(domain_referrals, "Saved/domain_referrals.json")

    twitter_data, twitter_referrals = (process_twitter(twitter_data, scope))
    # save referrals to file in case of break
    write_to_file(twitter_data, "Saved/twitter_data.json")
    if saved_twitter_referrals != {}:
        new = {key: value + saved_twitter_referrals[key] for key, value in twitter_referrals.items()}
        saved_twitter_referrals.update(new)
        twitter_referrals = saved_twitter_referrals

    write_to_file(twitter_referrals, "Saved/twitter_referrals.json")

     # create static nodes
    nodes = create_static_nodes(scope)
    # add these nodes to domain_data, prioritize domain_data if duplications
    nodes.update(domain_data)
    domain_data = nodes
    # cross match between domain and twitter data and creat the output dictionary
    start = timer()
    for node in domain_data:
        referring_articles = parse_referrals(domain_data[node], domain_referrals, twitter_referrals)
        create_output(domain_data[node], referring_articles, scope, output, interest_output, domain_pairs, twitter_pairs)
    end = timer()
    logging.info("Parsing domain output file - Took " + str(end - start) + " seconds")

    start = timer()
    for node in twitter_data:
        referring_articles = parse_referrals(twitter_data[node], domain_referrals, twitter_referrals)
        create_output(twitter_data[node], referring_articles, scope, output, interest_output, domain_pairs, twitter_pairs)
    end = timer()
    logging.info("Parsing twitter output file - Took " + str(end - start) + " seconds")
    logging.info("Output "+ str(len(output)) + " articles in scope and "+ str(len(interest_output)) + " articles in interest scope")
    start = timer()

    write_to_file(output, "Output/output.json")
    # Sorts interest output 
    interest_output = dict(sorted(interest_output.items(), key=lambda item: item[1]['hit count'], reverse=True))
    write_to_file(interest_output, "Output/interest_output.json")
    
    end = timer()
    logging.info("Serializing and writing final json output - Took " + str(end - start) + " seconds")

def write_to_file(dict, filename):
    # Serializing json    
    json_object = json.dumps(dict, indent = 4)  

    # Writing to output.json 
    with open(filename, "w") as outfile: 
        outfile.write(json_object) 

def load_saved_files():
    with open('Saved/domain_data.json') as json_file: 
        domain_data = json.load(json_file) 
  
    with open('Saved/twitter_data.json') as json_file: 
        twitter_data = json.load(json_file) 

    with open('Saved/domain_referrals.json') as json_file: 
        domain_referrals = json.load(json_file) 
    with open('Saved/twitter_referrals.json') as json_file: 
        twitter_referrals = json.load(json_file) 
    return domain_data, twitter_data, domain_referrals, twitter_referrals

def create_static_nodes(scope):
    data = {}
    for source in scope:
        # create node for source (twitter handle or domain)
        if scope[source]["Type"] == "News Source":
            node_type = "domain"
        else:
            node_type = "twitter handle"
        uid = str(uuid.uuid5(uuid.NAMESPACE_DNS, source))
        data[source] = {'id': uid, 'url': source,
                    'domain': source, 
                    "date": "",
                    'type': node_type, 
                    'author_metadata': "", 
                    'article_text': "",
                    'language': "",
                    'completed': True}
        
        # create node for twitter handle, if exists
        for handle in scope[source]["twitter_handles"]:
            uid = str(uuid.uuid5(uuid.NAMESPACE_DNS, handle))
            data[handle] = {'id': uid, 'url': handle,
                        'domain': source, 
                        "date": "",
                        'type': "twitter handle", 
                        'author_metadata': "", 
                        'article_text': "",
                        'language': "",
                        'completed': True}
            
        # create node for text alias
        if scope[source]["aliases"]:
            alias = str(scope[source]["aliases"])
            uid = str(uuid.uuid5(uuid.NAMESPACE_DNS, alias))
            data[alias] = {'id': uid, 'url': alias,
                        'domain': source, 
                        "date": "",
                        'type': "text aliases", 
                        'author_metadata': "", 
                        'article_text': "",
                        'language': "",
                        'completed': True}

    return data


if __name__ == '__main__':
    load_saved_files()
    read_from_memory = False
    start = timer()
    scope_timer = timer()
    scope = load_scope('./input_scope_final.csv')
    scope_timer_end = timer()
    

    twitter_timer = timer()
    twitter_data, twitter_pairs = load_twitter_csv()
    twitter_timer_end = timer()
    domain_timer = timer()
    domain_data, domain_pairs = load_json()
    domain_timer_end = timer()
    if read_from_memory: 
        domain_data, twitter_data, domain_referrals, twitter_referrals = load_saved_files()
        process_crawler(domain_data, twitter_data, scope, domain_pairs, twitter_pairs, domain_referrals, twitter_referrals)
    else:   
        process_crawler(domain_data, twitter_data, scope, domain_pairs, twitter_pairs)
    end = timer()
    logging.info("Time to run whole post-processor took " + str(end - start) + " seconds") # Time in seconds
    logging.info("Time to read scope took " + str(scope_timer_end - scope_timer) + " seconds") # Time in seconds
    logging.info("Time to read twitter files took " + str(twitter_timer_end - twitter_timer) + " seconds") # Time in seconds
    logging.info("Time to read domain files took " + str(domain_timer_end - domain_timer) + " seconds") # Time in seconds
    logging.info("FINISHED")