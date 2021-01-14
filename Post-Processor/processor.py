import json, os
import re
import csv 
import ast
import itertools
"""Loads the domain output json into a dictionary"""
def load_json():
    # used to parse domain files in a folder called results
    print("Loading domain files")
    path_to_json = './Results/'
    all_data = {}
    pairings = {}
    for file_name in [file for file in os.listdir(path_to_json) if file.endswith('.json')]:
        with open(path_to_json + file_name) as json_file:
            data = json.load(json_file)
            data['id'] = os.path.splitext(file_name)[0]
            all_data[data['url']] = data
            pairings[data['id']] = {'url': data['url'], 'domain': data['domain']}
    return all_data, pairings

"""Loads the twitter output csv into a dictionary"""
def load_twitter_csv():
    print("Loading twitter files")
    data = {}
    pairings = {}
    path = './TwitterOutput/'
    for file_name in [file for file in os.listdir(path) if file.endswith('.csv')]:
        with open(path + file_name, mode='r', encoding='utf-8-sig') as csv_file:
            for line in csv.DictReader(csv_file):
                if not(line['Found URL'] and line['Hashtags'] and line['Mentions']):
                    print(line)
                    continue
                else:
                    lst = ast.literal_eval(line['Found URL'])
                    hashtags = ast.literal_eval(line['Hashtags'])
                    mentions = ast.literal_eval(line['Mentions']) 
                pairings[line['Hit Record Unique ID']] = {'url': line['URL to article/Tweet'], 'domain': line['Source']}
                data[line['URL to article/Tweet']] = {'url': line['URL to article/Tweet'], 'id': line['Hit Record Unique ID'], 'domain': line['Source'], 
                'Location': line['Location'], 'Hit Type': line['Hit Type'], 'Tags': line['Passed through tags'], 
                'Associated Publisher' : line['Associated Publisher'], 'author_metadata': line['Authors'], 'article_text': line['Plain Text of Article or Tweet'],
                'date': line['Date'], 'Mentions': mentions, 'Hashtags': hashtags, 'found_urls': lst}
    return data, pairings

"""Loads the scope csv into a dictionary."""
def load_scope(file):
    print("Loading scope")
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
        aliases = [source]
        if info['aliases']:
            aliases += info['aliases'] 
        if info['twitter_handles']:
            aliases += info['twitter_handles']
        pattern = r"(\W|^)(%s)(\W|$)" % "|".join(aliases)
        if re.search(pattern, sequence, re.IGNORECASE):
            found_aliases.append(source)
    # find all twitter handles in the text 
    pattern = r"(?<=^|(?<=[^a-zA-Z0-9-_\.]))(@[A-Za-z]+[A-Za-z0-9-_]+)"
    twitters = re.findall(pattern, sequence, re.IGNORECASE)
    # check these twitter handles arent in the scope
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
    print("Processing Twitter")
    referrals = {}
    num_processed = 0
    number_of_articles = len(data)
    for node in data:
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
        num_processed += 1
        print("Processed "+ str(num_processed)+ "/"+ str(number_of_articles)+ " twitter articles")

    print("Finished processing twitter")
    return referrals

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
    print("Processing Domain")
    referrals = {}
    num_processed = 0
    number_of_articles = len(data)
    for node in data:
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
        num_processed += 1
        print("Processed "+ str(num_processed)+ "/"+ str(number_of_articles)+ " domain articles")
    print("Finished Processing Domain")
    return referrals
"""
Creates the data format for the output json
"""
def create_output(article, referrals, scope, output, interest_output, domain_pairs, twitter_pairs):
    
    if article["domain"] in scope.keys():
        output[article['id']] = {'id': article['id'], 
                    'url':article['url'], 
                    'source': article['url'], 
                    'name': scope[article["domain"]]['Name'], 
                    'tags':scope[article["domain"]]['Tags'], 
                    'publisher':scope[article["domain"]]['Publisher'], 
                    'referring record id':referrals, 
                    'authors': article['author_metadata'], 
                    'plain text':article['article_text'], 
                    'date of publication':article['date'], 
                    'image reference':'', 
                    'anchor text':'', 
                    'language':''}
    else: 
        cited = {}
        for ref_id in referrals:
            if ref_id in domain_pairs:
                try:
                    cited[domain_pairs[ref_id]['domain']] += 1
                except:
                    cited[domain_pairs[ref_id]['domain']] =1
            else:
                try:
                    cited[twitter_pairs[ref_id]['domain']] += 1
                except:
                    cited[twitter_pairs[ref_id]['domain']] =1
        # sort top referring domains by number of hits descending
        top = dict(sorted(cited.items(), key=lambda item: item[1], reverse=True))
        
        interest_output[article['id']] = {'id': article['id'], 'hit count': len(referrals), 'url':article['url'], 'source': article['url'], 
                    'name': '', 'tags':[], 'publisher':'', 'referring record id':referrals, 
                    'authors': article['author_metadata'], 
                    'plain text':article['article_text'], 
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
    
    # get all referrals for this domain (by text alias or twitter handle)
    if article['domain'] in domain_referrals:
        referring_articles += domain_referrals[article['domain']]
    
    # get all referrals for this url (who is referring to me)
    if str(article['url']) in twitter_referrals:
        referring_articles += twitter_referrals[str(article['url'])]

    # get all referrals for this domain (by text alias or twitter handle)
    if article['domain'] in twitter_referrals:
        referring_articles += twitter_referrals[article['domain']]

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
def process_crawler(domain_data, twitter_data, scope, domain_pairs, twitter_pairs):
    output = {}
    interest_output = {}
    # get a dictionary of all the referrals for each source
    domain_referrals = process_domain(domain_data, scope)
    twitter_referrals = process_twitter(twitter_data, scope)
    
    # cross match between domain and twitter data and creat the output dictionary
    print("Parsing domain output file")
    for node in domain_data:
        referring_articles = parse_referrals(domain_data[node], domain_referrals, twitter_referrals)
        create_output(domain_data[node], referring_articles, scope, output, interest_output, domain_pairs, twitter_pairs)
    print("Parsing twitter output file")
    for node in twitter_data:
        referring_articles = parse_referrals(twitter_data[node], domain_referrals, twitter_referrals)
        create_output(twitter_data[node], referring_articles, scope, output, interest_output, domain_pairs, twitter_pairs)
    
    print("Serializing and writing final json output")
    # Serializing json    
    json_object = json.dumps(output, indent = 4)  

    # Writing to output.json 
    with open("output.json", "w") as outfile: 
        outfile.write(json_object) 

    # Sorts interest output 
    interest_output = dict(sorted(interest_output.items(), key=lambda item: item[1]['hit count'], reverse=True))
    # Serializing json    
    json_object = json.dumps(interest_output, indent = 4)  

    # Writing to output.json 
    with open("interest_output.json", "w") as outfile: 
        outfile.write(json_object) 

scope = load_scope('./input_scope_final.csv')
twitter_data, twitter_pairs = load_twitter_csv()
domain_data, domain_pairs = load_json()
process_crawler(domain_data, twitter_data, scope, domain_pairs, twitter_pairs)