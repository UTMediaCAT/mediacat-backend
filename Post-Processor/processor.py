import json
import re
import csv 

"""Loads the domain output json into a dictionary"""
def load_json(file):
    # used to parse domain output file
    with open(file) as f:
        data = json.load(f)
    return data

"""Loads the twitter output csv into a dictionary"""
def load_twitter_csv(file):
    data = {}
    #format: {url: data}
    with open(file) as csv_file:
        for line in csv.DictReader(csv_file): 
            # parse string as a list
            urls = line['Found URL'][2:-2]
            lst = list(urls.split("', '")) 

            data[line['URL to article/Tweet']] = {'url': line['URL to article/Tweet'], 'id': line['Hit Record Unique ID'], 'Source': line['Source'], 
            'Location': line['Location'], 'Name': line['Name'], 'Hit Type': line['Hit Type'], 'Tags': line['Passed through tags'], 
            'Associated Publisher' : line['Associated Publisher'], 'Authors': line['Authors'], 'article_text': line['Plain Text of Article or Tweet'],
            'Date': line['Date'], 'Mentions': line['Mentions'], 'Hashtags': line['Hashtags'], 'found_urls': lst}
    json_object = json.dumps(data, indent = 4)  

    # Writing to sample_twitter.json (just to visualize the twitter dictionary)
    with open("sample_twitter.json", "w") as outfile: 
        outfile.write(json_object) 
    return data 

"""Loads the scope csv into a dictionary."""
def load_scope(file):
    # parse all the text aliases from each source using the scope file
    scope = {}
    #format: {source: {aliases: [], twitter_handles:[]}}
    with open(file) as csv_file:
        for line in csv.DictReader(csv_file): 
            # TODO: Find expected out put to chan together the list of aliases
            aliases = line['Text Aliases'].split('|')
            twitter = line['Associated Twitter Handle'].split('|')
            scope[line['Source']] = {'aliases': aliases, 'twitter_handles': twitter}
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
        aliases = info['aliases'] + info['twitter_handles'] + [source]
        pattern = r"(\W|^)(%s)(\W|$)" % "|".join(aliases)
        if re.search(pattern, sequence, re.IGNORECASE):
            found_aliases.append(source)
    # find all twitter handles in the text 
    pattern = r"(?<=^|(?<=[^a-zA-Z0-9-_\.]))(@[A-Za-z]+[A-Za-z0-9-_]+)"
    twitters = re.findall(pattern, sequence, re.IGNORECASE)
    # check these twitter handles arent in the scope
    t = [item for item in twitters if item not in found_aliases]
    random_tweets = [item for item in t if item not in scope.keys()]
    print(found_aliases)
    # print(random_tweets)
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
def process_twitter(data, scope, output, interest_output):
    print('NOW PROCESSING TWITTER!')
    for node in data:
        found_aliases, twitter_handles = find_aliases(data, node, scope)
        links = {}
        # each key in links is an article url, and it has a list of article ids that are talking about it
        for link in data[node]['found_urls']:
            
            # if this article is already in output dictionary, then append this node's id
            if link[0] in output:
                output[link[0]]['referring record id'].append(id)
            else: 
                # otherwise save all future links/referrals in links, 
                # where the key is each link in 'found_urls' and the value is this node
                if link[0] in links:
                    links[link[0]].append(id)
                else:
                    links[link[0]] = [id] 
        
                referrals = []
        # get all referrals for this url (who is referring to me)
        if data[node]['url'] in links:
            referrals = links[data[node]['url']]

        output[node] = {'id': data[node]['id'], 'url':data[node]['url'], 'source': data[node]['url'], 
                        'name': '', 'tags':[], 'publisher':'', 'referring record id':referrals, 
                        'authors': '', 
                        'plain text':data[node]['article_text'], 
                        'date of publication':'', 
                        'image reference':'', 
                        'anchor text':'', 
                        'language':''}

"""
Processes the domain data by finding all the articles that are referring to it
and mutating the output dictionary. 
Parameters: 
    data: the domain output dictionary
    scope: the scope dictionary
    output: the output dictionary to add to 
    interest_output: the of interest dictionary of articles not in the scope
"""
def process_domain(data, scope, output, interest_output):
    id = 1
    inScope = True
    for node in data:
        # TODO: Check against scope data to find articles not in the scope
        # if not data[node]['url'] in scope.keys():
        #     inScope = False

        # found_urls in data are the articles that this node is talking about 
        # 'referring record id' is which records are referring to this? aka who is talking about me

        # TODO: Take found_aliases and complete matching on the correct nodes
        found_aliases, twitter_handles = find_aliases(data, node, scope)
        links = {}
        # each key in links is an article url, and it has a list of article ids that are talking about it
        for link in data[node]['found_urls']:
            
            # if this article is already in output dictionary, then append this node's id
            if link[0] in output:
                output[link[0]]['referring record id'].append(id)
            else: 
                # otherwise save all future links/referrals in links, 
                # where the key is each link in 'found_urls' and the value is this node
                if link[0] in links:
                    links[link[0]].append(id)
                else:
                    links[link[0]] = [id] 

        # looks for sources in found aliases, and adds it to the linking
        for source in found_aliases:
            if source in links:
                links[source].append(id)
            else:
                links[source] = [id] 

        referrals = []
        # get all referrals for this url (who is referring to me)
        if data[node]['url'] in links:
            referrals += links[data[node]['url']]
        # get all referrals for this domain (by text alias or twitter handle)
        # TODO: Bug: always has itself as a referring
        # TODO: Bug: articles found earlier were not properly matched (for domain)
        if data[node]['domain'] in links:
            referrals += links[data[node]['domain']]
        if inScope:
            output[node] = {'id': id, 'url':data[node]['url'], 'source': data[node]['url'], 
                        'name': '', 'tags':[], 'publisher':'', 'referring record id':referrals, 
                        'authors': data[node]['author_metadata'], 
                        'plain text':data[node]['article_text'], 
                        'date of publication':data[node]['date'], 
                        'image reference':'', 
                        'anchor text':'', 
                        'language':''}
        else: 
            interest_output[node] = {}
        id+=1

"""
The main point of entry for the processor. Calls the domain and twitter 
processor seperately.
Parameters:
    domain_data: domain dictionary
    twitter_data: twitter dictionary
    scope: scope dictionary

Outputs the post processing data into output.json
"""
def process_crawler(domain_data, twitter_data, scope):
    output = {}
    interest_output = {}
    process_domain(domain_data, scope, output, interest_output)
    process_twitter(twitter_data, scope, output, interest_output)
    
    # Serializing json    
    json_object = json.dumps(output, indent = 4)  

    # Writing to output.json 
    with open("output.json", "w") as outfile: 
        outfile.write(json_object) 

scope = load_scope('./scope.csv')
twitter_data = load_twitter_csv('./twitter_output.csv')
domain_data = load_json('./domain_output.json')
process_crawler(domain_data, twitter_data, scope)
