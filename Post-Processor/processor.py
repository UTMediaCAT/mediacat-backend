import json
import re
import csv 

def load_json(file):
    with open(file) as f:
        data = json.load(f)
    return data

def load_csv(file):
    # parse all the text aliases from each source using the scope file
    scope = {}
    with open(file) as csv_file:
        for line in csv.DictReader(csv_file): 
            scope[line['Source']] = [line['Text aliases']]
    return scope 

# finds all the aliases that are in this node's text
# returns a list of urls that it is referring to
def find_aliases(data, node, scope):
    found_aliases = []
    # format: aliases = {'Source': [alias1, alias2]}
    sequence = data[node]['article_text']
    for source, aliases in scope.items():
        pattern = r"(\W|^)(%s)(\W|$)" % "|".join(aliases)
        if re.search(pattern, sequence, re.IGNORECASE):
            found_aliases.append(source)
    #         print("Match!" + source)
    # print(found_aliases)
    return found_aliases

def process_domain_crawler(data, regex_scope):
    id = 1
    output = {}
    links = {}
    for node in data:
        # TODO: Check against scope data to find articles not in the scope
        # found_urls in data are the articles that this node is talking about 
        # 'referring record id' is which records are referring to this? aka who is talking about me

        # TODO: Take found_aliases and complete matching on the correct nodes
        found_aliases = find_aliases(data, node, regex_scope)

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

        output[node] = {'id': id, 'url':data[node]['url'], 'source': data[node]['url'], 
                    'name': '', 'tags':[], 'publisher':'', 'referring record id':referrals, 
                    'authors': data[node]['author_metadata'], 
                    'plain text':data[node]['article_text'], 
                    'date of publication':data[node]['date'], 
                    'image reference':'', 
                    'anchor text':'', 
                    'language':''}
        id+=1

    # Serializing json    
    json_object = json.dumps(output, indent = 4)  

    # Writing to sample.json 
    with open("output.json", "w") as outfile: 
        outfile.write(json_object) 

scope = load_csv('./domain.csv')
data = load_json('./link_title_list.json')
process_domain_crawler(data, scope)
