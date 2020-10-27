import json

def load_json(file):
    with open(file) as f:
        data = json.load(f)
    return data

def process_domain_crawler(data):
    id = 1
    output = {}
    links = {}
    for node in data:
        # TODO: Check against scope data to find articles not in the scope
        # found_urls in data are the articles that this node is talking about 
        # 'referring record id' is which records are referring to this? aka who is talking about me
        
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

data = load_json('./link_title_list.json')
process_domain_crawler(data)
