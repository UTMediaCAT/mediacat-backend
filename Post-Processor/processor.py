"""
processor.py
Author: Amy Gao
Date: March 15, 2021
Description: This script takes crawler output files and then
creates links between articles using url, twitter handle and text aliases.
Returns a JSON file containing nodes for each article and
its corresponding information.
Usage: "python3 processor.py"
Output: link_title_list.json
"""
import tldextract
import json
import os
import re
import csv
import ast
import itertools
import uuid
from timeit import default_timer as timer
import logging
import sys
import gc
from multiprocessing import Process, Manager
import urlexpander
from urllib.parse import urlparse
logging.basicConfig(filename='./logs/processor.log', level=logging.DEBUG, filemode='w')  # nopep8
NUM_PROCS = -1
MEM_LIMIT = -1


def load_json():
    """Loads the domain output json from
    folder ./DomainOutput/ into a dictionary.
    Returns domain data dict and a dict of
    domain id to url and domain pairings."""
    # used to parse domain files in a folder called results
    logging.info("Loading domain files")
    path_to_json = './DomainOutput/'
    all_data = {}
    pairings = {}
    for file_name in [file for file in os.listdir(path_to_json) if file.endswith('.json')]:  # nopep8
        file_size = os.path.getsize(path_to_json + file_name)
        with open(path_to_json + file_name) as json_file:
            data = json.load(json_file)
            data['id'] = os.path.splitext(file_name)[0]
            data['completed'] = False
            data["type"] = "article"
            data["language"] = ""
            all_data[data['url']] = data
            pairings[data['id']] = {'url': data['url'], 'domain': data['domain']}  # nopep8
    return all_data, pairings

def convert_dict(original):
    key_dict = {'urls': 'Found URL', 'hashtags': 'Hashtags', 'link': 'URL to article/Tweet',
    'id': 'Hit Record Unique ID', 'username': 'Source', 'place': 'Location',
    'mentions': 'Mentions', 'tweet': 'Plain Text of Article or Tweet', 'date': 'Date',
    'language': 'Language'}
    new_dict = {}
    for key in original:
        if key in key_dict:
            new_dict[key_dict[key]] = original[key]
        else:
            new_dict[key] = original[key]
    new_dict['Hit Type'] = 'Twitter Hanlde'
    new_dict['Passed through tags'] = ''
    new_dict['Associated Publisher'] = ''
    new_dict['Authors'] = ''
    return new_dict


def load_twitter_csv():
    '''Loads the twitter output csv from
    folder ./TwitterOutput/ into a dictionary
    Returns twitter data dict and a dict of
    twitter id to url and domain pairings.'''
    logging.info("Loading twitter files")
    data = {}
    pairings = {}
    path = './TwitterOutput/'
    for file_name in [file for file in os.listdir(path) if file.endswith('.csv')]:  # nopep8
        with open(path + file_name, mode='r', encoding='utf-8-sig') as csv_file:  # nopep8
            for line in csv.DictReader(csv_file):
                line = convert_dict(line)
                if not(line['Found URL'] and line['Hashtags'] and line['Mentions']):  # nopep8
                    logging.warning("Line format not properly formed :" + line['URL to article/Tweet'] + line['Hit Record Unique ID'])  # nopep8
                    continue
                else:
                    try:
                        lst = ast.literal_eval(line['Found URL'])
                        hashtags = ast.literal_eval(line['Hashtags'])
                        mentions = ast.literal_eval(line['Mentions'])
                        cashtags = ast.literal_eval(line['cashtags'])
                        reply_to = ast.literal_eval(line['reply_to'])
                    except Exception:
                        logging.warning("Line format not properly formed :" + line['URL to article/Tweet'] + line['Hit Record Unique ID'])  # nopep8

                try:
                    language = line['Language']
                except Exception:
                    language = ""
                #pairings[line['Hit Record Unique ID']] = {'url': line['URL to article/Tweet'], 'domain': line['Source']}  # nopep8
                pairings[line['Hit Record Unique ID']] = {
                    'url': line['URL to article/Tweet'],
                    'domain': line['Source'], 'Location': line['Location'],
                    'type': "twitter",
                    'Tags': line['Passed through tags'],
                    "language": language,
                    'Associated Publisher': line['Associated Publisher'],
                    'author_metadata': line['Authors'],
                    'article_text': line['Plain Text of Article or Tweet'],
                    'date': line['Date'], 'Mentions': mentions,
                    'Hashtags': hashtags,
                    'found_urls': lst, 
                    'name': line['name'], 'reply_count': line['replies_count'], 
                    'retweet_count': line['retweets_count'], 'like_count': line['likes_count'],
                    'cashtags': cashtags, 'retweet': line['retweet'], 'video': line['video'],
                    'thumbnail': line['thumbnail'], 'reply_to': reply_to}
                data[line['URL to article/Tweet']] = {
                    'url': line['URL to article/Tweet'],
                    'id': line['Hit Record Unique ID'],
                    'domain': line['Source'], 'Location': line['Location'],
                    'type': "twitter",
                    'Tags': line['Passed through tags'],
                    "language": language,
                    'Associated Publisher': line['Associated Publisher'],
                    'author_metadata': line['Authors'],
                    'article_text': line['Plain Text of Article or Tweet'],
                    'date': line['Date'], 'Mentions': mentions,
                    'Hashtags': hashtags,
                    'found_urls': lst, 'completed': False}
    return data, pairings


def load_scope(file):
    """
    Loads the scope csv into a dictionary.
    Returns a dict of the scope with Source as key.
    """
    logging.info("Loading scope")
    # parse all the text aliases from each source using the scope file
    scope = {}
    # format: {source: {aliases: [], twitter_handles:[]}}
    with open(file) as csv_file:
        for line in csv.DictReader(csv_file):
            aliases, twitter, tags = [], [], []
            if line['Text Aliases'] != " " and line['Text Aliases']:
                aliases = line['Text Aliases'].split('|')
            if line['Associated Twitter Handle'] != " " and line['Associated Twitter Handle']:  # nopep8
                twitter = line['Associated Twitter Handle'].split('|')
            if line['Tags'] != " " and line['Tags']:
                tags = line['Tags'].split('|')
            scope[line['Source']] = {'Name': line['Name'],
                                     'RSS': line['RSS feed URLs (where available)'],  # nopep8
                                     'Type': line['Type'],
                                     'Publisher': line['Associated Publisher'],
                                     'Tags': tags,
                                     'aliases': aliases,
                                     'twitter_handles': twitter}
    write_to_file(scope, "scope.json")
    return scope


def find_aliases(data, node, scope):
    '''
    Finds all the text aliases and twitter handles in this node's text.
    Returns a list of sources in scope that have been mentioned
    in this node's text, as well as a list of twitter handles found
    that are not in the scope.
    Parameters:
        data: the data dictionary
        node: the node in the dictionary that we are searching on
        scope: the scope dictionary
    Returns 2 lists in a tuple:
        - found aliases is a list of all
            the sources that this article node refers to
        - random tweets is a list of all
            twitter handles found in the plain text that are not in the scope
    '''
    found_aliases = []
    twitters = []
    sequence = data[node]['article_text']
    for source, info in scope.items():
        if data[node]['domain'] == source:
            # ignore self-referrals
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
    # check these twitter handles arent in the scope,
    # and save them to random_tweets
    t = [item for item in twitters if item not in found_aliases]
    random_tweets = [item for item in t if item not in scope.keys()]

    return found_aliases, random_tweets


def process_twitter(data, scope):
    """
    Processes the twitter data by finding all the articles
    that are referring to it and mutating the output dictionary.
    Parameters:
        data: the twitter output dictionary
        scope: the scope dictionary
    Returns 2 dicts, one for the mutated data dictionary,
    and another dict of referrals.
    """
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
            # each key in links is an article url, and it has a list
            # of article ids that are talking about it
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
            logging.info("Processed " + str(num_processed) + "/" + str(number_of_articles) + " twitter articles")  # nopep8
        end = timer()
        logging.info("Finished processing twitter - Took " + str(end - start) + " seconds")  # nopep8
    except Exception:
        logging.warning('Exception at Processing Twitter, data written to Saved/')  # nopep8
        exc_type, exc_value, exc_traceback = sys.exc_info()
        logging.error(exc_value)
        logging.error(exc_type)
        write_to_file(referrals, "Saved/twitter_referrals.json")
        write_to_file(data, "Saved/twitter_data.json")
        raise
    return data, referrals


def multi_process_twitter(data, scope, assignemnts, referrals_shared=None):
    """
    Processes the twitter data using multi-processing by finding all the articles
    that are referring to it and mutating the output dictionary.
    Parameters:
        data: the twitter output dictionary
        scope: the scope dictionary
    Returns 2 dicts, one for the mutated data dictionary,
    and another dict of referrals.
    """
    logging.info("Processing Twitter")
    try:
        start = timer()
        referrals = {}
        num_processed = 0
        threshold = MEM_LIMIT
        number_of_articles = len(assignemnts)  # len(data)
        for node in assignemnts:  # data:
            if data[node]['completed']:
                continue
            found_aliases, twitter_handles = find_aliases(data, node, scope)
            # each key in links is an article url, and it has a list
            # of article ids that are talking about it
            for link in data[node]['found_urls']:
                if link in referrals:
                    referrals[link].append(data[node]['id'])
                    # referrals[link].update([data[node]['id']])
                    # periodically clear duplicates to save memory
                    if num_processed > 0 and num_processed % 5000 == 0:
                        referrals[link] = list(dict.fromkeys(referrals[link]))
                else:
                    referrals[link] = list(dict.fromkeys([data[node]['id']]))
                    #referrals[link] = set(data[node]['id'])
            # looks for sources in found aliases, and adds it to the linking
            for source in found_aliases:
                if source in referrals:
                    referrals[source].append(data[node]['id'])
                    # periodically clear duplicates to save memory
                    if num_processed > 0 and num_processed % 5000 == 0:
                        referrals[source] = list(
                            dict.fromkeys(referrals[source]))
                    # referrals[source].update([data[node]['id']])
                else:
                    referrals[source] = list(dict.fromkeys([data[node]['id']]))
                    #referrals[source] = set(data[node]['id'])

            data[node]['completed'] = True
            num_processed += 1
            if threshold > -1 and sys.getsizeof(referrals) > threshold:
                filename = './tempFiles/Twitter/twitter_referrals_' + \
                    str(os.getpid()) + '_' + str(num_processed) + '.json'
                with open(filename, 'w') as out:
                    out.write(json.dumps(referrals))
                    referrals.clear()
                    logging.info('Created file ' + filename)
            logging.info(str(os.getpid()) + ":Processed " + str(num_processed) + "/" + str(number_of_articles) + " twitter articles")  # nopep8
        if threshold > -1:
            final_filename = './tempFiles/Twitter/twitter_referrals_' + \
                str(os.getpid()) + '_final.json'
            with open(final_filename, 'w') as final:
                final.write(json.dumps(referrals))
        else:
            referrals_shared.update(referrals)
        end = timer()
        logging.info("Finished processing twitter - Took " + str(end - start) + " seconds")  # nopep8
    except Exception:
        logging.warning('Exception at Processing Twitter, data written to Saved/')  # nopep8
        exc_type, exc_value, exc_traceback = sys.exc_info()
        logging.error(exc_value)
        logging.error(exc_type)
        write_to_file(referrals, "Saved/twitter_referrals.json")
        write_to_file(data, "Saved/twitter_data.json")
        raise
    return data, referrals


def process_domain(data, scope):
    """
    Processes the domain data by finding all the articles that it is
    referring to and articles that are referring to it and mutating
    the output dictionary.
    Parameters:
        data: the domain output dictionary
        scope: the scope dictionary
    Return:
        Returns 2 dicts, one for the mutated data dictionary,
        and another dict of referrals.
    """
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
            # each key in links is an article url, and it has a list of
            # article ids that are referring it
            for link in data[node]['found_urls']:
                # save all referrals where each key is
                # each link in 'found_urls'
                # and the value is this article's id
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

            logging.info("Processed " + str(num_processed) + "/" + str(number_of_articles) + " domain articles")  # nopep8
        end = timer()
        logging.info("Finished Processing Domain - Took " + str(end - start) + " seconds")  # nopep8
    except Exception:
        logging.warning('Exception at Processing Domain, data written to Saved/')  # nopep8
        exc_type, exc_value, exc_traceback = sys.exc_info()
        logging.error(exc_value)
        logging.error(exc_type)
        write_to_file(referrals, "Saved/domain_referrals.json")
        write_to_file(data, "Saved/domain_data.json")
        raise
    return data, referrals


def multi_process_domain(data, scope, assignments, referrals_shared=None):
    """
    Processes the domain data using multi processing by finding all the articles that it is
    referring to and articles that are referring to it and mutating
    the output dictionary.
    Parameters:
        data: the domain output dictionary
        scope: the scope dictionary
    Return:
        Returns 2 dicts, one for the mutated data dictionary,
        and another dict of referrals.
    """
    logging.info("Processing Domain")
    try:
        start = timer()
        referrals = {}
        num_processed = 0
        threshold = MEM_LIMIT
        number_of_articles = len(assignments)  # len(data)
        for node in assignments:  # data:
            if data[node]['completed']:
                continue
            found_aliases, twitter_handles = find_aliases(data, node, scope)
            # each key in links is an article url, and it has a list of
            # article ids that are referring it
            for link in data[node]['found_urls']:
                # save all referrals where each key is
                # each link in 'found_urls'
                # and the value is this article's id
                if link['url'] in referrals:
                    referrals[link['url']].append(data[node]['id'])
                    # periodically clear duplicates to save memory
                    if num_processed > 0 and num_processed % 5000 == 0:
                        print('removing duplicates')
                        referrals[link['url']] = list(
                            dict.fromkeys(referrals[link['url']]))
                else:
                    referrals[link['url']] = list(
                        dict.fromkeys([data[node]['id']]))

            # looks for sources in found aliases, and adds it to the linking
            for source in found_aliases:
                if source in referrals:
                    referrals[source].append(data[node]['id'])
                    # periodically clear duplicates to save memory
                    if num_processed > 0 and num_processed % 5000 == 0:
                        print('removing duplicates')
                        referrals[source] = list(
                            dict.fromkeys(referrals[source]))
                else:
                    referrals[source] = list(dict.fromkeys([data[node]['id']]))

            data[node]['completed'] = True
            num_processed += 1
            if threshold > -1 and sys.getsizeof(referrals) > threshold:
                filename = './tempFiles/Domain/domain_referrals_' + \
                    str(os.getpid()) + '_' + str(num_processed) + '.json'
                with open(filename, 'w') as out:
                    out.write(json.dumps(referrals))
                    referrals.clear()
                    logging.info('Created file ' + filename)
            logging.info(str(os.getpid()) + ": Processed " + str(num_processed) + "/" + str(number_of_articles) + " domain articles")  # nopep8
        # referrals_shared.update(referrals)
        if threshold > -1:
            final_filename = './tempFiles/Domain/domain_referrals_' + \
                str(os.getpid()) + '_final.json'
            with open(final_filename, 'w') as final:
                final.write(json.dumps(referrals))
        else:
            referrals_shared.update(referrals)
        end = timer()
        logging.info("Finished Processing Domain - Took " + str(end - start) + " seconds")  # nopep8
        logging.info('Process ' + str(os.getpid()) +
                     ' processed ' + str(num_processed) + ' articles')
    except Exception:
        logging.warning('Exception at Processing Domain, data written to Saved/')  # nopep8
        exc_type, exc_value, exc_traceback = sys.exc_info()
        logging.error(exc_value)
        logging.error(exc_type)
        write_to_file(referrals, "Saved/domain_referrals.json")
        write_to_file(data, "Saved/domain_data.json")
        raise
    return data, referrals


def create_output(article, referrals, scope, output, interest_output, domain_pairs, twitter_pairs, domains, domain_to_url, final_pairs):  # nopep8
    '''
    Creates the data format for the output json by
    mutating the output and interest output object.
    '''

    domain = tldextract.extract(article["domain"])[1] if article['type'] == 'domain' else ''
    if (article["domain"] in scope.keys()) or (domain in domains):
        # article is in scope
        for ref in referrals:
            if ref in twitter_pairs:
                final_pairs[ref] = twitter_pairs[ref]
                #output[ref] = twitter_pairs[ref]
        if (article['type'] == "domain" or
            article['type'] == "twitter handle" or
                article['type'] == "text aliases") and len(referrals) == 0:
            # does not include these static nodes
            # in output if referral count is 0.
            return
        try:
            publisher = scope[article["domain"]]['Publisher']
        except Exception:
            publisher = scope[domain_to_url[domain]]['Publisher']
        try:
            tags = scope[article["domain"]]['Tags']
        except Exception:
            tags = scope[domain_to_url[domain]]['Tags']
        try:
            name = scope[article["domain"]]['Name']
        except Exception:
            name = scope[domain_to_url[domain]]['Name']
        output[article['id']] = {
                    'id': article['id'],
                    'url or alias text': article['url'],
                    'referring record id': referrals,
                    'number of referrals': len(referrals),
                    'type': article['type'],
                    'associated publisher': publisher,  # nopep8
                    'tags': tags,
                    'name/title': name,
                    'language': article["language"],
                    'authors': article['author_metadata'],
                    'date of publication': article['date'],
                    'plain text': article['article_text'],
                    'image reference': '',
                    'anchor text': '', }
    else:
        # article is not in scope
        cited = {}
        for ref_id in referrals:
            # print(domain_pairs)
            if ref_id in domain_pairs:
                try:
                    cited[domain_pairs[ref_id]['domain']] += 1
                except Exception:
                    cited[domain_pairs[ref_id]['domain']] = 1
            else:
                try:
                    cited[twitter_pairs[ref_id]['domain']] += 1
                except Exception:
                    cited[twitter_pairs[ref_id]['domain']] = 1
        # sort top referring domains by number of hits descending
        top = dict(sorted(cited.items(), key=lambda item: item[1], reverse=True))  # nopep8

        # top referrals gets the top 5 domains that
        # referred to this article the most
        interest_output[article['id']] = {
            'id': article['id'],
            'hit count': len(referrals),
            'url or alias text': article['url'],
            'source': article['domain'],
            'name': '',
                    'tags': [],
                    'publisher': '',
                    'referring record id': referrals,
                    'authors': article['author_metadata'],
                    'plain text': article['article_text'],
                    'type': article['type'],
                    'date of publication': article['date'],
                    'top referrals': dict(itertools.islice(top.items(), 5))}


def parse_referrals(article, domain_referrals, twitter_referrals):
    """
    Cross-match the referrals for domain referrals and twitter referrals.
    Returns a list of all sources that are referring to this specific article.
    Parameters:
      article: an article in the data
      domain_referrals: a dictionary of all the referrals in the domain data
      twitter_referrals: a dictionary of all the referrals in the twitter data
    """
    referring_articles = []
    # get all referrals for this url (who is referring to me)

    if article['url'] in domain_referrals:
        # if str(article['url']) in domain_referrals:
        referring_articles += domain_referrals[article['url']]

    if str(article['url']) in twitter_referrals:
        referring_articles += twitter_referrals[str(article['url'])]
    # print(referring_articles)
    # remove duplicates from list
    referring_articles = list(dict.fromkeys(referring_articles))
    # remove itself from list
    if article['id'] in referring_articles:
        referring_articles.remove(article['id'])

    return referring_articles


def merge_referals(source, dest):
    """
    Merge the source into dest 
    """
    ref_start = timer()
    c = 0
    for link in source:
        if link in dest:
            # dest[link].append(source[link])
            dest[link] += source[link]
        else:
            dest[link] = source[link].copy()
        c += 1
        logging.info('Merged ' + str(c) + '/' +
                     str(len(source)) + ' referrals')
    ref_end = timer()
    logging.info('Merged referrals in ' +
                 str(ref_end - ref_start) + ' seconds')


def mergeFiles(pathToFiles, destDict):
    """
    Merges all JSONs in pathToFiles into a single dictionary
    Parameters:
        pathToFiles: the path to the directory of JSONs that will be merged
        destDict: the destination dictionary where the results will be stored
    """
    files = os.listdir(pathToFiles)
    file_start = timer()
    count = 0
    for f in files:
        with open(pathToFiles + f, 'r') as readFile:
            tmp = json.loads(readFile.read())
            for key in tmp:
                if key in destDict:
                    destDict[key] += tmp[key]
                    destDict[key] = list(dict.fromkeys(destDict[key]))
                else:
                    destDict[key] = list(dict.fromkeys(tmp[key]))
        count += 1
        logging.info(f + ': Merged ' + str(count) +
                     '/' + str(len(files)) + ' files')
    file_end = timer()
    logging.info('Merged files in ' + str(file_end - file_start) + ' seconds')


def generateNode(url, result):
    """
    Generates a node for the given link and adds it to the result dictionary
    Parameters:
        url: the URL for which the node will be created
        result: a dictionary to store the URL-node pair
    """
    # domain = tldextract.extract(url)[1]#urlparse(url).netloc
    path = urlparse(url).path
    domain = url.replace(path, '')
    result[url] = {'id': str(uuid.uuid5(uuid.NAMESPACE_DNS, url)), 'type': 'domain', 'domain': domain,
                   'url': url, 'article_text': '', 'date': '', 'author_metadata': '', 'language': ''}


def process_crawler(domain_data, twitter_data, scope, domain_pairs,
                    twitter_pairs, saved_domain_referrals={},
                    saved_twitter_referrals={}):
    """
    The main point of entry for the processor.
    Calls the domain and twitter processor seperately.
    Parameters:
        domain_data: domain dictionary
        twitter_data: twitter dictionary
        scope: scope dictionary
        domain_pairs: domain dictionary mapping article id to url
        twitter_pairs: twitter dictionary mapping tweet id to url
        saved_domain_referrals: A dictionary of saved domain referrals,
                                with mapping of url to id.
        saved_twitter_referrals: A dictionary of saved twitter referrals,
                                with mapping of url to id.

    Outputs the post processing data into output.json and interest_output.json.
    """
    output = {}
    interest_output = {}
    # get a dictionary of all the referrals for each source
    if NUM_PROCS == -1 and MEM_LIMIT == -1:  # run normally w/o multiprocessing
        domain_data, domain_referrals = (process_domain(domain_data, scope))
        twitter_data, twitter_referrals = (
            process_twitter(twitter_data, scope))
    elif NUM_PROCS > 0:  # multiprocessing
        num_procs = NUM_PROCS
        ##### INITIALIZE DOMAIN PROCESSES #####
        managerDomain = Manager()
        domain_referrals = {}
        domain_dicts = [None] * num_procs
        assignments = {k: [] for k in range(num_procs)}
        process_index = 0
        domain_procs = [None] * num_procs
        for node in domain_data:
            assignments[process_index % num_procs].append(node)
            process_index += 1
        if MEM_LIMIT == -1:  # initialize shared dictionaries to store results in memory
            for proc in range(num_procs):
                domain_dicts[proc] = managerDomain.dict()

        for proc in range(num_procs):
            domain_procs[proc] = Process(target=multi_process_domain, args=(
                domain_data, scope, assignments[proc], domain_dicts[proc], ))
            domain_procs[proc].start()

        ##### INITIALIZE TWITTER PROCESSES #####
        managerTwitter = Manager()
        twitter_referrals = {}
        assignmentsTwitter = {k: [] for k in range(num_procs)}
        process_index = 0
        twit_dicts = [None] * num_procs
        if MEM_LIMIT == -1:  # initialize shared dictionaries to store results in memory
            for proc in range(num_procs):
                twit_dicts = managerTwitter.dict()
        for node in twitter_data:
            assignmentsTwitter[process_index % num_procs].append(node)
            process_index += 1
        twit_procs = [None] * num_procs
        for h in range(num_procs):
            twit_procs[h] = Process(target=multi_process_twitter, args=(
                twitter_data, scope, assignmentsTwitter[h], twit_procs[proc], ))
            twit_procs[h].start()

        ##### JOIN DOMAIN PROCESSES #####
        for proc in range(num_procs):
            domain_procs[proc].join()
            if MEM_LIMIT == -1:  # merge from memory
                merge_referals(domain_dicts[proc], domain_referrals)

        if MEM_LIMIT > 0:  # merge from disk files
            mergeFiles('./tempFiles/Domain/', domain_referrals)
        twitter_referrals = {}
        ##### JOIN TWITTER PROCESSES #####
        for p in range(num_procs):
            twit_procs[p].join()
            if MEM_LIMIT == -1:  # merge from memory
                merge_referals(twit_dicts[proc], twitter_referrals)
        if MEM_LIMIT > 0:  # merge from disk files
            mergeFiles('./tempFiles/Twitter/', twitter_referrals)

    # save referrals to file in case of break
    write_to_file(domain_data, "Saved/domain_data.json")
    write_to_file(domain_referrals, "Saved/domain_referrals.json")
    """
    if saved_domain_referrals != {}:
        # merge saved referrals and newly found referrals
        new = {key: value + saved_domain_referrals[key] for key, value in domain_referrals.items()}  # nopep8
        saved_domain_referrals.update(new)
        domain_referrals = saved_domain_referrals
        write_to_file(domain_referrals, "Saved/domain_referrals.json")
    """
    # save referrals to file in case of break
    write_to_file(twitter_data, "Saved/twitter_data.json")
    write_to_file(twitter_referrals, "Saved/twitter_referrals.json")
    """
    if saved_twitter_referrals != {}:
        print('merging saved and newly found referrals')
        # merge saved referrals and newly found referrals
        new = {key: value + saved_twitter_referrals[key] for key, value in twitter_referrals.items()}  # nopep8
        saved_twitter_referrals.update(new)
        twitter_referrals = saved_twitter_referrals
        write_to_file(twitter_referrals, "Saved/twitter_referrals.json")
    """
    # create static nodes


    print('creating static nodes')
    nodes = create_static_nodes(scope)
    # add these nodes to domain_data, prioritize domain_data if duplications
    nodes.update(domain_data)
    domain_data = nodes

    start = timer()
    # cross match between domain and twitter data and
    # create the output dictionary
    print('cross match between domain and twitter data ')

    domains = []
    domain_to_url = {}
    id_to_tweet = {}

    for url in scope.keys():
        if not url.startswith('@'):
            curr_dom = tldextract.extract(url)[1]
            domains.append(curr_dom)
            domain_to_url[curr_dom] = url
    #create output for articles found in 'found_urls' of twitter data
    twitter_urls = {}
    for link in twitter_referrals:
        if link in twitter_data or link in domain_data:
            continue
        generateNode(link, twitter_urls)
    for val in twitter_urls:
        referring_articles = parse_referrals(
            twitter_urls[val], domain_referrals, twitter_referrals)
        create_output(twitter_urls[val], referring_articles, scope, output,
                      interest_output, domain_pairs, twitter_pairs, domains, domain_to_url, id_to_tweet)

    #create output for articles found in 'found_urls' of domain data
    domain_urls = {}
    for link in domain_referrals:
        if link in domain_data or link in twitter_data:
            continue
        generateNode(link, domain_urls)
    for url in domain_urls:
        referring_articles = parse_referrals(domain_urls[url], domain_referrals, twitter_referrals)
        create_output(domain_urls[url], referring_articles, scope, output, interest_output, domain_pairs, twitter_pairs, domains, domain_to_url, id_to_tweet)

    
    for node in domain_data:
        referring_articles = parse_referrals(domain_data[node], domain_referrals, twitter_referrals)  # nopep8
        create_output(domain_data[node], referring_articles, scope, output, interest_output, domain_pairs, twitter_pairs, domains, domain_to_url, id_to_tweet)  # nopep8
    end = timer()
    logging.info("Parsing domain output file - Took " + str(end - start) + " seconds")  # nopep8

    start = timer()
    print('cross match between twitter and domain data ')
    for node in twitter_data:
        referring_articles = parse_referrals(twitter_data[node], domain_referrals, twitter_referrals)  # nopep8
        create_output(twitter_data[node], referring_articles, scope, output, interest_output, domain_pairs, twitter_pairs, domains, domain_to_url, id_to_tweet)  # nopep8
    end = timer()
    logging.info("Parsing twitter output file - Took " + str(end - start) + " seconds")  # nopep8
    logging.info("Output " + str(len(output)) + " articles in scope and " +
                 str(len(interest_output)) + " articles in interest scope")
    start = timer()
    write_to_file(id_to_tweet, 'id_to_tweet.json')
    # write final output to file
    write_to_file(output, "Output/output.json")
    # Sorts interest output
    interest_output = dict(sorted(interest_output.items(), key=lambda item: item[1]['hit count'], reverse=True))  # nopep8
    write_to_file(interest_output, "Output/interest_output.json")

    end = timer()
    logging.info("Serializing and writing final json output - Took " + str(end - start) + " seconds")  # nopep8


def write_to_file(dict, filename):
    """ Writes the dict to a json file with the given filename."""
    # Serializing json
    json_object = json.dumps(dict, indent=4)

    # Writing to output.json
    with open(filename, "w") as outfile:
        outfile.write(json_object)


def load_saved_files():
    '''Loads saved files from previous
    processor run and returns them as dicts.'''
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
    '''Creates nodes of type domain, twitter handle and text aliases,
    using each source from the scope.
    Returns created nodes in a dict.'''
    data = {}
    for source in scope:
        # create node for source (twitter handle or domain)
        if scope[source]["Type"] == "News Source":
            node_type = "domain"
        else:
            node_type = "twitter handle"
        # create uuid from string
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


def parse_args():
    """
    Parse the script arguments, setting NUM_PROCS & MEM_LIMIT accordingly
    """
    global NUM_PROCS, MEM_LIMIT
    usage = 'Usage: python3 processor.py [-num_procs=number [-limit=number]]'
    if len(sys.argv) > 3 or len(sys.argv) == 2:
        print(usage)
        sys.exit(1)
    elif len(sys.argv) == 3:
        options = sys.argv[1:]
        if options[0] == options[1]:
            print(usage)
            sys.exit(1)
        for op in options:
            spec = op.split('=')[0]
            val = op.split('=')[1]
            if not val.isdigit():
                print('Argument values must be positive integers')
                sys.exit(1)
            val = int(val)
            if spec not in ['-num_procs', '-limit']:
                print(usage)
                sys.exit(1)
            elif spec == '-num_procs':
                NUM_PROCS = val
            elif spec == '-limit':
                MEM_LIMIT = val

if __name__ == '__main__':
    # change this flag to true if restarting after a break,
    # and want to use saved data.
    read_from_memory = False

    parse_args()
    print('running with', NUM_PROCS, 'processes and', MEM_LIMIT, 'byte limit')

    start = timer()
    scope_timer = timer()
    # load scope
    scope = load_scope('./input_scope_final.csv')

    scope_timer_end = timer()

    twitter_timer = timer()
    print('loading twitter data')
    # load twitter data
    twitter_data, twitter_pairs = load_twitter_csv()

    twitter_timer_end = timer()

    domain_timer = timer()
    print('loading domain data')
    # load domain data
    domain_data, domain_pairs = load_json()

    domain_timer_end = timer()
    if read_from_memory:
        # read saved data into memory
        domain_data, twitter_data, domain_referrals, twitter_referrals = load_saved_files()  # nopep8
        process_crawler(domain_data, twitter_data,
                        scope, domain_pairs, twitter_pairs,
                        domain_referrals, twitter_referrals)
    else:
        process_crawler(domain_data, twitter_data,
                        scope, domain_pairs, twitter_pairs)
    end = timer()
    # Time in seconds
    logging.info("Time to run whole post-processor took " + str(end - start) + " seconds")  # nopep8
    logging.info("Time to read scope took " + str(scope_timer_end - scope_timer) + " seconds")  # nopep8
    logging.info("Time to read twitter files took " + str(twitter_timer_end - twitter_timer) + " seconds")  # nopep8
    logging.info("Time to read domain files took " + str(domain_timer_end - domain_timer) + " seconds")  # nopep8
    logging.info("FINISHED")
