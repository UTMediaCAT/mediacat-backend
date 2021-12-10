import os
import json
import sys

for f in os.listdir(sys.argv[1]):
    dir_name = sys.argv[1] if sys.argv[1].endswith('/') else sys.argv[1] + '/'
    with open(dir_name + f, 'r') as json_file:
        d = json.loads(json_file.read())

        results = os.popen('node dates.js ' + d['url']).read().split('\n')
        try:
            date = results[0]
        except Exception:
            date = ""
        try:
            author = results[1]
        except Exception:
            author = ""
        try:
            title = results[2]
        except Exception:
            title = ""
        if date != 'null' and len(date) > 0:
            d['date'] = date
        if author != 'null' and len(author) > 0:
            d['author_metascraper'] = author
        if title != 'null' and len(title) > 0:
            d['title_metascraper'] = title
    with open('./DatedOutput/' + f, 'w') as outfile:
        outfile.write(json.dumps(d))


