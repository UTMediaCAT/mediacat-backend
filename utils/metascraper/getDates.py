import os
import json
import sys
from multiprocessing import Process, Manager
def getDate(f):

#for f in os.listdir(sys.argv[1]):
    '''
    if i == 10:
        break
    '''
    with open(f, 'r') as json_file:
        d = json.loads(json_file.read())

        results = os.popen('node dates.js ' + d['url']).read().split('\n')
        try:
            date = results[0]
        except Exception:
            date = ""
        try:
            author = results[1]
        except Exception:
            author=""
        try:
            title = results[2]
        except Exception:
            title=""
        if date != 'null' and len(date) > 0:
            d['date'] = date
        if author != 'null' and len(author) > 0:
            d['author_metascraper'] = author
        if title != 'null' and len(title) > 0:
            d['title_metascraper'] = title
    with open('./DatedOutput/' + f.split('/')[-1], 'w') as outfile:
        outfile.write(json.dumps(d))

def init(files):
    for path in files:
        getDate(path)
    exit(0)


if __name__ == '__main__':
    num_procs = 50
    process_index = 0
    assignments = {k: [] for k in range(num_procs)}
    for f in os.listdir(sys.argv[1]):
        path = sys.argv[1] + f if sys.argv[1].endswith('/') else sys.argv[1] + '/' + f
        assignments[process_index % num_procs].append(path)
        process_index+=1
    procs = [None] * num_procs
    for proc in range(num_procs):
        procs[proc] = Process(target=init, args=(assignments[proc], ))
        procs[proc].start()