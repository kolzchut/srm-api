import json
import os
import requests

if __name__ == '__main__':
    print('PREPARING...')
    urls = [x.strip() for x in os.environ['ES_DATAPACKAGE'].split('\n') if x.strip()]
    total = []
    for url in urls:
        print('GET', url)
        total.append(requests.get(url, timeout=60).json())
    json.dump(total, open('datapackages.json', 'w'))
