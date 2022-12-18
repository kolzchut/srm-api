import os
import json

from flask import Flask
from flask_cors import CORS

import elasticsearch
from datapackage import Package

from apisql import apisql_blueprint

from apies import apies_blueprint
from apies.logger import logger
from apies.query import Query


def text_field_rules(field):
    if field['name'].split('_')[-1] in ('name', 'purpose', 'description', 'details', 'synonyms', 'heb'):
        print('CONVERTED TYPE FOR HEBREW', field['name'])
        return [('inexact', '^10'), ('natural', '.hebrew^3')]
    if field['name'].split('_')[-1] in ('id', 'ids', 'categories', 'category', 'key'):
        return []
    if field.get('es:autocomplete'):
        return [('inexact', '^10'), ('inexact', '._2gram^10'), ('inexact', '._3gram^10')]
    if field.get('es:title'):
        if field.get('es:keyword'):
            return [('exact', '^10')]
        else:
            return [('inexact', '^3')]
    elif field.get('es:boost'):
        if field.get('es:keyword'):
            return [('exact', '^10')]
        else:
            return [('inexact', '^10')]
    elif field.get('es:keyword'):
        return [('exact', '')]
    else:
        return [('inexact', '')]


class SRMQuery(Query):

    extract_agg = False

    def apply_extra(self, extras):
        if extras:
            extras = extras.split('|')
            for x in extras:
                if x in ('distinct-situations', 'distinct-responses'):
                    if 'cards' in self.q:
                        field = x[9:]
                        self.q['cards'].setdefault('aggs', {})[field] = {
                            'terms': {
                                'field': f'{field}.id',
                                'size': 1000
                            }
                        }
                        self.extract_agg = True
                if x == 'did-you-mean':
                    if 'cards' in self.q:
                        self.q['cards'].setdefault('aggs', {})['inner_pac'] = {
                            'diversified_sampler': {
                                'shard_size': 50,
                                'field': 'service_id'
                            },
                            'aggs': {
                                'possible_autocomplete': {
                                    'terms': {
                                        'field': "possible_autocomplete",
                                        'size': 10
                                    }
                                }
                            }
                        }
                        self.extract_agg = True
                if x == 'collapse':
                    if 'cards' in self.q:
                        field = 'collapse_key'
                        self.q['cards']['collapse'] = {
                            'field': field
                        }
                if x == 'collapse-collect':
                    if 'cards' in self.q:
                        field = 'collapse_key'
                        self.q['cards'].setdefault('aggs', {})[field] = {
                            'terms': {
                                'field': field,
                                'size': 20000,
                                'min_doc_count': 2
                            }
                        }
                        self.extract_agg = True
                if x == 'point-ids':
                    if 'cards' in self.q:
                        field = 'point_id'
                        self.q['cards'].setdefault('aggs', {})[field] = {
                            'terms': {
                                'field': field,
                                'size': 2500
                            }
                        }
                        self.extract_agg = True
                if x == 'point-ids-extended':
                    if 'cards' in self.q:
                        field = 'point_id'
                        self.q['cards'].setdefault('aggs', {})[field] = {
                            'terms': {
                                'field': field,
                                'size': 2500,
                                'aggs': {
                                    'response_category': {
                                        'terms': {
                                            'field': 'response_category',
                                            'size': 1
                                        }
                                    },
                                    'branch_geometry': {
                                        'terms': {
                                            'field': 'coords',
                                            'size': 1
                                        }
                                    }
                                }
                            }
                        }
                        self.extract_agg = True
        return self

    def process_extra(self, return_value, response):
        if self.extract_agg:
            for _type, resp in zip(self.types, response['responses']):
                if _type == 'cards':
                    for k, v in resp['aggregations'].items():
                        if k.startswith('inner_'):
                            for k_, v_ in v.items():
                                if k_ != 'doc_count':
                                    return_value[k_] = v_['buckets']
                        else:
                            return_value[k] = v['buckets']


app = Flask(__name__)
CORS(app, supports_credentials=True)

# SQL API
app.register_blueprint(
    apisql_blueprint(
        connection_string=os.environ['DATABASE_READONLY_URL'],
        max_rows=20000, debug=False
    ),
    url_prefix='/api/db/'
)

# ES API
index_name = os.environ['ES_INDEX_NAME']
# TYPES = ['cards', 'places', 'responses', 'situations', 'points', 'presets', 'geo_data', 'orgs', 'autocomplete']
datapackages = json.load(open('datapackages.json'))
datapackages = [Package(x) for x in datapackages]
types = [p.resources[0].name for p in datapackages]
print('TYPES:', types)

blueprint = apies_blueprint(app,
    datapackages,
    elasticsearch.Elasticsearch(
        [dict(host=os.environ['ES_HOST'], port=int(os.environ['ES_PORT']))], timeout=60,
        **({"http_auth": os.environ['ES_HTTP_AUTH'].split(':')} if os.environ.get('ES_HTTP_AUTH') else {})
    ),
    dict(
        (t, f'{index_name}__{t}')
        for t in types
    ),
    f'{index_name}__cards',
    debug_queries=True,
    text_field_rules=text_field_rules,
    # text_field_select=dict(
    #     cards=['service_name', 'organization_name', 'responses.name', 'branch_address', 
    #            'branch_name', 'situations.name', 'responses.synonyms', 
    #            'situations.synonyms', 'service_details', 'service_description'],
    #     places=['name'],
    #     responses=['name', 'synonyms'],
    #     points=[]
    # ),
    multi_match_type='bool_prefix',
    multi_match_operator='or',
    query_cls=SRMQuery,
)
app.register_blueprint(blueprint, url_prefix='/api/idx/')


@app.after_request
def add_header(response):
    response.cache_control.max_age = 600
    return response


if __name__=='__main__':
    app.run()
else:
    import logging
    gunicorn_error_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers.extend(gunicorn_error_logger.handlers)
    app.logger.setLevel(logging.DEBUG)
    app.logger.info('SERVER STARTING')
