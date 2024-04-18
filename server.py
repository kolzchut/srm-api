import os
import json

from flask import Flask, current_app, request
from flask_cors import CORS

import elasticsearch
from datapackage import Package

from apisql import apisql_blueprint

from apies import apies_blueprint
from apies.logger import logger
from apies.query import Query


def text_field_rules(field):
    if field['name'].split('_')[-1] in ('id', 'ids', 'categories', 'category', 'key'):
        return []
    elif field.get('es:autocomplete'):
        return [('inexact', '^10'), ('inexact', '._2gram^10'), ('inexact', '._3gram^10')]
    elif field['name'].split('_')[-1] in ('name', 'synonyms', 'heb'):
        return [('inexact', '^10'), ('natural', '.hebrew^10')]
    elif field.get('es:hebrew') or field['name'].split('_')[-1] in ('purpose', 'description', 'details', 'query'):
        return [('inexact', ''), ('natural', '.hebrew')]
    # elif field.get('es:title'):
    #     if field.get('es:keyword'):
    #         return [('exact', '^10')]
    #     else:
    #         return [('inexact', '^3')]
    # elif field.get('es:boost'):
    #     if field.get('es:keyword'):
    #         return [('exact', '^10')]
    #     else:
    #         return [('inexact', '^10')]
    elif field.get('es:keyword'):
        return [('exact', '')]
    else:
        return [('inexact', '')]


class SRMQuery(Query):

    extract_agg = False
    extract_viewport = False
    collapse_hits = False

    STOPWORDS = [
        'עמותה',
        'גיל', 'הגיל', 'לגיל',
        'קבוצה', 'קבוצת', 'הקבוצה', 'לקבוצה',
        'עבור', 'טיפול', 'ניתן',
        'אנשים',
        'שירות', 'שירותים', 'השירות', 'לשירות', 'שרות', 'שרותים',
        'בכל', 'לכל',
        'תוכנית', 'תכנית',
        'על', 'בעלי',
    ]

    def cleanup_query(self, q):
        return ' '.join(filter(lambda x: x not in self.STOPWORDS, q.split(' ')))

    def apply_term(self, term, *args, **kwargs):
        return super().apply_term(self.cleanup_query(term), *args, **kwargs)

    def apply_highlighting(self, term, *args, **kwargs):
        return super().apply_highlighting(self.cleanup_query(term), *args, **kwargs)

    def apply_extra(self, extras):
        if extras:
            extras = extras.split('|')
            for x in extras:
                if x == 'distinct-situations':
                    if 'cards' in self.q:
                        self.q['cards'].setdefault('aggs', {})['situations'] = {
                            'terms': {
                                'field': f'situations.id',
                                'size': 1000
                            }
                        }
                        self.extract_agg = True
                if x in 'distinct-responses':
                    if 'cards' in self.q:
                        min_score = self.q['cards'].get('min_score', 0)
                        if min_score > 0:
                            self.q['cards'].setdefault('aggs', {})['responses'] = {
                                'terms': {
                                    'field': f'responses_parents.id',
                                    'size': 1000
                                },
                                'aggs': {
                                    'max_score': {
                                        'max': {
                                            'script': '_score'
                                        }
                                    }
                                },
                            }
                        else:
                            self.q['cards'].setdefault('aggs', {})['responses'] = {
                                'terms': {
                                    'field': f'responses_parents.id',
                                    'size': 1000
                                }
                            }
                        self.q['cards'].setdefault('aggs', {})['categories'] = {
                            'terms': {
                                'field': 'response_categories',
                                'size': 20
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
                            'field': field,
                            'inner_hits': {
                                'name': 'collapse_hits',
                                'size': 1000,
                                '_source': [
                                    'card_id',
                                    'organization_name',
                                    'organization_short_name',
                                    'organization_name_parts',
                                    'address_parts',
                                    'branch_city',
                                    'branch_address',
                                    'branch_geometry',
                                    'point_id',
                                    'service_name',
                                    'national_service',
                                ]
                            }
                        }
                        self.collapse_hits = True
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
                            },
                            'aggs': {
                                'response_category': {
                                    'terms': {
                                        'field': 'response_category',
                                        'size': 1
                                    }
                                },
                                'branch_location_accurate': {
                                    'terms': {
                                        'field': 'branch_location_accurate',
                                        'size': 1
                                    }
                                },
                                'branch_id': {
                                    'terms': {
                                        'field': 'branch_id',
                                        'size': 99
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
                        self.extract_agg = True
                if x == 'national-services':
                    if 'cards' in self.q:
                        self.q['cards']['sort'].insert(0, {'national_service': {'order': 'asc'}})
                if x == 'viewport':
                    if 'cards' in self.q:
                        self.q['cards'].setdefault('aggs', {})['viewport'] = {
                            'geo_bounds': {
                                'field': 'branch_geometry',
                                'wrap_longitude': True
                            }
                        }
                        self.extract_viewport = True

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
                        elif 'buckets' in v:
                            return_value[k] = v['buckets']
        if self.extract_viewport:
            for _type, resp in zip(self.types, response['responses']):
                if _type == 'cards':
                    if 'viewport' in resp['aggregations']:
                        viewport = resp['aggregations']['viewport']
                        if 'bounds' in viewport:
                            return_value['viewport'] = viewport['bounds']
                        else:
                            print('NO BOUNDS', viewport)
        if self.collapse_hits:
            for _type, resp in zip(self.types, response['responses']):
                if _type == 'cards':
                    for h in resp.get('hits', {}).get('hits', []):
                        collapse_hits = h.get('inner_hits', {}).get('collapse_hits', {}).get('hits', {}).get('hits', [])
                        collapse_hits = [x.get('_source', {}) for x in collapse_hits]
                        h['_source']['collapse_hits'] = collapse_hits

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
    debug_queries=False,
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
    multi_match_operator='and',
    query_cls=SRMQuery,
)
app.register_blueprint(blueprint, url_prefix='/api/idx/')


# Simple API, with four parameters: q, response, situation and bounds
@app.route('/api/simple/cards')
def simple_cards():
    q = request.args.get('q', '')
    responses = request.args.get('response', '')
    situations = request.args.get('situation', '')
    bounds = request.args.get('bounds', '')
    filters = {}
    if responses:
        filters['response_ids_parents']= responses
    if situations:
        filters['situation_ids']= situations
    if bounds:
        bounds = bounds.split(',')
        bounds = [float(x) for x in bounds]
        filters['branch_geometry__bounded'] = [
            [
                [bounds[0], bounds[3]],
                [bounds[2], bounds[1]],
            ]
        ]
    filters = json.dumps([filters])

    es_client = current_app.config['ES_CLIENT']        
    ret = blueprint.controllers.search(
        es_client, ['cards'], q,
        size=10,
        offset=0,
        filters=filters,
        score_threshold=0, 
        match_type='cross_fields',
        match_operator='or',
    )
    KEYS = {
        'service_name',
        'service_description',
        'service_details',
        'service_payment_details',
        'service_payment_required',
        'service_phone_numbers',
        'service_urls',
        'service_email_address',
        'branch_urls',
        'branch_orig_address',
        'branch_phone_numbers',
        'branch_email_address',
        'branch_description',
        'organization_name',
        'organization_kind',
        'organization_email_address',
        'organization_phone_numbers',
        'organization_urls',
        'national_service',
        'situations',
        'responses',
    }
    results = []
    search_results = ret.get('search_results')
    for rec in search_results:
        rec = rec.get('source')
        rec = {k: v for k, v in rec.items() if k in KEYS and v is not None and v != []}
        if rec.get('service_description'):
            rec['service_description'] = rec['service_description'][:200]
        results.append(rec)
        for r in rec.get('responses', []):
            r.pop('synonyms', None)
        for r in rec.get('situations', []):
            r.pop('synonyms', None)
    ret['search_results'] = results
    return ret

@app.route('/api/simple/taxonomy')
def simple_taxonomy():
    q = request.args.get('q', '')

    es_client = current_app.config['ES_CLIENT']        
    ret = blueprint.controllers.search(
        es_client, ['cards'], q,
        size=1,
        offset=0,
        extra='distinct-situations|distinct-responses',
        score_threshold=0, 
        match_type='cross_fields',
        match_operator='or',
    )
    return dict(
        situations=ret.get('situations', [])[:30],
        responses=ret.get('responses', [])[:30],
    )



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
