import os

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
            if 'distinct-situations' in extras:
                if 'cards' in self.q:
                    self.q['cards']['aggs'] = {
                        'situations': {
                            'value_count': {
                                'field': 'situations.id'
                            }
                        }
                    }
                    self.extract_agg = True
        return self

    def process_extra(self, return_value, response):
        if self.extract_agg:
            for _type, resp in zip(self.types, response['responses']):
                if _type == 'cards':
                    return_value['situations'] = resp['aggs']['situations']


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
datapackages = [x.strip() for x in os.environ['ES_DATAPACKAGE'].split('\n') if x.strip()]
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
