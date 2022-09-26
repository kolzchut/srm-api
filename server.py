import os
from apies.logger import logger
from apies.query import Query

from flask import Flask
from flask_cors import CORS

import elasticsearch

from apisql import apisql_blueprint
from apies import apies_blueprint


def text_field_rules(field):
    if field['name'].split('_')[-1] in ('name', 'purpose', 'description', 'details', 'synonyms'):
        print('CONVERTED TYPE FOR HEBREW', field['name'])
        return [('inexact', '^10'), ('natural', '.hebrew^3')]
    if field.get('es:autocomplete'):
        return [('inexact', ''), ('inexact', '._2gram'), ('inexact', '._3gram')]
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

    TRIVIAL_PREFIXES = ('human_situations:age_group', 'human_situations:language')

    def apply_extra(self, extras):
        if extras:
            situations = extras.split('|')

            specific_situations = dict()
            must_match_one = list()
            by_kind = dict()
            non_trivial_prefixes = set()

            for situation in situations:
                prefix = ':'.join(situation.split(':')[:2])
                specific_situations.setdefault(prefix, []).append(situation)
                if prefix not in self.TRIVIAL_PREFIXES:
                    non_trivial_prefixes.add(prefix)

            for prefix, situations in specific_situations.items():
                if len(situations) > 1:
                    situations = [s for s in situations if s != prefix]
                    by_kind[prefix] = situations
                if len(non_trivial_prefixes) == 0 or prefix not in self.TRIVIAL_PREFIXES:
                    must_match_one.extend(situations)

            if len(by_kind) > 0:
                for t in self.types:
                   if t in ('cards', 'points'): 
                        filter_must = self.filter(t).setdefault('must', [])
                        for kind, kind_situations in by_kind.items():
                            filter_must.append(dict(
                                bool=dict(
                                    should=[
                                        dict(
                                            terms=dict(
                                                situation_ids=kind_situations
                                            )
                                        ),
                                        dict(
                                            bool=dict(
                                                must_not=dict(
                                                    term=dict(
                                                        situation_ids=kind
                                                    )
                                                )
                                            )
                                        )
                                    ],
                                    minimum_should_match=1
                                )
                            ))
                        if must_match_one:
                            must = self.must(t)
                            must.append(dict(
                                bool=dict(
                                    should=[
                                        dict(
                                            term=dict(
                                                situation_ids=dict(
                                                    value=s
                                                )
                                            )
                                        )
                                        for s in must_match_one
                                    ],
                                    minimum_should_match=1
                                )
                            ))

        # if 'points' in self.types:
        #     self.q['points']['collapse'] = dict(field='point_id')
        return self


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
TYPES = ['cards', 'places', 'responses', 'points', 'presets', 'geo_data', 'orgs', 'autocomplete']
datapackages = [x.strip() for x in os.environ['ES_DATAPACKAGE'].split('\n') if x.strip()]
blueprint = apies_blueprint(app,
    datapackages,
    elasticsearch.Elasticsearch(
        [dict(host=os.environ['ES_HOST'], port=int(os.environ['ES_PORT']))], timeout=60,
        **({"http_auth": os.environ['ES_HTTP_AUTH'].split(':')} if os.environ.get('ES_HTTP_AUTH') else {})
    ),
    dict(
        (t, f'{index_name}__{t}')
        for t in TYPES
    ),
    f'{index_name}__cards',
    debug_queries=True,
    text_field_rules=text_field_rules,
    text_field_select=dict(
        cards=['service_name', 'organization_name', 'responses.name', 'branch_address', 
               'branch_name', 'situations.name', 'responses.synonyms', 
               'situations.synonyms', 'service_details', 'service_description'],
        places=['name'],
        responses=['name', 'synonyms'],
        points=[]
    ),
    multi_match_type='bool_prefix',
    multi_match_operator='or',
    dont_highlight=['*'],
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
