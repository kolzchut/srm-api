import os
from apies.logger import logger
from apies.query import Query

from flask import Flask
from flask_cors import CORS

import elasticsearch

from apisql import apisql_blueprint
from apies import apies_blueprint


def text_field_rules(field):
    if field['name'] not in ('name', 'service_name'):
        return []
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

    def apply_extra(self, extras):
        if extras:
            situations = extras.split('|')
            all_situations = []
            spare_all_situations = []
            by_kind = dict()
            for situation in situations:
                prefix = ':'.join(situation.split(':')[:2])
                if situation != prefix:
                    if prefix != 'human_situations:age_group':
                        all_situations.append(situation)
                    else:
                        spare_all_situations.append(situation)
                    by_kind.setdefault(prefix, []).append(situation)
            if len(all_situations) == 0:
                all_situations = spare_all_situations
            if len(by_kind) > 0:
                for t in self.types:
                   if t in ('cards', 'points'): 
                        must = self.must(t)
                        if len(by_kind) > 1:
                            for kind, kind_situations in by_kind.items():
                                must.append(dict(
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
                        if all_situations:
                            must.append(dict(
                                terms=dict(
                                    situation_ids=all_situations
                                )
                            ))
        if 'points' in self.types:
            self.q['points']['collapse'] = dict(field='point_id')
        return self


app = Flask(__name__)
CORS(app, supports_credentials=True)

# SQL API
app.register_blueprint(
    apisql_blueprint(
        connection_string=os.environ['DATABASE_READONLY_URL'],
        max_rows=10000, debug=False
    ),
    url_prefix='/api/db/'
)

# ES API
index_name = os.environ['ES_INDEX_NAME']
TYPES = ['cards', 'places', 'responses', 'points', 'presets']
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
    multi_match_type='bool_prefix',
    dont_highlight=['*'],
    query_cls=SRMQuery,
)
app.register_blueprint(blueprint, url_prefix='/api/idx/')



if __name__=='__main__':
    app.run()
else:
    import logging
    gunicorn_error_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers.extend(gunicorn_error_logger.handlers)
    app.logger.setLevel(logging.DEBUG)
    app.logger.info('SERVER STARTING')
