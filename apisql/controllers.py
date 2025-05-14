import re
import math
from decimal import Decimal
from datetime import date
from backports.cached_property import cached_property
from itertools import islice
import codecs
import urllib.parse

from sqlalchemy import create_engine, text

from .logger import logger


class Controllers():

    FIELD_RE = re.compile('<([-a-z0-9()_]+)$')
    PARAM_RE = re.compile(':([-a-z()_]+)$')

    def __init__(self, connection_string, max_rows, debug, engine):
        self.connection_string = connection_string
        self.engine_ = engine
        self.max_rows = max_rows
        self.debug = debug

    @cached_property
    def engine(self):
        if self.engine_:
            return self.engine_
        return create_engine(self.connection_string, pool_size=20, max_overflow=0)

    def query_db_streaming(self, query_str, formatters):
        try:
            query_str = self.prepare_query(query_str)
            headers, formatters = self.parse_formatters(formatters)

            with self.engine.connect() as connection:
                logger.debug('executing %r', query_str)
                result = connection.execution_options(stream_results=True)\
                    .execute(text('select * from (%s) s' % query_str))
                yield headers
                yield from (
                    [f(row) for f in formatters]
                    for row in map(self.jsonable, map(lambda r: r._asdict(), result))
                )
        except Exception:
            logger.exception('EXC')
            raise

    def query_db(self, query_str, num_rows, page_size, page):
        headers = []
        query_str = self.prepare_query(query_str)
        try:
            with self.engine.connect() as connection:
                count_query = text("select count(1) from (%s) s" % query_str)
                logger.debug('executing %r', count_query)
                count = connection.execute(count_query).fetchone()[0]
                logger.debug('count %r', count)

                num_rows = min(num_rows, self.max_rows, page_size)
                pages = math.ceil(count / num_rows)
                page = min(page, pages-1)
                page = max(page, 0)
                offset = page * page_size
                query = text("select * from (%s) s limit %s offset %s" % (query_str, num_rows, offset))
                logger.debug('executing %r', query)
                result = connection.execute(query)
                headers = list(result.keys())
                rows = list(map(lambda r: r._asdict(), islice(iter(result), 0, num_rows)))
                rows = [self.jsonable(row) for row in rows]
                logger.debug('rowcount %r', len(rows))
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
        ret = {
            'success': True,
            'total': count,
            'pages': pages,
            'page_size': num_rows,
            'page': page,
            'rows': rows,
        }
        if headers:
            b64query = codecs.encode(query_str.encode('utf8'), 'base64').decode('ascii')
            b64query = urllib.parse.quote(b64query)
            headers = ';'.join(headers)
            headers = urllib.parse.quote(headers)
            ret['download_url'] = '/download?query=%s&headers=%s' % (b64query, headers)
        return ret

    def prepare_query(self, query_str):
        query_str = query_str.strip()
        query_str = query_str.rstrip(';')
        return query_str

    def parse_formatters(self, formatters):
        _headers = []
        _formatters = []
        for h in formatters:
            field = self.FIELD_RE.findall(h)
            if len(field) == 1:
                field = field[0]
                h = h[:-(len(field)+1)]
            else:
                field = None
            matches = self.PARAM_RE.findall(h)
            funcs = [self.formatter('')]
            while len(matches) > 0:
                mod = matches[0]
                h = h[:-(len(mod)+1)]
                funcs.append(self.formatter(mod))
                matches = self.PARAM_RE.findall(h)
            f = self.getter(field or h)
            for g in reversed(funcs):
                f = self.compose(f, g)
            k = self.wrapper(f)
            _formatters.append(k)
            _headers.append(h)
        return _headers, _formatters

    def wrapper(self, f):
        def _f(row):
            return f('', row)
        return _f

    def getter(self, h):
        hdr = h

        def _f(x, row):
            return row[hdr]
        return _f

    def compose(self, f, g):
        def _f(x, row):
            return g(f(x, row), row)
        return _f

    def formatter(self, mod):
        if mod == 'number':
            def _f(x, row):
                return str(x) if x else ''
            return _f
        elif mod == 'yesno':
            def _f(x, row):
                return 'Yes' if x else 'No'  # TODO
            return _f
        elif mod == 'comma-separated':
            def _f(x, row):
                if x and isinstance(x, list):
                    return ','.join(x)
                return x
            return _f
        else:
            def _f(x, row):
                return str(x) if x else ''
            return _f

    def jsonable(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, date):
            return obj.isoformat()
        if isinstance(obj, list):
            return [self.jsonable(x) for x in obj]
        if isinstance(obj, dict):
            return dict((k, self.jsonable(v)) for k, v in obj.items())
        return obj
