# API Server for SRM

This server contains the two main api servers for the SRM project.

It makes use of these two libraries:
- [APISQL](https://github.com/dataspot/apisql), for providing a read only query API to a DBMS
- [APIES](https://github.com/OpenBudget/apies), for providing a read only search and fetch API to an ElasticSearch instance

## Routes

It maps these two route prefixes:
- `/api/db` for apisql
- `/api/idx` for apies

(for specific enpoints see the respective library documentation)

## Serving

This image uses gunicorn for web serving.

It uses 4 workers and listens on port 5000.

## Configuration

All configuration is done via environment variables:
- `DATABASE_READONLY_URL` - connection string for a read-only access to a DB instance
- `ES_INDEX_NAME` - The ElasticSearch index name
- `ES_DATAPACKAGE` - A URL pointing to a datapacakge describing the index and containing search hints
- `ES_HOST` - Host for the ES instance
- `ES_PORT` - Port of the ES instance
- `ES_HTTP_AUTH` - Optional http auth for the ES instance in the following format: `username:password`

