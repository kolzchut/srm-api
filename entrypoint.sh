#!/bin/sh

python prepare.py
/usr/local/bin/gunicorn -w 4 --bind 0.0.0.0:5000 server:app