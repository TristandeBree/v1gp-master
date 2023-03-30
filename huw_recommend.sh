#!/bin/sh
export FLASK_APP=huw_recommend.py
python -m flask run --port 5001
# python -m flask --app huw_recommend run --port 5001