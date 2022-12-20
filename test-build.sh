#!/bin/sh


virtualenv env1
source env1/bin/activate
pip3 install -r requirements.txt
ls -latr /Users/runner/hostedtoolcache/Python/3.10.6/x64/lib/python3.10/site-packages/*
python3 oracle_python_connector.py