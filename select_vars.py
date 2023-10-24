#!/usr/bin/env python3
import censusdata
import duckdb
import us
from dotenv import load_dotenv

import sys
import os
import json

from initdb import init_db

load_dotenv()
patterns = [pattern for pattern in sys.argv[1:]]
if not patterns:
    print("Usage: select_vars.py pattern [ pattern] ...")
    sys.exit(1)
init_db()
var_label_pairs = []
for pattern in patterns:
    duckdb.execute("SELECT * FROM dp_var_lookup(?)", [pattern])
    var_label_pairs += duckdb.fetchall()
selected_vars = {
    item[0]: {"label": item[1], "name": item[0]} for item in var_label_pairs
}
print(json.dumps(selected_vars))
"""
if selected_vars:
    data = censusdata.download(
     os.environ['CENSUS_TABLE'],
     int(environ['CENSUS_DATA_YEAR']),
     censusdata.censusgeo([("state", environ['STATE_FIPS']), ("county", environ['COUNTY_FIPS'])]),
     selected_vars,
     key=environ['CENSUS_API_KEY'],
     tabletype="profile",
    )
    print(data)
else:
    print("No matches")
"""
