#!/usr/bin/env python3
import censusdata
import duckdb
import us
from dotenv import load_dotenv

import sys
from os import environ

from initdb import init_db

load_dotenv()
patterns = [pattern for pattern in sys.argv[1:]]
if not patterns:
    print("Usage: select_vars.py pattern [ pattern] ...")
    sys.exit(1)
try:
    con = duckdb.connect(database='census.db', read_only=True)
except duckdb.CatalogException:
    init_db()
    con = duckdb.connect(database='census.db', read_only=True)

selected_vars = []
for pattern in patterns:
    con.execute("""
        WITH item_label_matches AS (
            SELECT DISTINCT mm_id
            ,cui
            ,score
            ,ROW_NUMBER() OVER (PARTITION BY mm_id ORDER BY score DESC) AS cui_score_rank
            FROM dp_vw
            WHERE regexp_matches(item_label, ?)
        )
        select dp_vars_to_nums_vw.var
        from item_label_matches
        inner join dp_vars_to_nums_vw
            on item_label_matches.mm_id = dp_vars_to_nums_vw.item_id
        where regexp_matches(dp_vars_to_nums_vw.var, 'DP\w+\_\d+E')
        and cui_score_rank=1
    """, [pattern])
    selected_vars += con.fetchall()
selected_vars = [item for sublist in selected_vars for item in sublist ]
if selected_vars:
    data = censusdata.download(
     environ['CENSUS_TABLE'],
     int(environ['CENSUS_DATA_YEAR']),
     censusdata.censusgeo([("state", environ['STATE_FIPS']), ("county", environ['COUNTY_FIPS'])]),
     selected_vars,
     key=environ['CENSUS_API_KEY'],
     tabletype="profile",
    )
    print(data)
else:
    print("No matches")