import duckdb

import os


def init_db():
    con = duckdb.connect('census.db')
    con.sql(""" CREATE OR REPLACE TABLE dp_mm AS
                SELECT * FROM read_csv('.\\resources\\mm_dp_fixed.csv', columns={'id':'VARCHAR',
                'mmi':'VARCHAR',
                'score': 'FLOAT',
                'umls_preferred_name':'VARCHAR',
                'cui':'VARCHAR',
                'semantic_type_list':'VARCHAR',
                'trigger_information':'VARCHAR',
                'location':'VARCHAR',
                'positional_information':'VARCHAR',
                'mesh_terms':'VARCHAR'},
                ignore_errors=True)
    """
    )
    con.sql(""" CREATE OR REPLACE TABLE st_mm AS
                SELECT * FROM read_csv('.\\resources\\mm_st_fixed.csv', columns={'id':'VARCHAR',
                'mmi':'VARCHAR',
                'score': 'FLOAT',
                'umls_preferred_name':'VARCHAR',
                'cui':'VARCHAR',
                'semantic_type_list':'VARCHAR',
                'trigger_information':'VARCHAR',
                'location':'VARCHAR',
                'positional_information':'VARCHAR',
                'mesh_terms':'VARCHAR'},
                ignore_errors=True)
    """
    )
    con.sql(""" CREATE OR REPLACE TABLE dp_num AS
        SELECT * FROM read_csv('.\\resources\\num_dp_fixed.csv', columns={'item_id':'VARCHAR',
        'item_label':'VARCHAR'},
        ignore_errors=True)

    """
    )
    con.sql(""" CREATE OR REPLACE TABLE st_num AS
        SELECT * FROM read_csv('.\\resources\\num_st_fixed.csv', columns={'item_id':'VARCHAR',
        'item_label':'VARCHAR'},
        ignore_errors=True)

    """
    )
    con.sql(""" CREATE OR REPLACE TABLE dp_vars AS
        SELECT * FROM read_csv_auto('.\\resources\\ACS5_DP.csv', header=True)

    """
    )
    con.sql(""" CREATE OR REPLACE TABLE st_vars AS
        SELECT * FROM read_csv_auto('.\\resources\\ACS5_ST.csv', header=True)

    """
    )
    con.sql(""" CREATE OR REPLACE VIEW dp_vw AS
        select dp_num.item_id
        ,dp_num.item_label
        ,dp_mm.id as mm_id
        ,dp_mm.mmi
        ,dp_mm.score
        ,dp_mm.umls_preferred_name
        ,dp_mm.cui
        ,dp_mm.semantic_type_list
        ,dp_mm.trigger_information
        ,dp_mm.location
        ,dp_mm.positional_information
        ,dp_mm.mesh_terms 
        from dp_num
        inner join dp_mm
            on dp_num.item_id = dp_mm.id
    """
    )
    con.sql(""" CREATE OR REPLACE VIEW dp_vars_to_nums_vw AS
        select dp_vars.var
        ,dp_vars.label
        ,dp_vars.concept
        ,dp_num.item_id
        ,dp_num.item_label
        from dp_vars
        left outer join dp_num
            on string_split(dp_vars.label,'!!')[-1] = dp_num.item_label
    """
    )