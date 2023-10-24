import duckdb
import pandas as pd

import os


def init_db():
    columns = {
        "id": "str",
        "mmi": "str",
        "score": "float",
        "umls_preferred_name": "str",
        "cui": "str",
        "semantic_type_list": "str",
        "trigger_information": "str",
        "location": "str",
        "positional_information": "str",
        "mesh_terms": "str",
    }

    mm_dp = pd.read_csv(
        os.path.join("resources", "mm_dp_fixed.csv"),
        header=0,
        dtype=columns,
        names=columns.keys(),
        on_bad_lines="skip",
    )
    duckdb.sql("CREATE OR REPLACE TABLE dp_mm AS SELECT * FROM mm_dp")

    mm_st = pd.read_csv(
        os.path.join("resources", "mm_st_fixed.csv"),
        header=0,
        dtype=columns,
        names=columns.keys(),
        on_bad_lines="skip",
    )
    duckdb.sql("CREATE OR REPLACE TABLE mm_st AS SELECT * FROM mm_st")

    num_dp = pd.read_csv(
        os.path.join("resources", "num_dp_fixed.csv"),
        header=0,
        names=["item_id", "item_label"],
        on_bad_lines="skip",
    )
    duckdb.sql("CREATE OR REPLACE TABLE dp_num AS SELECT * FROM num_dp")

    num_st = pd.read_csv(
        os.path.join("resources", "num_st_fixed.csv"),
        header=0,
        names=["item_id", "item_label"],
        on_bad_lines="skip",
    )
    duckdb.sql("CREATE OR REPLACE TABLE st_num AS SELECT * FROM num_st")

    acs5_dp = pd.read_csv(os.path.join("resources", "ACS5_DP.csv"), on_bad_lines="skip")
    duckdb.sql("CREATE OR REPLACE TABLE dp_vars AS SELECT * from acs5_dp")

    acs5_st = pd.read_csv(os.path.join("resources", "ACS5_ST.csv"), on_bad_lines="skip")
    duckdb.sql("CREATE OR REPLACE TABLE st_vars AS SELECT * from acs5_st")

    duckdb.sql(
        """ CREATE OR REPLACE VIEW dp_vw AS
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
    duckdb.sql(
        """ CREATE OR REPLACE VIEW dp_vars_to_nums_vw AS
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
    duckdb.sql(
        """
         CREATE OR REPLACE MACRO dp_var_lookup(pattern) AS TABLE (
         WITH item_label_matches AS (
            SELECT DISTINCT mm_id
            ,cui
            ,score
            ,ROW_NUMBER() OVER (PARTITION BY mm_id ORDER BY score DESC) AS cui_score_rank
            FROM dp_vw
            WHERE regexp_matches(item_label, pattern)
        )
        select var, item_label
        from item_label_matches
        inner join dp_vars_to_nums_vw
            on item_label_matches.mm_id = dp_vars_to_nums_vw.item_id
        where regexp_matches(dp_vars_to_nums_vw.var, 'DP\w+\_\d+E')
        and cui_score_rank=1);
    """
    )


if __name__ == "__main__":
    init_db()
