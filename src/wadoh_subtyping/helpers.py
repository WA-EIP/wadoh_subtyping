import polars as pl
import polars.selectors as cs

import redcap
import yaml
from databricks.sdk import WorkspaceClient 
import base64 

def get_secrets(keys, config_path):
    """ get secret

    Usage
    -----
    To be used if you're running scripts on a local machine (not in the cloud).
    It will pull secrets from databricks secret scopes, such as db connections and file paths.

    Examples
    --------
    ```python
    # Get WDRS connection params from Az KV
    wdrs_server, wdrs_db, wdrs_trusted, wdrs_intent = helpers.get_secrets(
        ['wdrs-server', 
            'wdrs-db', 
            'wdrs-trusted', 
            'wdrs-intent']
            )

    # Establish connection to WDRS=
    conn_wdrs = pyodbc.connect(DRIVER='SQL Server Native Client 11.0',
                            SERVER=wdrs_server,
                            DATABASE=wdrs_db,
                            Trusted_Connection=wdrs_trusted,
                            ApplicationIntent=wdrs_intent)

    ```
    """
    with open(config_path) as f:
        config = yaml.load(f,Loader=yaml.SafeLoader)['default']
        scope = config['dbx_scope']

    w = WorkspaceClient()  # picks up configured PAT automatically 

    def _get_one(key):
        secret = w.secrets.get_secret(scope=scope, key=key)
        return base64.b64decode(secret.value).decode("utf-8")

    # Single key
    if isinstance(keys, str):
        return _get_one(keys)

    # Multiple keys
    return tuple(_get_one(key) for key in keys)

def get_query_from_file(filename):
    with open(filename, 'r') as f:
        return f.read()

def safe_insert(con, table_name: str, source_view: str, join_key: str):
    # Get column names from the target table
    cols = [row[1] for row in con.sql(f"PRAGMA table_info({table_name})").fetchall()]
    
    insert_cols = ", ".join(cols)               # unqualified for INSERT
    select_cols = ", ".join([f"r.{col}" for col in cols])  # qualified for SELECT

    sql = f"""
    INSERT INTO {table_name} ({insert_cols})
    SELECT {select_cols}
    FROM {source_view} AS r
    LEFT OUTER JOIN {table_name} AS rt
    ON rt.{join_key} = r.{join_key}
    WHERE rt.{join_key} IS NULL;
    """
    con.sql(sql)

def align_to_duckdb(df, duck_cols):
    """
    to get the duck_cols, run
    con.sql('SELECT * FROM table LIMIT 0').pl().columns
    see https://github.com/pola-rs/polars/issues/6895 for more thoughts
    
    """
    df = df.select([c for c in df.columns if c in duck_cols])
    for col in duck_cols:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col))
    return df.select(duck_cols)

def fill_redcap_subtypes(roster_inp,pull_res):

    with open("config.yaml") as f:
        config = yaml.load(f,Loader=yaml.SafeLoader)['default']
        redcap_token = config['redcap_token']
        redcap_url = config['redcap_url']
        # dev_redcap_token = config['dev_redcap_token']

    project = redcap.Project(redcap_url,redcap_token)

    records = project.export_records(format_type='json',raw_or_label='label')
    redcap_data = pl.DataFrame(records)

    # get redcap columns
    join_roster_to_redcap = (
        roster_inp
        .join(redcap_data,left_on="CaseID",right_on="wdrs_case_id", how='inner') # do an inner join, if a record is not in redcap, leave it out
    )

    # get multiple rows for each case id and lab block 1-4
    lab_number_df = (
        join_roster_to_redcap
        .unpivot(cs.contains(('poststdt','tsttyp','tstres','hosplab')),index='CaseID')
        .with_columns([
            pl.col("variable")
            .str.extract(r"([a-zA-Z]+)")
            .alias("field"),

            pl.col("variable")
            .str.extract(r"(\d+)")
            .cast(pl.Int64)
            .alias("lab_number")
        ])
        .drop('variable')
        .pivot(
            values="value",
            index=["CaseID", "lab_number"],
            on="field",
            aggregate_function='first'
        )
        .drop(cs.contains(('covid','rsv')))
    )


    group_by_case_id = (
        lab_number_df
        # .join(roster.select(['CaseID']),left_on='wdrs_case_id',right_on='CaseID',how='left')
        # .filter(pl.col('wdrs_case_id').is_in(roster['CaseID']))
        .group_by('CaseID')
        .agg(
            pl.col("lab_number")
            .filter(
                ((pl.col("hosplab").is_null()) | (pl.col("hosplab")=="") ) & 
                ((pl.col("poststdt").is_null()) | (pl.col("poststdt")==""))
            )
            .min()
            .alias("first_empty_lab")
        )
        # join back to get the redcap columns
        .join(lab_number_df,on='CaseID',how='inner')
        # join back to get the roster columns
        .join(roster_inp,left_on='CaseID',right_on='CaseID',how='inner')

        # none of them should be the first lab inputted, so remove any where it's the first
        .filter(pl.col('first_empty_lab')!=1)

    )


    final = (
        group_by_case_id
        # now remove the flattened cols so we can just take single cases
        .drop(cs.contains(('poststdt','tsttyp','tstres','hosplab','lab_number')))

        # format the cols
        .with_columns(
            pl.when(pl.col('WDRS_TEST_PERFORMED')=='G_PCR').then(2)
            .when(pl.col('WDRS_TEST_PERFORMED')=='G_RAPID_TEST_EIA').then(1)
            .when(pl.col('WDRS_TEST_PERFORMED')=='G_VIRAL_CULTURE').then(3)
            .when(pl.col('WDRS_TEST_PERFORMED')=='G_DFA_IHC').then(5)
            .when(pl.col('WDRS_TEST_PERFORMED')=='G_OTHER').then(6)
            .otherwise(None)
            .alias('wdrs_test_performed2'),

            pl.when(pl.col("WDRS_RESULT") == "G_FLU_A_(09_PDM_H1N1)_D").then(5)
            .when(pl.col("WDRS_RESULT") == "G_FLU_A_(H3)_D").then(8)
            .when(pl.col("WDRS_RESULT") == "G_FLU_A_(H3N2)_D").then(12)
            .when(pl.col("WDRS_RESULT") == "G_FLU_A_(H5)_D").then(11)
            .when(pl.col("WDRS_RESULT") == "G_FLU_A_(H7)_D").then(11)
            .when(pl.col("WDRS_RESULT") == "G_FLU_A_D").then(1)
            .when(pl.col("WDRS_RESULT") == "G_FLU_A_OTHER_D").then(1)
            .when(pl.col("WDRS_RESULT") == "G_FLU_B_(VICTORIA)_D").then(14)
            .when(pl.col("WDRS_RESULT") == "G_FLU_B_(YAMAGATA)_D").then(13)
            .when(pl.col("WDRS_RESULT") == "G_FLU_B_D").then(2)
            .when(pl.col("WDRS_RESULT") == "G_FLU_B_OTHER_D").then(2)
            .when(pl.col("WDRS_RESULT") == "G_FLU_A/B").then(4)
            .when(pl.col("WDRS_RESULT") == "G_OTHER").then(11)
            .otherwise(None)
            .alias("wdrs_result2")
        )

        # create the tsttyp1-4 column. for 1-4, fill the col based on first_empty_lab
        .with_columns([
            pl.when(pl.col("first_empty_lab") == i)
            .then(pl.col("wdrs_test_performed2"))
            # .otherwise(pl.col(f"tsttyp{i}"))
            .alias(f"tsttyp{i}")
            for i in range(1, 5)
        ])
        .with_columns([
            pl.when(pl.col("first_empty_lab") == i)
            .then(pl.col("SPECIMEN_COLLECTION_DTTM"))
            .alias(f"poststdt{i}")
            for i in range(1, 5)
        ])
        .with_columns([
            pl.when(pl.col("first_empty_lab") == i)
            .then(pl.col("wdrs_result2"))
            .alias(f"tstres{i}")
            for i in range(1, 5)
        ])
        .with_columns([
            pl.when(pl.col("first_empty_lab") == i)
            .then(pl.lit("WAPHL"))
            .alias(f"hosplab{i}")
            for i in range(1, 5)
        ])

        .unique()

        # gotta change col type to join back to wdrs data.. :)

        # join to get the RESPNET_CASE_ID
        .join(
            (
                pull_res.respnet
                .select(['CASE_ID','RESPNET_CASE_ID'])
                .with_columns(pl.col('CASE_ID').cast(pl.String))
            ),
            left_on='CaseID',
            right_on='CASE_ID',
            how='inner'
        )
        .with_columns(
            wdrs_case_id=pl.col('CaseID'),
            party_external_id=pl.col('PARTY_EXTERNAL_ID'),
            caseid=pl.col('RESPNET_CASE_ID'),
        )
        # now just select the appropriate columns
        .select('wdrs_case_id','party_external_id','caseid',cs.contains(('poststdt','tsttyp','tstres','hosplab','lab_number')))
        .unique()
    )

    final_json = (
        final
        .with_columns(cs.date().cast(pl.String))
        .to_dicts()
    )

    return final_json, final, project

def import_to_redcap(final_json_inp,project_inp):
    project = project_inp

    project.import_records(
        to_import=final_json_inp,
        import_format='json'
    )
