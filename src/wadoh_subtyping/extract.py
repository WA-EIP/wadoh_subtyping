import polars as pl
import yaml
import duckdb as dd
import pyodbc
from wadoh_subtyping import helpers
import wadoh_subtyping.transform as tf
from dataclasses import dataclass
import polars.selectors as cs
from pathlib import Path
from wadoh_raccoon.utils.helpers import save_raw_values
from sparkpl.converter import spark_to_polars
from databricks.sdk.runtime import *


@dataclass
class PullResult:
    respnet: pl.DataFrame
    respnet_wizard: pl.DataFrame
    respnet_investigation: pl.DataFrame
    con: dd.DuckDBPyConnection
    net_drive: str
    phl_df: pl.DataFrame
    received_submissions_df: pl.DataFrame
    base_cols: list

# create the types
@dataclass
class InternalTables:
    con: dd.DuckDBPyConnection
    net_drive: str
    redcap_linelist_drive: str


# create the types
@dataclass
class PHLTables:
    phl_df: pl.DataFrame 
    received_submissions_df: pl.DataFrame
    base_cols: list

# create the types
@dataclass
class WDRSTables:
    cases_joined: pl.DataFrame
    respnet_investigation: pl.DataFrame 
    respnet_wizard: pl.DataFrame

def internal_tables() -> InternalTables:
    """ Pull Internal Tables

    Usage
    -----
    Use this function when you need to connect to duckdb tables and pull internal network drives.

    Returns
    -------
    con: duckdb.connect
        a duckdb connection
    net_drive: str
        path to internal network drive
    redcap_linelist_drive: str
        path to internal drive containing linelists

    Examples
    --------
    
    ```python
    import src.subtype_link.utils.pulls as pulls

    # if you don't need the linelists
    con, net_drive, _ = pulls.internal_tables()

    # if you need the linelists
    con, net_drive, redcap_linelists = pulls.internal_tables()

    ```


    """
    with open("config.yaml") as f:
        config = yaml.load(f,Loader=yaml.SafeLoader)['default']
        net_drive = config['net_drive']
        redcap_linelist_drive = config['redcap_linelist_drive']

    con = dd.connect(f'{net_drive}/respnet.db')

    it = InternalTables(
        con=con,
        net_drive=net_drive,
        redcap_linelist_drive=redcap_linelist_drive
    )
    return it

def dbx_duckdb(duckdb_path):
    con = dd.connect(duckdb_path)
    return con


def phl_tables(query, dbx: bool=False) -> PHLTables:
    """ Read PHL

    Usage
    -----
    To be run after initialization of the data_processor class.
    This function reads in data from LIMS. It uses the LIMS connection found in Azure KeyVault and pulls in the subtyping table. 
    See the repo's README for more information on setting up the KeyVault connection. 

    Returns
    -------
    phl_df: pl.DataFrame
        a Polars dataframe containing PHL data
    received_submissions_df: pl.DataFrame
        a Polars dataframe containing a receipt of the PHL data in json format
    base_cols: list
        a list of col names used throughout the process for easy reference


    Examples
    --------
    
    ```python
    import src.subtype_link.utils.pulls as pulls

    phl_df, received_submissions_df, base_cols = pulls.phl_tables()
    ```

    it will save a receipt of the data as a json column in the received_submissions_tbl in duckdb

    ```python
    # make received submissions df
    # useful for collecting all submissions in the same format
    received_submissions_df = (
        helpers.save_raw_values(df_inp=phl,primary_kel_col="PHLAccessionNumber")
    )

    # unnest it to get the submission_number
    # subset out records from df_to_process_inp with submission number in list_to_fuzzy, assign to submissions_to_fuzzy_df
    phl_df = (
        received_submissions_df
        .unnest(pl.col("raw_inbound_submission"))
        .select(sorted(base_cols))
    )
    ```

    """

    if dbx is False:

        # Get LIMS db connection params from Az KV
        lims_server, lims_db, lims_trusted, lims_intent = helpers.get_secrets(
            ['lims-server', 
                'lims-db', 
                'lims-trusted', 
                'lims-intent']
                )

        # Establish connection to LIMS=
        conn_lims = pyodbc.connect(DRIVER='SQL Server Native Client 11.0',
                                SERVER=lims_server,
                                DATABASE=lims_db,
                                Trusted_Connection=lims_trusted,
                                ApplicationIntent=lims_intent)
        print('Connection to LIMS established:')

        phl = pl.read_database(
            query=query,
            connection=conn_lims
        )

    else: 

        spark_df = spark.sql(query)
        phl = spark_to_polars(spark_df)

    base_cols = ["submission_number", "internal_create_date"] + phl.columns 

    # make received submissions df
    # useful for collecting all submissions in the same format
    received_submissions_df = (
        save_raw_values(df_inp=phl,primary_key_col="PHLAccessionNumber")
    )

    # unnest it to get the submission_number
    # subset out records from df_to_process_inp with submission number in list_to_fuzzy, assign to submissions_to_fuzzy_df
    phl_df = (
        received_submissions_df
        .unnest(pl.col('raw_inbound_submission'))
        .select(sorted(base_cols))
    )

    pt = PHLTables(
        phl_df=phl_df,
        received_submissions_df=received_submissions_df,
        base_cols=base_cols
    )

    return pt

def wdrs_tables(cases_joined_query,respnet_investigation_query, respnet_wizard_query, dbx: bool=False) -> WDRSTables:
    """ Pull WDRS Tables

    Usage
    -----
    Call this to read in tables from WDRS.

    Returns
    -------
    cases: pl.DataFrame
        entire table demographics filtered to just be RESPNET cases
    respnet: pl.DataFrame
        respnet_investigation table

    Examples
    --------
    ```python
    import src.subtype_link.utils.pulls as pulls

    # pull in cases and respnet table
    cases, respnet = pulls.wdrs_tables()
    ```
    """

    if dbx is False:
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
        print('Connection to WDRS established:')

        cases_joined = pl.read_database(
            query=cases_joined_query,
            connection=conn_wdrs
        )

        # self.entire_table = pl.read_database(
        #     query=sqlqueries.pos_flu_entire_tbl,
        #     connection=conn_wdrs
        # )
        respnet_investigation = pl.read_database(
            query=respnet_investigation_query,
            connection=conn_wdrs
        )

        respnet_wizard = pl.read_database(
            query=respnet_wizard_query,
            connection=conn_wdrs
        )
    
    else: 
        cases_joined_df = spark.sql(cases_joined_query)
        cases_joined = spark_to_polars(cases_joined_df)

        respnet_investigation_df = spark.sql(respnet_investigation_query)
        respnet_investigation = spark_to_polars(respnet_investigation_df)

        respnet_wizard_df = spark.sql(respnet_wizard_query)
        respnet_wizard = spark_to_polars(respnet_wizard_df)

    wt = WDRSTables(
        cases_joined=cases_joined,
        respnet_investigation=respnet_investigation,
        respnet_wizard=respnet_wizard
    )

    return wt

def run_pulls(duckdb_path,cases_joined_query,respnet_investigation_query, respnet_wizard_query, query, dbx: bool=False):
    """ Pull All Data

    Usage
    -----
    Runs PHL, WDRS, and internal table pulls.
    You need access to Azure KeyVault to get the WDRS server info. See README for more info.

    Returns: Entire table and respnet tables

    Returns
    -------
    phl_df: pl.DataFrame
        a Polars dataframe containing PHL data
    received_submissions_df: pl.DataFrame
        a Polars dataframe containing a receipt of the PHL data in json format
    base_cols: list
        a list of col names used throughout the process for easy reference
    respnet: pl.DataFrame
        a Polars dataframe containing joined respnet tables
    respnet_wizard: pl.DataFrame
        a Polars dataframe containing the respnet wizard table
    respnet_investigation: pl.DataFrame
        a Polars dataframe containing the respnet investigation table
    con: duckdb.DuckDBPyConnection
        a duckdb connection, used to query internal tables
    net_drive: str
        the network drive path

    Examples
    --------
    Call in the class and it will execute this init function:

    ```python
    import src.subtype_link.processor as processor

    # ---- run processor ---- #
    # the processor contains general pulls and objects used in all processing (WDRS pulls, duckdb pull, etc)
    instance = processor.data_processor()
    ```

    Then you can call in specific objects if you want, like specific tables init read in:
    
    ```python
    result = processor.run_pulls()

    respnet_table = result.respnet_table
    ```

    """
    if dbx:

        con = dbx_duckdb(duckdb_path=duckdb_path)
        pt = phl_tables(query, dbx=True)
        wt = wdrs_tables(cases_joined_query,respnet_investigation_query, respnet_wizard_query, dbx=True)
        net_drive = ""
    
    else:
        it = internal_tables()
        con = it.con
        it.net_drive = net_drive
        pt = phl_tables(query, dbx=False)
        wt = wdrs_tables(cases_joined_query,respnet_investigation_query, respnet_wizard_query, dbx=False)

    # join the respnet tables together to get all possible cases
    # there are records in the wizard that aren't in investigation - they might not have a lab completed
    respnet = (
        wt.respnet_wizard
        .join(wt.respnet_investigation,how="left",on='CASE_ID')
        .rename({'SPECIMEN__COLLECTION__DTTM': 'SPECIMEN_COLLECTION_DTTM'})
    )

    return PullResult(
        respnet=respnet,
        respnet_wizard=wt.respnet_wizard,
        respnet_investigation=wt.respnet_investigation,
        con=con,
        net_drive=net_drive,
        phl_df=pt.phl_df,
        received_submissions_df=pt.received_submissions_df,
        base_cols=pt.base_cols
        )

def pull_manual_reviewed_files(pull_res):
    # first need to combine with rematch roster

    folder = Path(f"{pull_res.net_drive}/qa/completed_review/")

    any_csv = any(folder.glob("*.csv"))

    if any_csv:
        manual = pl.read_csv(f"{pull_res.net_drive}/qa/completed_review/*.csv")

        review = pull_res.con.sql('SELECT * FROM fuzzy_matched_review_tbl').pl()

        manual_reviewed = (
            review
            # join it to the full table to get the transformed columns
            .join(manual,how="inner",on="submission_number", suffix="_joined") # need suffix cus duplicate col names
            .select(cs.exclude(cs.contains("_joined"))) # remove the dup col names, keep the metadata
        )
    else: 
        manual_reviewed = pl.DataFrame()
        manual = pl.DataFrame()


    if len(manual_reviewed) > 0:
        print("\nAdd records that were reviewed")
        create_df_roster = (
            tf.create_roster(
                matched_and_transformed_df=manual_reviewed,
                respnet=pull_res.respnet
            )
        )
        reviewed_roster = (
            tf.dedup_roster(
                roster_inp=create_df_roster,
                reference_inp=pull_res.respnet
            )
        )
    else:
        reviewed_roster = pl.DataFrame()
    
    return reviewed_roster, manual_reviewed, manual, folder
