import polars as pl
import polars.selectors as cs
from sparkpl.converter import spark_to_polars

from wadoh_raccoon.utils import helpers
from wadoh_subtyping import transform as tf, qa

import utils

from pydantic import BaseModel
from databricks.sdk.runtime import *

# create the types
class InternalTables(BaseModel):
    rostered: pl.DataFrame

    model_config = {
        "arbitrary_types_allowed": True
    }

# create the types
class PHLTables(BaseModel):
    phl_df: pl.DataFrame 
    received_submissions_df: pl.DataFrame
    base_cols: list

    model_config = {
        "arbitrary_types_allowed": True
    }

def internal_tables() -> InternalTables:
    """ Pull Internal Tables
    """

    query = """
        SELECT * 
        FROM tc_aim_prod.diqa_sandbox.rostered_resp
    """

    spark_df = spark.sql(query)
    rostered = spark_to_polars(spark_df)
    
    it = InternalTables(
        rostered=rostered
    )
    return it

# create the types
class WDRSTables(BaseModel):
    respnet_investigation: pl.DataFrame 
    respnet_wizard: pl.DataFrame

    model_config = {
        "arbitrary_types_allowed": True
    }

def phl_tables() -> PHLTables:
    """ Read PHL

    Description
    -----------
    To be run inside databricks

    """
    
    query = """
        SELECT * 
        FROM `01_bronze_prod`.comp_data.vz_epi_micro_virology_influenza
        WHERE SpecimenDateCollected > '2024-01-01'
    """

    spark_df = spark.sql(query)
    phl = spark_to_polars(spark_df)

    base_cols = ["submission_number", "internal_create_date"] + phl.columns 

    # make received submissions df
    # useful for collecting all submissions in the same format
    received_submissions_df = (
        helpers.save_raw_values(df_inp=phl,primary_key_col="PHLAccessionNumber")
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

def wdrs_tables() -> WDRSTables:
    """ Pull WDRS Tables

    Usage
    -----
    Run this inside databricks
    """

    resp_inves = """
        SELECT *
        FROM fc_bronze_wdrs_prod.dbo.dd_gcd_respnet_investigation
        WHERE CODE = 'FLUHP';
    """

    resp_wiz = """
        SELECT *
        FROM fc_bronze_wdrs_prod.dbo.dd_gcd_respnet_wizard
        WHERE CODE = 'FLUHP';
    """

    resp_inves_df = spark.sql(resp_inves)
    respnet_investigation = spark_to_polars(resp_inves_df)

    resp_wiz_df = spark.sql(resp_wiz)
    respnet_wizard = spark_to_polars(resp_wiz_df)

    wt = WDRSTables(
        respnet_investigation=respnet_investigation,
        respnet_wizard=respnet_wizard
    )

    return wt
    