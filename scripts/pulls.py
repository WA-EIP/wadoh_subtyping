import polars as pl
import polars.selectors as cs

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

    # spark_df = spark.sql(
    #     """
    #     SELECT * 
    #     FROM tc_aim_prod.diqa_sandbox.rostered_resp
    #     """
    # )

    # rostered = pl.from_pandas(spark_df.toPandas())

    query = """
        SELECT * 
        FROM tc_aim_prod.diqa_sandbox.rostered_resp
    """

    rostered = utils.uc_catalog_to_polars(spark=spark,query=query)
    
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
    # spark_df = spark.sql(
    #     """
    #     SELECT * 
    #     FROM `01_bronze_prod`.comp_data.vz_epi_micro_virology_influenza
    #     WHERE SpecimenDateCollected > '2024-01-01'
    #     """
    # )

    # phl = pl.from_pandas(spark_df.toPandas())

    query = """
        SELECT * 
        FROM `01_bronze_prod`.comp_data.vz_epi_micro_virology_influenza
        WHERE SpecimenDateCollected > '2024-01-01'
    """

    phl = utils.uc_catalog_to_polars(spark=spark,query=query)

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

    # spark_resp_inves = spark.sql(
    #     """
    #     SELECT *
    #     FROM fc_bronze_wdrs_prod.dbo.dd_gcd_respnet_investigation
    #     WHERE CODE = 'FLUHP';
    #     """
    # )

    # spark_resp_wiz = spark.sql(
    #     """
    #     SELECT *
    #     FROM fc_bronze_wdrs_prod.dbo.dd_gcd_respnet_wizard
    #     WHERE CODE = 'FLUHP';
    #     """
    # )

    # respnet_investigation = pl.from_pandas(spark_resp_inves.toPandas())

    # respnet_wizard = pl.from_pandas(spark_resp_wiz.toPandas())

    resp_inves = """
        SELECT *
        FROM fc_bronze_wdrs_prod.dbo.dd_gcd_respnet_investigation
        WHERE CODE = 'FLUHP';
    """

    spark_resp_wiz = """
        SELECT *
        FROM fc_bronze_wdrs_prod.dbo.dd_gcd_respnet_wizard
        WHERE CODE = 'FLUHP';
    """

    respnet_investigation = utils.uc_catalog_to_polars(spark=spark,query=resp_inves)

    respnet_wizard = utils.uc_catalog_to_polars(spark=spark,query=spark_resp_wiz)


    wt = WDRSTables(
        respnet_investigation=respnet_investigation,
        respnet_wizard=respnet_wizard
    )

    return wt
    