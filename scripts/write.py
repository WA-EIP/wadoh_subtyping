import polars as pl
import polars.selectors as cs
from pyspark.sql import functions as F
from sparkpl.converter import spark_to_polars, polars_to_spark

from wadoh_raccoon.utils import helpers
from wadoh_subtyping import transform as tf, qa

import utils

from pydantic import BaseModel
from databricks.sdk.runtime import *

def write_phl(pull_res,w_res,main_res):
    """ Write PHL

    Usage
    -----
    To be used after processor.match_phl() function.
    This function will write dataframes out to duckdb tables.    
    """
    if len(w_res.qa_issues) > 0:
        # append a copy of these records to received_submissions so we have a raw data snapshot
        
        # Convert to temp table so spark can join them
        spark_df = polars_to_spark(w_res.qa_issues)
        spark_df.createOrReplaceTempView("qa_issues_temp")

        print(f'{len(w_res.qa_issues)} QA issues found. Writing them to qa_tbl.')
        utils.safe_insert_sql_uc(
            spark=spark,
            catalog='tc_aim_prod',
            schema='diqa_sandbox',
            table='qa_tbl_resp',
            source_view='qa_issues_temp',
            join_key='submission_number'
        )
        

    if len(pull_res.received_submissions_df) > 0:
        # append a copy of these records to received_submissions so we have a raw data snapshot
        
        # utils.polars_to_spark_temp_view(spark,pull_res.received_submissions_df,'received_submissions_df')
        spark_df = polars_to_spark(pull_res.received_submissions_df)
        spark_df.createOrReplaceTempView("received_submissions_df")
        utils.safe_insert_sql_uc(
            spark=spark,
            catalog='tc_aim_prod',
            schema='diqa_sandbox',
            table='received_submissions_resp',
            source_view='received_submissions_df',
            join_key='submission_number'
        )

    if len(main_res.fuzzy_unmatched_full) > 0:

        no_match_df = (
            main_res.fuzzy_unmatched_full
            .select([
                'submission_number',
                'internal_create_date',
                'submitted_dob',
                'submitted_collection_date',
                'reference_collection_date',
                'first_name_clean',
                'last_name_clean',
                'WDRS_RESULT',
                'WDRS_TEST_PERFORMED',
                'WDRS_SPECIMEN_SOURCE',
                'WDRS_SPECIMEN_TYPE',
                'SPECIMEN_TYPE',
                'SPECIMEN_COLLECTION_DTTM',
                'SPECIMEN_RECEIVED_DTTM',
                'SUBMITTER',
                'SUBMITTER_OTHER',
                'WDRS_PERFORMING_ORG',
                'WDRS_RESULT_SUMMARY',
                'SPECIMEN_SOURCE_SITE',
                'PERFORMING_LAB_ENTIRE_REPORT',
                'TEST_PERFORMED_DESC',
                'TEST_RESULT'
            ])
            # add match_found col for table
            .with_columns(
                match_found = False
            )
        )
        print('Appending records into duckdb no_match_tbl')
        # utils.polars_to_spark_temp_view(spark,no_match_df,'no_match_df')
        spark_df = polars_to_spark(no_match_df)
        spark_df.createOrReplaceTempView("no_match_df")
        utils.safe_insert_sql_uc(
            spark=spark,
            catalog='tc_aim_prod',
            schema='diqa_sandbox',
            table='unmatched_resp',
            source_view='no_match_df',
            join_key='submission_number'
        )

    if len(main_res.fuzzy_matched_full) > 0:

        # utils.polars_to_spark_temp_view(spark,main_res.fuzzy_matched_full,'fuzzy_matched_df')
        spark_df = polars_to_spark(main_res.fuzzy_matched_full)
        spark_df.createOrReplaceTempView("fuzzy_matched_df")
        utils.safe_insert_sql_uc(
            spark=spark,
            catalog='tc_aim_prod',
            schema='diqa_sandbox',
            table='fuzzy_matched_resp',
            source_view='fuzzy_matched_df',
            join_key='submission_number'
        )

    if len(main_res.no_demo_full) > 0:

        # utils.polars_to_spark_temp_view(spark,main_res.no_demo_full,'no_demo_df')
        spark_df = polars_to_spark(main_res.no_demo_full)
        spark_df.createOrReplaceTempView("no_demo_df")
        utils.safe_insert_sql_uc(
            spark=spark,
            catalog='tc_aim_prod',
            schema='diqa_sandbox',
            table='no_demo_resp',
            source_view='no_demo_df',
            join_key='submission_number'
        )

    if len(main_res.exact_matched_full) > 0:

        # utils.polars_to_spark_temp_view(spark,main_res.exact_matched_full,'exact_matched_df')
        spark_df = polars_to_spark(main_res.exact_matched_full)
        spark_df.createOrReplaceTempView("exact_matched_df")
        utils.safe_insert_sql_uc(
            spark=spark,
            catalog='tc_aim_prod',
            schema='diqa_sandbox',
            table='exact_matched_resp',
            source_view='exact_matched_df',
            join_key='submission_number'
        )

def write_rematch(rematch_res,pull_res):
    # Update the no_match_tbl in duckdb where matches were found
    # I'm too sketched to remove records, so just relabel them as match_found True or False
    if len(rematch_res.rematch_fuzzy_matched) > 0 or len(rematch_res.rematch_exact_matched) > 0:

        # get a list of previous no_match that had found a match this time around
        match_found = list(rematch_res.rematch_fuzzy_matched['submission_number']) + list(rematch_res.rematch_exact_matched['submission_number'])
        
        print('Tagging rows from no_match_tbl that did find a match')
        # mark that the match has been found in the no_match_tbl (im afraid to remove them for record keeping purposes)

        no_match_df = (
            rematch_res.no_match_reprocess
            .with_columns(
                pl.when(pl.col('submission_number').is_in(match_found))
                .then(True)
                .otherwise(False)
                .alias('match_found')
            )
            .select([
                'submission_number',
                'internal_create_date',
                'submitted_dob',
                'submitted_collection_date',
                'reference_collection_date',
                'first_name_clean',
                'last_name_clean',
                'WDRS_RESULT',
                'WDRS_TEST_PERFORMED',
                'WDRS_SPECIMEN_SOURCE',
                'WDRS_SPECIMEN_TYPE',
                'SPECIMEN_TYPE',
                'SPECIMEN_COLLECTION_DTTM',
                'SPECIMEN_RECEIVED_DTTM',
                'SUBMITTER',
                'SUBMITTER_OTHER',
                'WDRS_PERFORMING_ORG',
                'WDRS_RESULT_SUMMARY',
                'SPECIMEN_SOURCE_SITE',
                'PERFORMING_LAB_ENTIRE_REPORT',
                'TEST_PERFORMED_DESC',
                'TEST_RESULT'
            ])
        )

        # temporary register dataframe into duckdb
        # utils.polars_to_spark_temp_view(
        #     spark,
        #     no_match_df,
        #     'no_match_df'
        # )

        no_match_spark = polars_to_spark(no_match_df)

        print('Updating no_match_tbl where rows have a match found')
        updated_df = (
            spark.table("tc_aim_prod.diqa_sandbox.unmatched_resp")
            .alias("t")
            .join(no_match_spark.alias("n"), on="submission_number", how="left")
            .withColumn("match_found", F.coalesce(F.col("n.match_found"), F.col("t.match_found")))
            .select("t.*")  # Keep original columns with updated match_found
        )
        # write to UC
        updated_df.write.mode("overwrite").saveAsTable("tc_aim_prod.diqa_sandbox.unmatched_resp")



    if len(rematch_res.rematch_fuzzy_matched) > 0:

        # utils.polars_to_spark_temp_view(
        #     spark,
        #     rematch_res.rematch_fuzzy_matched,
        #     'rematch_matched_df'
        # )
        spark_df = polars_to_spark(rematch_res.rematch_fuzzy_matched)
        spark_df.createOrReplaceTempView("rematch_matched_df")
        
        print('Appending rematch fuzzy match found to review table')

        utils.safe_insert_sql_uc(
            spark=spark,
            catalog='tc_aim_prod',
            schema='diqa_sandbox',
            table='fuzzy_review_resp',
            source_view='rematch_matched_df',
            join_key='submission_number'
        )
    if len(rematch_res.rematch_exact_matched) > 0:
        print('Appending rematch match found to fuzzy_matched_roster_tbl')
        # utils.polars_to_spark_temp_view(
        #     spark,
        #     rematch_res.rematch_exact_matched,
        #     'rematch_exact_matched_df'
        # )

        spark_df = polars_to_spark(rematch_res.rematch_exact_matched)
        spark_df.createOrReplaceTempView("rematch_exact_matched_df")
        
        utils.safe_insert_sql_uc(
            spark=spark,
            catalog='tc_aim_prod',
            schema='diqa_sandbox',
            table='rostered_resp',
            source_view='rematch_exact_matched_df',
            join_key='submission_number'
        )
