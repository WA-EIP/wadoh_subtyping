import polars as pl
import polars.selectors as cs

from wadoh_raccoon.utils import helpers
from wadoh_raccoon import dataframe_matcher
from wadoh_subtyping import transform as tf, qa
import pulls
import utils

from pydantic import BaseModel
from dataclasses import dataclass
from databricks.sdk.runtime import *

@dataclass
class PullResult:
    respnet: pl.DataFrame
    respnet_wizard: pl.DataFrame
    respnet_investigation: pl.DataFrame
    phl_df: pl.DataFrame
    received_submissions_df: pl.DataFrame
    base_cols: list
    rostered: pl.DataFrame

@dataclass
class WrangleResult:
    transformed_df: pl.DataFrame
    qa_issues: pl.DataFrame 
    no_qa_issues: pl.DataFrame 
    submissions_to_fuzzy: pl.DataFrame

@dataclass
class MainResult:
    pull_res: PullResult
    w_res: WrangleResult
    exact_matched: pl.DataFrame
    no_demo: pl.DataFrame
    fuzzy_unmatched: pl.DataFrame
    fuzzy_matched: pl.DataFrame
    exact_matched_full: pl.DataFrame
    no_demo_full: pl.DataFrame
    fuzzy_unmatched_full: pl.DataFrame
    fuzzy_matched_full: pl.DataFrame

@dataclass
class RematchResult:
    no_match_reprocess: pl.DataFrame
    rematch_exact_matched: pl.DataFrame
    rematch_no_demo: pl.DataFrame
    rematch_fuzzy_unmatched: pl.DataFrame
    rematch_fuzzy_matched: pl.DataFrame


def run_pulls():
    """ Pull All Data
    """
    it = pulls.internal_tables()
    pt = pulls.phl_tables()
    wt = pulls.wdrs_tables()

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
        phl_df=pt.phl_df,
        received_submissions_df=pt.received_submissions_df,
        base_cols=pt.base_cols,
        rostered=it.rostered
    )

def wrangle_phl(pull_res):
    """ Wrangle PHL

    Usage
    -----
    Run inside of databricks

    """

    # transformations and qa
    transformed_df_temp = (
        tf.transform(df=pull_res.phl_df)
        .with_columns(
            pl.col('PatientBirthDate').dt.date(),
            pl.col('SpecimenDateCollected').dt.date()
        )
    )

    # filter out the ones already pushed to WDRS
    old = pull_res.rostered

    
    transformed_df_filtered = (
        transformed_df_temp
        .join(
            old,
            left_on=['submission_number', 'SPECIMEN_COLLECTION_DTTM'],
            right_on=['submission_number', 'submitted_collection_date'],
            how='left'
        )
        .filter((pl.col('CASE_ID').is_null()))
    )


    # config for col names
    wdrs_res_output="WDRS_RESULT"
    wdrs_res_sum_output="WDRS_RESULT_SUMMARY"
    test_res_output="TEST_RESULT"

    qa_mult_subtypes = (
        qa.qa_multiple_subtypes(
            transformed_df_inp=transformed_df_filtered,
            wdrs_res_sum_output=wdrs_res_sum_output,
            test_res_output=test_res_output,
            wdrs_res_output=wdrs_res_output
        )
    )

    apply_qa = (
        transformed_df_filtered
        .with_columns(
            # qa.qa_wdrs_test_performed(wdrs_test_perf_output=wdrs_test_perf_output),
            # qa.qa_test_performed_desc(test_perf_output=test_perf_output),
            qa.qa_wdrs_result(wdrs_res_output=wdrs_res_output),
            qa.qa_wdrs_result_summary(wdrs_res_sum_output=wdrs_res_sum_output),
            pl.when(pl.col('submission_number').is_in(qa_mult_subtypes['submission_number']))
            .then(True)
            .otherwise(False)
            .alias('qa_multiple_subtypes')
        )
        .with_columns(
            pl.when(pl.any_horizontal(cs.contains('qa_')))
            .then(True)
            .otherwise(False)
            .alias('qa_flag')
        )
    )

    # run qa

    # filter out QA records and put into a qa table. still need to make a QA table
    # filter where qa_flag is False 
    no_qa_issues = (
        apply_qa
        .filter(pl.col('qa_flag').not_())
        .select(~cs.contains('qa_'))
        # remove the negative records
        .filter(pl.col('WDRS_RESULT_SUMMARY')!='G_NEGATIVE')
        .filter(pl.col('WDRS_RESULT')!='G_FLU_ND')
    )
    # filter where qa_flag is True
    qa_issues = (
        apply_qa
        .filter((pl.col('qa_flag')))
        .filter(pl.col('WDRS_RESULT_SUMMARY')!='G_NEGATIVE')
        .select([
            "submission_number",
            "internal_create_date",
            "WDRS_RESULT",
            "WDRS_TEST_PERFORMED",
            "WDRS_SPECIMEN_SOURCE",
            "SPECIMEN_SOURCE_SITE",
            "WDRS_SPECIMEN_TYPE",
            "SPECIMEN_TYPE",
            "SPECIMEN_COLLECTION_DTTM",
            "SPECIMEN_RECEIVED_DTTM",
            "SUBMITTER",
            "WDRS_PERFORMING_ORG",
            "WDRS_RESULT_SUMMARY",
            "TEST_PERFORMED_DESC",
            "TEST_RESULT",
            "PERFORMING_LAB_ENTIRE_REPORT",
            "SUBMITTER_OTHER",
            cs.contains('qa_')
        ])
    ) 

    # only take the names to match. will join back to transformed_df to get the full data
    submissions_to_fuzzy = (
        no_qa_issues 
        .unique(subset=['submission_number'])
        .select([
            'submission_number',
            'internal_create_date',
            'PatientFirstName',
            'PatientLastName',
            'PatientBirthDate',
            'SpecimenDateCollected'
        ])
    )

    return WrangleResult(
        transformed_df=transformed_df_filtered,
        qa_issues=qa_issues, 
        no_qa_issues=no_qa_issues, 
        submissions_to_fuzzy=submissions_to_fuzzy
    )

def match_phl():
    """ Match PHL

    Usage
    -----
    To be called after the pulls and wrangle functions.
    
    """

    pull_res = run_pulls()
    
    if len(pull_res.phl_df) == 0:
        raise ValueError("Expected non-empty PHL DataFrame. Exit program. Bleep Blop")

    w_res = wrangle_phl(pull_res=pull_res)
    
    if len(pull_res.phl_df) == 0:
        raise ValueError("Expected non-empty PHL DataFrame. Exit program. Bleep Blop")

    # w_res = wrangle_res
    
    instance = dataframe_matcher.DataFrameMatcher(
        df_subm=w_res.submissions_to_fuzzy,
        df_ref=pull_res.respnet,

        first_name_ref='FIRST_NAME',
        last_name_ref='LAST_NAME',
        dob_ref='BIRTH_DATE',
        spec_col_date_ref='SPECIMEN_COLLECTION_DTTM',

        first_name_src='PatientFirstName',
        last_name_src='PatientLastName',
        dob_src='PatientBirthDate',
        spec_col_date_src='SpecimenDateCollected',
        
        key='submission_number',
        threshold=80
    )

    match_res = instance.match()
    
    # get the original transformed columns
    # need these because i want the fuzzy functions to not need to deal with any extra columns
    # things get confusing enough with the columns that fuzzy produces itself, so make it as simple as possible,
    # then join back the original data after.
    # this should help with carrying fuzzy over to other pathogen workflows too
    og_data = (
        w_res.no_qa_issues
        .select([
            'submission_number',
            # 'internal_create_date', # don't need this since it will get brought in from the other data
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
            'TEST_RESULT',
            cs.contains('qa_')
        ])
    )

    if len(match_res.exact_matched) > 0:
        # rejoin matches to roster dataframe
        exact_matched_full = (
            match_res.exact_matched.with_columns(pl.col('submission_number').cast(pl.String)) # need to cast as str since it freaks out with null col joins
            .join(
                og_data,
                on='submission_number',
                how='left'
            )
        )
    else:
        exact_matched_full = pl.DataFrame()

    if len(match_res.fuzzy_matched) > 0:
        # rejoin matches to roster dataframe
        fuzzy_matched_full = (
            match_res.fuzzy_matched.with_columns(pl.col('submission_number').cast(pl.String)) # need to cast as str since it freaks out with null col joins
            .join(
                og_data,
                on='submission_number',
                how='left'
            )
        )
    else:
        fuzzy_matched_full = pl.DataFrame()

    if len(match_res.fuzzy_unmatched) > 0:
        # rejoin matches to no match dataframe
        fuzzy_unmatched_full = (
            match_res.fuzzy_unmatched.with_columns(pl.col('submission_number').cast(pl.String)) # need to cast as str since it freaks out with null col joins
            .join(
                og_data,
                on='submission_number',
                how='left'
            )
        )
    else:
        fuzzy_unmatched_full = pl.DataFrame()
    
    if len(match_res.no_demo) > 0:
        no_demo_full = (
            match_res.no_demo.with_columns(pl.col('submission_number').cast(pl.String)) # need to cast as str since it freaks out with null col joins
            .join(
                og_data,
                on='submission_number',
                how='left'
            )
        )
    else:
        no_demo_full = pl.DataFrame()

    return MainResult(
        pull_res=pull_res,
        w_res=w_res,
        exact_matched=match_res.exact_matched,
        no_demo=match_res.no_demo,
        fuzzy_unmatched=match_res.fuzzy_unmatched, 
        fuzzy_matched=match_res.fuzzy_matched,
        exact_matched_full=exact_matched_full,
        no_demo_full=no_demo_full, 
        fuzzy_unmatched_full=fuzzy_unmatched_full, 
        fuzzy_matched_full=fuzzy_matched_full
    )

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
        utils.polars_to_spark_temp_view(spark,w_res.qa_issues,'qa_issues_temp')

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
        
        utils.polars_to_spark_temp_view(spark,pull_res.received_submissions_df,'received_submissions_df')
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
        utils.polars_to_spark_temp_view(spark,no_match_df,'no_match_df')
        utils.safe_insert_sql_uc(
            spark=spark,
            catalog='tc_aim_prod',
            schema='diqa_sandbox',
            table='unmatched_resp',
            source_view='no_match_df',
            join_key='submission_number'
        )

    if len(main_res.fuzzy_matched_full) > 0:

        utils.polars_to_spark_temp_view(spark,main_res.fuzzy_matched_full,'fuzzy_matched_df')
        utils.safe_insert_sql_uc(
            spark=spark,
            catalog='tc_aim_prod',
            schema='diqa_sandbox',
            table='fuzzy_matched_resp',
            source_view='fuzzy_matched_df',
            join_key='submission_number'
        )

    if len(main_res.no_demo_full) > 0:

        utils.polars_to_spark_temp_view(spark,main_res.no_demo_full,'no_demo_df')
        utils.safe_insert_sql_uc(
            spark=spark,
            catalog='tc_aim_prod',
            schema='diqa_sandbox',
            table='no_demo_resp',
            source_view='no_demo_df',
            join_key='submission_number'
        )

    if len(main_res.exact_matched_full) > 0:

        utils.polars_to_spark_temp_view(spark,main_res.exact_matched_full,'exact_matched_df')
        utils.safe_insert_sql_uc(
            spark=spark,
            catalog='tc_aim_prod',
            schema='diqa_sandbox',
            table='exact_matched_resp',
            source_view='exact_matched_df',
            join_key='submission_number'
        )

def rematch(pull_res):

    query = """
        SELECT * 
        FROM tc_aim_prod.diqa_sandbox.unmatched_resp 
        WHERE match_found = FALSE
    """
    no_match_reprocess_base = utils.uc_catalog_to_polars(spark=spark,query=query)

    no_match_reprocess = no_match_reprocess_base.rename(
        {"first_name_clean": "first_name_clean_temp",
         "last_name_clean": "last_name_clean_temp",
         "submitted_dob": "submitted_dob_temp",
         "submitted_collection_date": "submitted_collection_date_temp"
        }
    )

    submissions_to_fuzzy = (
        no_match_reprocess
        .unique(subset='submission_number')
    )

    instance = dataframe_matcher.DataFrameMatcher(
        df_subm=submissions_to_fuzzy,
        df_ref=pull_res.respnet,

        first_name_ref='FIRST_NAME',
        last_name_ref='LAST_NAME',
        dob_ref='BIRTH_DATE',
        spec_col_date_ref='SPECIMEN_COLLECTION_DTTM',

        first_name_src='first_name_clean_temp',
        last_name_src='last_name_clean_temp',
        dob_src='submitted_dob_temp',
        spec_col_date_src='submitted_collection_date_temp',
        
        key='submission_number',
        threshold=80
    )

    match_res = instance.match()

    # rename them
    rematch_exact_matched = match_res.exact_matched
    rematch_no_demo = match_res.no_demo
    rematch_fuzzy_unmatched = match_res.fuzzy_unmatched
    rematch_fuzzy_matched = match_res.fuzzy_matched

    og_data = (
        no_match_reprocess
        .select([
            'submission_number',
            # 'internal_create_date', # don't need this since it will get brought in from the other data
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

    if len(rematch_fuzzy_matched)>0:
        # rejoin matches to roster dataframe
        rematch_fuzzy_matched = (
            rematch_fuzzy_matched
            .join(
                og_data,
                on='submission_number',
                how='left',
                suffix='_'
            )
        )

    if len(rematch_fuzzy_unmatched)>0:
        # rejoin matches to no match dataframe
        rematch_fuzzy_unmatched = (
            rematch_fuzzy_unmatched
            .join(
                og_data,
                on='submission_number',
                how='left',
                suffix='_'
            )
        )

    if len(rematch_no_demo)>0:
        rematch_no_demo = (
            rematch_no_demo
            .join(
                og_data,
                on='submission_number',
                how='left',
                suffix='_'
            )
        )
    
    if len(rematch_exact_matched)>0:
        # rejoin matches to roster dataframe
        rematch_exact_matched = (
            rematch_exact_matched
            .join(
                og_data,
                on='submission_number',
                how='left',
                suffix='_'
            )
        )

    return RematchResult(
        no_match_reprocess=no_match_reprocess,
        rematch_exact_matched=rematch_exact_matched,
        rematch_no_demo=rematch_no_demo, 
        rematch_fuzzy_unmatched=rematch_fuzzy_unmatched, 
        rematch_fuzzy_matched=rematch_fuzzy_matched
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
        )

        # temporary register dataframe into duckdb
        utils.polars_to_spark_temp_view(
            spark,
            no_match_df,
            'no_match_df'
        )

        print('Updating no_match_tbl where rows have a match found')
        # spark.sql("""
        #     UPDATE unmatched_resp AS t
        #     SET match_found = no_match_df.match_found
        #     FROM no_match_df
        #     WHERE t.submission_number = no_match_df.submission_number 
        # """
        # )
        spark.sql("""
            MERGE INTO tc_aim_prod.diqa_sandbox.unmatched_resp AS t
            USING no_match_df AS n
            ON t.submission_number = n.submission_number
            WHEN MATCHED THEN UPDATE SET t.match_found = n.match_found
        """
        )



    if len(rematch_res.rematch_fuzzy_matched) > 0:

        utils.polars_to_spark_temp_view(
            spark,
            rematch_res.rematch_fuzzy_matched,
            'rematch_matched_df'
        )
        
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
        utils.polars_to_spark_temp_view(
            spark,
            rematch_res.rematch_exact_matched,
            'rematch_exact_matched_df'
        )
        
        utils.safe_insert_sql_uc(
            spark=spark,
            catalog='tc_aim_prod',
            schema='diqa_sandbox',
            table='roster_resp',
            source_view='rematch_exact_matched_df',
            join_key='submission_number'
        )
