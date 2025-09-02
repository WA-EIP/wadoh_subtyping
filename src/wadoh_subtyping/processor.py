import polars as pl
import polars.selectors as cs
import duckdb as dd

from wadoh_raccoon.utils import pulls, helpers
from wadoh_subtyping import matching as tp, transform as tf, qa

from dataclasses import dataclass

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
    fuzzy_matched_review_df: pl.DataFrame 
    fuzzy_without_demo_df: pl.DataFrame
    fuzzy_matched_none: pl.DataFrame
    fuzzy_matched_roster: pl.DataFrame
    fuzzy_matched_review_df_full: pl.DataFrame
    fuzzy_without_demo_df_full: pl.DataFrame
    fuzzy_matched_none_full: pl.DataFrame
    fuzzy_matched_roster_full: pl.DataFrame

@dataclass
class RematchResult:
    no_match_reprocess: pl.DataFrame
    rematch_matched_review_df: pl.DataFrame
    rematch_without_demo_df: pl.DataFrame
    rematch_matched_none: pl.DataFrame
    rematch_matched_roster: pl.DataFrame

def run_pulls():
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
    from wadoh_subtyping import processor

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
        con=it.con,
        net_drive=it.net_drive,
        phl_df=pt.phl_df,
        received_submissions_df=pt.received_submissions_df,
        base_cols=pt.base_cols
    )

def wrangle_phl(pull_res):
    """ Wrangle PHL

    Usage
    -----
    To be called after the read_pulls() function.

    Parameters
    ----------
    pull_res: class
        a class containing all the table pulls. see processor.read_pulls()

    Returns
    -------
    transformed_df: pl.DataFrame
        a Polars dataframe containing transformed PHL data
    qa_issues: pl.DataFrame
        a Polars dataframe containing records with QA issues
    no_qa_issues: pl.DataFrame
        a Polars dataframe with records that are cleared to send to fuzzy matching
    submissions_to_fuzzy: pl.DataFrame
        records to be sent to fuzzy matching, like no_qa_issues but with only necessary columns

    Examples
    --------

    ```python
    from src.subtype_link import processor

    result = processor.run_pulls()
    ```
    Now call the wrangle_phl() function

    ```python
    wrangled_dfs = processor.wrangle_phl(pull_res=result)
    ```
    And you can get the dataframes like this:

    ```python
    wrangled_dfs.qa_issues

    wrangled_dfs.no_qa_issues
    ``` 

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
    old = pull_res.con.sql("SELECT * FROM rostered_tbl").pl()

    
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

def write_phl(pull_res,w_res,main_res):
    """ Write PHL

    Usage
    -----
    To be used after processor.match_phl() function.
    This function will write dataframes out to duckdb tables.

    Parameters
    ----------
    pull_res: class
        class that contains all the original table pulls
    w_res: class
        class that contains all the wrangled PHL tables
    main_res: class
        class that contains all the matched tables

    Examples
    --------

    The function will ingest all the processed data and output it into duckdb tables (or delta tables in the future)

    It will use the duckdb connection created in the init, and then insert/append data into the tables

    ```python
    if len(received_submissions_df) > 0:
        # append a copy of these records to received_submissions so we have a raw data snapshot
        # self.con.sql(f"INSERT INTO received_submissions SELECT * FROM received_submissions_df") 

        self.con.sql(
            '''
            INSERT INTO received_submissions AS rt
            SELECT r.*
            FROM received_submissions_df AS r
            LEFT OUTER JOIN received_submissions AS rt
            ON rt.submission_number = r.submission_number
            WHERE rt.submission_number IS NULL;
            '''
        )

    ```

    
    """
    if len(w_res.qa_issues) > 0:

        # append a copy of these records to received_submissions so we have a raw data snapshot
        pull_res.con.register("qa_issues", w_res.qa_issues)  
        print(f'{len(w_res.qa_issues)} QA issues found. Writing them to qa_tbl.')
        # pull_res.con.sql(
        #     """
        #     INSERT INTO qa_tbl AS rt
        #     SELECT r.*
        #     FROM qa_issues AS r
        #     LEFT OUTER JOIN qa_issues AS rt
        #     ON rt.submission_number = r.submission_number
        #     WHERE rt.submission_number IS NULL;
        #     """
        # )
        helpers.safe_insert(
            con=pull_res.con,
            table_name='qa_tbl',
            source_view='qa_issues',
            join_key='submission_number'
        )
        

    if len(pull_res.received_submissions_df) > 0:
        # append a copy of these records to received_submissions so we have a raw data snapshot
        
        pull_res.con.register("received_submissions_df", pull_res.received_submissions_df) 
        # pull_res.con.sql(
        #     """
        #     INSERT INTO received_submissions AS rt
        #     SELECT r.*
        #     FROM received_submissions_df AS r
        #     LEFT OUTER JOIN received_submissions AS rt
        #     ON rt.submission_number = r.submission_number
        #     WHERE rt.submission_number IS NULL;
        #     """
        # )
        helpers.safe_insert(
            con=pull_res.con,
            table_name='received_submissions',
            source_view='received_submissions_df',
            join_key='submission_number'
        )

    if len(main_res.fuzzy_matched_none_full) > 0:

        no_match_df = (
            main_res.fuzzy_matched_none_full
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
        # pull_res.con.sql(
        #     """
        #     INSERT INTO no_match_tbl AS rt
        #     SELECT r.*
        #     FROM no_match_df AS r
        #     LEFT OUTER JOIN no_match_tbl AS rt
        #     ON rt.submission_number = r.submission_number
        #     WHERE rt.submission_number IS NULL;
        #     """
        # )
        pull_res.con.register("no_match_df", no_match_df) 
        helpers.safe_insert(
            con=pull_res.con,
            table_name='no_match_tbl',
            source_view='no_match_df',
            join_key='submission_number'
        )

        pull_res.con.register("fuzzy_matched_none", main_res.fuzzy_matched_none_full) 
        print('Appending records into duckdb fuzzy_no_match_tbl')
        # pull_res.con.sql(
        #     """
        #     INSERT INTO fuzzy_matched_none_tbl AS rt
        #     SELECT r.*
        #     FROM fuzzy_matched_none AS r
        #     LEFT OUTER JOIN fuzzy_matched_none_tbl AS rt
        #     ON rt.submission_number = r.submission_number
        #     WHERE rt.submission_number IS NULL;
        #     """
        # )
        helpers.safe_insert(
            con=pull_res.con,
            table_name='fuzzy_matched_none_tbl',
            source_view='fuzzy_matched_none',
            join_key='submission_number'
        )

    if len(main_res.fuzzy_matched_review_df_full) > 0:
        
        pull_res.con.register("fuzzy_matched_review_df", main_res.fuzzy_matched_review_df_full) 
        print('Appending records into duckdb fuzzy_matched_review_tbl')
        # append a copy of these records to matched_review
        # pull_res.con.sql(
        #     """
        #     INSERT INTO fuzzy_matched_review_tbl AS rt
        #     SELECT r.*
        #     FROM fuzzy_matched_review_df AS r
        #     LEFT OUTER JOIN fuzzy_matched_review_tbl AS rt
        #     ON rt.submission_number = r.submission_number
        #     WHERE rt.submission_number IS NULL;
        #     """
        # )
        helpers.safe_insert(
            con=pull_res.con,
            table_name='fuzzy_matched_review_tbl',
            source_view='fuzzy_matched_review_df',
            join_key='submission_number'
        )

    if len(main_res.fuzzy_matched_roster_full) > 0:

        pull_res.con.register("roster_df", main_res.fuzzy_matched_roster_full) 
        print('Appending records into duckdb rostered_tbl')
        # append a copy of these records to no_match
        # pull_res.con.sql(
        #     """
        #     INSERT INTO rostered_tbl AS rt
        #     SELECT r.*
        #     FROM roster_df AS r
        #     LEFT OUTER JOIN rostered_tbl AS rt
        #     ON rt.submission_number = r.submission_number
        #     WHERE rt.submission_number IS NULL;
        #     """
        # )
        helpers.safe_insert(
            con=pull_res.con,
            table_name='rostered_tbl',
            source_view='roster_df',
            join_key='submission_number'
        )

    if len(main_res.fuzzy_without_demo_df_full) > 0:

        pull_res.con.register("fuzzy_without_demo_df", main_res.fuzzy_without_demo_df_full) 
        fuzzy_without_demo_df = main_res.fuzzy_without_demo_df
        print('Appending records into duckdb fuzzy_without_demo_tbl')
        # append a copy of these records to no_match
        # self.con.sql(f"INSERT INTO fuzzy_without_demo_tbl SELECT * FROM fuzzy_without_demo_df_base")
        # pull_res.con.sql(
        #     """
        #     INSERT INTO fuzzy_without_demo_tbl AS rt
        #     SELECT r.*
        #     FROM fuzzy_without_demo_df AS r
        #     LEFT OUTER JOIN fuzzy_without_demo_tbl AS rt
        #     ON rt.submission_number = r.submission_number
        #     WHERE rt.submission_number IS NULL;
        #     """
        # )
        helpers.safe_insert(
            con=pull_res.con,
            table_name='fuzzy_without_demo_tbl',
            source_view='fuzzy_without_demo_df',
            join_key='submission_number'
        )

def rematch(pull_res):
    no_match_reprocess = pull_res.con.sql(f'SELECT * FROM no_match_tbl WHERE match_found = FALSE').pl()

    submissions_to_fuzzy = (
        no_match_reprocess
        .unique(subset='submission_number')
    )

    # try to match the records to cases in WDRS
    instance = tp.fuzzy_process(
            df_pos_sars_cases_inp=pull_res.respnet,
            # df_to_process_inp=received_submissions_df,
            no_match_df_inp=no_match_reprocess,
            first_name_ref="FIRST_NAME",
            last_name_ref="LAST_NAME",
            dob_ref="BIRTH_DATE",
            spec_col_date_ref="SPECIMEN_COLLECTION_DTTM",
            last_name_src="last_name_clean",
            first_name_src="first_name_clean",
            dob_src="submitted_dob",
            spec_col_date_src="submitted_collection_date"
        )

    rematch_matched_review_df, rematch_without_demo_df, rematch_matched_none, rematch_matched_roster = instance.fuzzZ()


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

    if len(rematch_matched_roster)>0:
        # rejoin matches to roster dataframe
        rematch_matched_roster = (
            rematch_matched_roster
            .join(
                og_data,
                on='submission_number',
                how='left'
            )
        )

    if len(rematch_matched_none)>0:
        # rejoin matches to no match dataframe
        rematch_matched_none = (
            rematch_matched_none
            .join(
                og_data,
                on='submission_number',
                how='left'
            )
        )

    if len(rematch_matched_review_df)>0:
        # rejoin matches to matched review dataframe
        rematch_matched_review_df = (
            rematch_matched_review_df
            .join(
                og_data,
                on='submission_number',
                how='left'
            )
        )
    
    if len(rematch_without_demo_df)>0:
        rematch_without_demo_df = (
            rematch_without_demo_df
            .join(
                og_data,
                on='submission_number',
                how='left'
            )
        )

    return RematchResult(
        no_match_reprocess=no_match_reprocess,
        rematch_matched_review_df=rematch_matched_review_df, 
        rematch_without_demo_df=rematch_without_demo_df, 
        rematch_matched_none=rematch_matched_none, 
        rematch_matched_roster=rematch_matched_roster
    )

def write_rematch(rematch_res,pull_res):
    # Update the no_match_tbl in duckdb where matches were found
    # I'm too sketched to remove records, so just relabel them as match_found True or False
    if len(rematch_res.rematch_matched_review_df) > 0 or len(rematch_res.rematch_matched_roster) > 0:

        # get a list of previous no_match that had found a match this time around
        match_found = list(rematch_res.rematch_matched_review_df['submission_number']) + list(rematch_res.rematch_matched_roster['submission_number'])
        
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
        pull_res.con.register('no_match_df', no_match_df)

        print('Updating no_match_tbl where rows have a match found')
        pull_res.con.sql("""
            UPDATE no_match_tbl AS t
            SET match_found = no_match_df.match_found
            FROM no_match_df
            WHERE t.submission_number = no_match_df.submission_number 
        """
        )

        # temporary register dataframe into duckdb
        # self.con.unregister('no_match_df', no_match_df)

    
    if len(rematch_res.rematch_matched_review_df) > 0:
        
        pull_res.con.register("rematch_matched_review_df", rematch_res.rematch_matched_review_df) 
        print('Appending rematch review to review table')
        # self.con.sql(f"INSERT INTO fuzzy_matched_review_tbl SELECT * FROM rematch_matched_review_df")
        # pull_res.con.sql(
        #     """
        #     INSERT INTO fuzzy_matched_review_tbl AS rt
        #     SELECT r.*
        #     FROM rematch_matched_review_df AS r
        #     LEFT OUTER JOIN fuzzy_matched_review_tbl AS rt
        #     ON rt.submission_number = r.submission_number
        #     WHERE rt.submission_number IS NULL;
        #     """
        # )
        helpers.safe_insert(
            con=pull_res.con,
            table_name='fuzzy_matched_review_tbl',
            source_view='rematch_matched_review_df',
            join_key='submission_number'
        )

    if len(rematch_res.rematch_matched_roster) > 0:
        
        pull_res.con.register("rematch_matched_roster", rematch_res.rematch_matched_roster) 
        print('Appending rematch match found to fuzzy_matched_roster_tbl')
        # self.con.sql(f"INSERT INTO fuzzy_matched_roster_tbl SELECT * FROM rematch_matched_roster_df")
        # pull_res.con.sql(
        #     """
        #     INSERT INTO fuzzy_matched_roster_tbl AS rt
        #     SELECT r.*
        #     FROM rematch_matched_roster AS r
        #     LEFT OUTER JOIN fuzzy_matched_roster_tbl AS rt
        #     ON rt.submission_number = r.submission_number
        #     WHERE rt.submission_number IS NULL;
        #     """
        # )
        helpers.safe_insert(
            con=pull_res.con,
            table_name='fuzzy_matched_roster_tbl',
            source_view='rematch_matched_roster',
            join_key='submission_number'
        )
