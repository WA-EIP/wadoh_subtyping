import polars as pl
from wadoh_subtyping import helpers
from dataclasses import dataclass
from datetime import date
from wadoh_raccoon import dataframe_matcher as dfm

@dataclass
class RematchResult:
    no_match_reprocess: pl.DataFrame
    rematch_matched_review_df: pl.DataFrame
    rematch_without_demo_df: pl.DataFrame
    rematch_matched_none: pl.DataFrame
    rematch_matched_roster: pl.DataFrame
    rematch_combined_roster: pl.DataFrame


def rematch(pull_res):
    no_match_reprocess = pull_res.con.sql('SELECT * FROM no_match_tbl WHERE match_found = FALSE').pl()

    submissions_to_fuzzy = (
        no_match_reprocess
        .unique(subset='submission_number')
        .with_columns(
            temp_collection_date=pl.col('submitted_collection_date'),
            temp_birth_date=pl.col('submitted_dob')
        )
        .drop(['submitted_collection_date','submitted_dob','SPECIMEN_COLLECTION_DTTM'])
    )

    # # try to match the records to cases in WDRS
    fuzzy_init = dfm.DataFrameMatcher(
        df_subm=submissions_to_fuzzy,
        df_ref=pull_res.respnet,
        first_name=('first_name_clean', 'FIRST_NAME'),
        last_name=('last_name_clean', 'LAST_NAME'),
        dob=('temp_birth_date','BIRTH_DATE'),
        spec_col_date=('temp_collection_date', 'SPECIMEN_COLLECTION_DTTM'),
        key='submission_number',
        threshold=80  # set what kind of fuzzy threshold you want, 100 being exact match
    )

    result=fuzzy_init.match()

    rematch_matched_none = result.fuzzy_unmatched
    rematch_matched_roster = result.exact_matched
    rematch_without_demo_df = result.no_demo
    rematch_matched_review_df = result.fuzzy_matched

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

    # if records did not match in fuzzy matching and they have an MRN number, then try to send through to MRN matching?
    # since the SubmitterPatientNumber is already basically gone after fuzzy matching, we need to join to get it back... frustrating
    # the first and last name column in the rematch dataset are going to be missing
    # because first/last_name_clean will remove digits (mrn's...........)
    # so need to join to original phl_df first to get the mrn and untouched name cols
    # this would all be 1000% easier if we had legitimate data standards but we dont so it's not.
    # match_by_mrn2 = (
    #     rematch_without_demo_df
    #     .join(
    #         submissions_to_fuzzy.select(pl.col('submission_number'),cs.starts_with(("WDRS_", "SPECIMEN","PERFORMING","SUBMITTER","TEST_")),pl.col('match_found'))
    #         , how='left',on='submission_number'
    #     )
    #     .join(
    #         pull_res.phl_df.select(['PHLAccessionNumber','SubmitterPatientNumber']),
    #         left_on='submission_number',right_on='PHLAccessionNumber',how='left'
    #     )
    #     .filter(
    #         (pl.col('SubmitterPatientNumber').is_not_null()) |
    #         (pl.col('SubmitterPatientNumber') != "")
    #     )
    #     .select(submissions_to_fuzzy.columns + ['SubmitterPatientNumber'])
    # )

    # find records without demographics or that didn't match and try to match on MRN matching
    mrn_base_cols = [
                'submission_number',
                'internal_create_date',
                'submitted_dob',
                'submitted_collection_date',
                'first_name_clean',
                'last_name_clean'
                ]
    match_by_mrn2 = (
        pl.concat([
            rematch_matched_none.select(mrn_base_cols),
            rematch_without_demo_df.select(mrn_base_cols)
        ])
        .join(submissions_to_fuzzy, how='left',on='submission_number')
        .join(
            pull_res.phl_df.select(['PHLAccessionNumber','SubmitterPatientNumber','PatientFirstName','PatientLastName','SpecimenDateCollected','SpecimenDateReceived']),
            left_on='submission_number',right_on='PHLAccessionNumber',how='left'
        )
        .filter(
            (pl.col('SubmitterPatientNumber').is_not_null()) &
            (pl.col('SubmitterPatientNumber') != "")
        )
        .select(mrn_base_cols + ['SubmitterPatientNumber','PatientFirstName','PatientLastName','SpecimenDateCollected','SpecimenDateReceived'])
    )

    # unpivot to get id list
    id_list = (
        match_by_mrn2
        .unpivot(
            on=['PatientFirstName','PatientLastName','SubmitterPatientNumber'],
            index='submission_number',
            value_name='MRN'
        )
        # join back to get more columns
        .join(
            match_by_mrn2,on='submission_number',how='left'
        )
        .filter(
            # if SubmitterPatientNumber is null, the MRN might be in the other Name columns..

            # (pl.col('SubmitterPatientNumber').is_not_null()) &
            # (pl.col('SubmitterPatientNumber') != "") &
            # just take MRNs that contain a number in them to match
            (pl.col('MRN').str.contains(r"\d"))
        )
        .unique()
        .filter((pl.col('MRN').is_not_null()) | (pl.col('MRN') != ""))
    )

    respnet_explode = (
        pull_res.respnet
        .with_columns(
            pl.col("CDC_N_COV_2019_HOSPITALIZED_RESPNET_MRN").str.split(", "),
            # pl.col("RESPNET_HOSPITALIZED_RHINO_MRN").str.split(","),
        )
        .explode(["CDC_N_COV_2019_HOSPITALIZED_RESPNET_MRN"])
        .with_columns(
            pl.col("RESPNET_HOSPITALIZED_RHINO_MRN").str.split(", "),
            # pl.col("RESPNET_HOSPITALIZED_RHINO_MRN").str.split(","),
        )
        .explode(["RESPNET_HOSPITALIZED_RHINO_MRN"])
        .with_columns(
            pl.when(
                (pl.col('CDC_N_COV_2019_HOSPITALIZED_RESPNET_MRN').is_null() & pl.col('RESPNET_HOSPITALIZED_RHINO_MRN').is_null())
            )
            .then(None)
            .when(
                (pl.col('CDC_N_COV_2019_HOSPITALIZED_RESPNET_MRN').is_not_null() & pl.col('RESPNET_HOSPITALIZED_RHINO_MRN').is_null())
            )
            .then(pl.col('CDC_N_COV_2019_HOSPITALIZED_RESPNET_MRN'))
            .when(
                (pl.col('CDC_N_COV_2019_HOSPITALIZED_RESPNET_MRN').is_null() & pl.col('RESPNET_HOSPITALIZED_RHINO_MRN').is_not_null())
            )
            .then(pl.col('RESPNET_HOSPITALIZED_RHINO_MRN'))
            # need to code for when both MRNs are not null, which one to use? both?
            # .when(
            #     (pl.col('CDC_N_COV_2019_HOSPITALIZED_RESPNET_MRN').is_not_null() & pl.col('RESPNET_HOSPITALIZED_RHINO_MRN').is_not_null())
            # )
            # .then()
            .otherwise(None)
            .alias('MRN')
        )
        .filter((pl.col('MRN').is_not_null()) | (pl.col('MRN') != ""))
    )

    id_matched = (
        id_list
        .join(
            respnet_explode.select([
                'CASE_ID',
                'MRN'
                # cs.contains(('_DATE','_DTTM'))
            ]),
            on='MRN',
            how='inner'
        )
        .unique()
        .filter(pl.col('CASE_ID').is_not_null())
    )

    print(f"\n {id_matched.height} records matched on MRN")

    if len(id_matched) > 0:
        # rejoin matches to roster dataframe
        id_matched_full = (
            id_matched.with_columns(pl.col('submission_number').cast(pl.String)) # need to cast as str since it freaks out with null col joins
                .join(
                    og_data,
                    on='submission_number',
                    how='left'
                )
                # need to manually add the internal_create_date
                .with_columns(
                    internal_create_date=date.today()
                )
                .select([
                    'submission_number',
                    'internal_create_date', # don't need this since it will get brought in from the other data
                    'CASE_ID',
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
            .unique()
        )
    else:
        id_matched_full = pl.DataFrame()
    
    if len(rematch_matched_roster)>0 and len(id_matched_full)>0:
        temp = pl.concat([rematch_matched_roster,id_matched_full],how='diagonal')
        # rejoin matches to roster dataframe
        rematch_combined_roster = temp.unique()
    elif len(rematch_matched_roster)>0 and len(id_matched_full)==0:
        # rejoin matches to roster dataframe
        rematch_combined_roster = rematch_matched_roster.unique()
    elif len(rematch_matched_roster)==0 and len(id_matched_full)>0:
        rematch_combined_roster = id_matched_full.unique()
    elif len(rematch_matched_roster)==0 and len(id_matched_full)==0:
        rematch_combined_roster = pl.DataFrame()

    # breakpoint()

    if len(rematch_matched_none)>0:
        # rejoin matches to no match dataframe
        rematch_matched_none = (
            rematch_matched_none
            .join(
                og_data,
                on='submission_number',
                how='left',
                suffix="_og"
            )
        )

    if len(rematch_matched_review_df)>0:
        # rejoin matches to matched review dataframe
        rematch_matched_review_df = (
            rematch_matched_review_df
            .join(
                og_data,
                on='submission_number',
                how='left',
                suffix="_og"
            )
        )
    
    if len(rematch_without_demo_df)>0:
        rematch_without_demo_df = (
            rematch_without_demo_df
            .join(
                og_data,
                on='submission_number',
                how='left',
                suffix="_og"
            )
        )

    return RematchResult(
        no_match_reprocess=no_match_reprocess,
        rematch_matched_review_df=rematch_matched_review_df, 
        rematch_without_demo_df=rematch_without_demo_df, 
        rematch_matched_none=rematch_matched_none, 
        rematch_matched_roster=rematch_matched_roster,
        rematch_combined_roster=rematch_combined_roster
    )

def write_rematch(rematch_res,pull_res):
    # Update the no_match_tbl in duckdb where matches were found
    # I'm too sketched to remove records, so just relabel them as match_found True or False
    if len(rematch_res.rematch_matched_review_df) > 0 or len(rematch_res.rematch_matched_roster) > 0:

        # get a list of previous no_match that had found a match this time around
        match_found = list(rematch_res.rematch_matched_review_df['submission_number']) + list(rematch_res.rematch_combined_roster['submission_number'])
        
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

        pull_res.con.register(
            "no_match_df", # register the name of the df into duckdb's register
            helpers.align_to_duckdb( # there may be many many columns that we don't need to add to duckdb. just take the cols necessary
                no_match_df,
                pull_res.con.sql('SELECT * FROM no_match_tbl LIMIT 0').pl().columns
            )
        )

        # temporary register dataframe into duckdb
        # pull_res.con.register('no_match_df', no_match_df)

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

        pull_res.con.register(
            "rematch_matched_review_df", # register the name of the df into duckdb's register
            helpers.align_to_duckdb( # there may be many many columns that we don't need to add to duckdb. just take the cols necessary
                rematch_res.rematch_matched_review_df,
                pull_res.con.sql('SELECT * FROM fuzzy_matched_review_tbl LIMIT 0').pl().columns
            )
        )
        
        # pull_res.con.register("rematch_matched_review_df", rematch_res.rematch_matched_review_df) 
        print('Appending rematch review to review table')
        # self.con.sql(f"INSERT INTO fuzzy_matched_review_tbl SELECT * FROM rematch_matched_review_df")

        helpers.safe_insert(
            con=pull_res.con,
            table_name='fuzzy_matched_review_tbl',
            source_view='rematch_matched_review_df',
            join_key='submission_number'
        )

    if len(rematch_res.rematch_matched_roster) > 0:

        pull_res.con.register(
            "rematch_matched_roster", # register the name of the df into duckdb's register
            helpers.align_to_duckdb( # there may be many many columns that we don't need to add to duckdb. just take the cols necessary
                rematch_res.rematch_matched_roster,
                pull_res.con.sql('SELECT * FROM fuzzy_matched_roster_tbl LIMIT 0').pl().columns
            )
        )
        
        # pull_res.con.register("rematch_matched_roster", rematch_res.rematch_matched_roster) 
        print('Appending rematch match found to fuzzy_matched_roster_tbl')
        # self.con.sql(f"INSERT INTO fuzzy_matched_roster_tbl SELECT * FROM rematch_matched_roster_df")
        helpers.safe_insert(
            con=pull_res.con,
            table_name='fuzzy_matched_roster_tbl',
            source_view='rematch_matched_roster',
            join_key='submission_number'
        )