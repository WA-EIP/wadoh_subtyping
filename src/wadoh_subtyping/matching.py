import polars as pl
import polars.selectors as cs
from dataclasses import dataclass
from datetime import date

from wadoh_raccoon import dataframe_matcher as dfm

# df_to_process = pl.read_csv('tests/data/fuzzy/df_to_process.csv')
# no_match_df = pl.read_csv('tests/data/fuzzy/no_match_df.csv')
# df_pos_sars_cases = pl.read_csv('tests/data/fuzzy/reference_test_data.csv')

@dataclass
class MainResult:
    pull_res: object
    w_res: object
    fuzzy_matched_review_df: pl.DataFrame 
    fuzzy_without_demo_df: pl.DataFrame
    fuzzy_matched_none: pl.DataFrame
    fuzzy_matched_roster: pl.DataFrame
    fuzzy_matched_review_df_full: pl.DataFrame
    fuzzy_without_demo_df_full: pl.DataFrame
    fuzzy_matched_none_full: pl.DataFrame
    fuzzy_matched_roster_full: pl.DataFrame
    id_matched: pl.DataFrame
    id_matched_full: pl.DataFrame
    respnet_explode: pl.DataFrame
    combined_roster: pl.DataFrame
    # send_to_roster: pl.DataFrame

def match_phl(pull_res,w_res):
    """ Match PHL

    Usage
    -----
    To be called after the pulls and wrangle functions.

    Returns
    -------
    fuzzy_matched_review_df: pl.DataFrame
        a Polars dataframe of records that successfully fuzzy matched with 90% + ratio, but will still be reviewed
    fuzzy_without_demo_df: pl.DataFrame
        a Polars dataframe of records that are missing demographics
    fuzzy_matched_none: pl.DataFrame
        a Polars dataframe of records that did not fuzzy match successfully
    fuzzy_matched_roster: pl.DataFrame
        a Polars dataframe of records that had a 1-1 perfect match
    transformed_df: pl.DataFrame
        a Polars dataframe of the original transformed records
    submissions_to_fuzzy: pl.DataFrame
        the original pre-transformation records
    pull_res: class
        class that contains all the original table pulls
    w_res: class
        class that contains all the results from the wrangle_phl() function

    Examples
    --------

    ```python
    from src.subtype_link import processor

    main = processor.match_phl()
    ```
    
    And you can pull any object like

    ```python
    main.fuzzy_matched_review_df
    ```
    
    """
    
    if len(pull_res.phl_df) == 0:
        raise ValueError("Expected non-empty PHL DataFrame. Exit program. Bleep Blop")

    # since some labs have submitted records without a collection date, 
    # do this terrible thing and make a random collection date so that 
    # we can try to match. 
    # uncomment this code to run it when the gates of hell open again.
    # from datetime import date 
    # temp_remove_col_date = (
    #     w_res.submissions_to_fuzzy
    #     .with_columns(
    #         pl.when(
    #             (pl.col('SpecimenDateCollected').is_null())
    #         )
    #         .then(date.today())
    #         .otherwise(pl.col('SpecimenDateCollected'))
    #         .alias('SpecimenDateCollected')
    #     )
    # )
    
    fuzzy_init = dfm.DataFrameMatcher(
        df_subm=w_res.submissions_to_fuzzy, # use temp_remove_col_date when gates of hell open
        df_ref=pull_res.respnet,
        first_name=('PatientFirstName', 'FIRST_NAME'),
        last_name=('PatientLastName', 'LAST_NAME'),
        dob=('PatientBirthDate','BIRTH_DATE'),
        spec_col_date=('SpecimenDateCollected', 'SPECIMEN_COLLECTION_DTTM'),
        key='submission_number',
        threshold=80  # set what kind of fuzzy threshold you want, 100 being exact match
    )

    result=fuzzy_init.match()


    fuzzy_matched_none = result.fuzzy_unmatched
    fuzzy_matched_roster = result.exact_matched
    fuzzy_without_demo_df = result.no_demo
    fuzzy_matched_review_df = result.fuzzy_matched

    # run ID matching - join to 
    # comma separate, need to explode the values in respnet
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
            .when(
                (pl.col('CDC_N_COV_2019_HOSPITALIZED_RESPNET_MRN').is_not_null() & pl.col('RESPNET_HOSPITALIZED_RHINO_MRN').is_not_null())
            )
            .then(pl.col('CDC_N_COV_2019_HOSPITALIZED_RESPNET_MRN'))
            .otherwise(None)
            .alias('MRN')
        )
        .filter(pl.col('MRN').is_not_null())
    )

    # find records without demographics or that didn't match and try to match on MRN matching
    mrn_base_cols = [
                'submission_number',
                'internal_create_date',
                'submitted_dob',
                'submitted_collection_date',
                'first_name_clean',
                'last_name_clean'
                ]
    match_by_mrn = (
        pl.concat([
            fuzzy_matched_none.select(mrn_base_cols),
            fuzzy_without_demo_df.select(mrn_base_cols)
        ])
        .join(w_res.submissions_to_fuzzy, how='left',on='submission_number')
        .filter(
            (pl.col('SubmitterPatientNumber').is_not_null()) &
            (pl.col('SubmitterPatientNumber') != "")
        )
        .select(mrn_base_cols + ['SubmitterPatientNumber','PatientFirstName','PatientLastName','SpecimenDateCollected','SpecimenDateReceived'])
    )

    # if records did not match in fuzzy matching and they have an MRN number, then try to send through to MRN matching?
    # since the SubmitterPatientNumber is already basically gone after fuzzy matching, we need to join to get it back... frustrating
    # re_find_by_mrn = (
    #     fuzzy_without_demo_df
    #     .join(w_res.submissions_to_fuzzy, how='left',on='submission_number')
    #     .filter(
    #         (pl.col('SubmitterPatientNumber').is_not_null()) &
    #         (pl.col('SubmitterPatientNumber') != "")
    #     )
    #     .select(w_res.submissions_to_fuzzy.columns)
    # )

    # unpivot to get id list
    id_list = (
        match_by_mrn
        .unpivot(
            on=['PatientFirstName','PatientLastName','SubmitterPatientNumber'],
            index='submission_number',
            value_name='MRN'
        )
        # join back to get more columns
        .join(
            match_by_mrn,on='submission_number',how='left'
        )
        .filter(
            # if SubmitterPatientNumber is null, the MRN might be in the other Name columns..

            # (pl.col('SubmitterPatientNumber').is_not_null()) &
            # (pl.col('SubmitterPatientNumber') != "") &
            # just take MRNs that contain a number in them to match
            (pl.col('MRN').str.contains(r"\d"))
        )
    )

    # now match any number to a number in WDRS
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

    if len(fuzzy_matched_roster) > 0:
        # rejoin matches to roster dataframe
        fuzzy_matched_roster_full = (
            fuzzy_matched_roster.with_columns(pl.col('submission_number').cast(pl.String)) # need to cast as str since it freaks out with null col joins
            .join(
                og_data,
                on='submission_number',
                how='left'
            )
        )
    else:
        fuzzy_matched_roster_full = pl.DataFrame()

    if len(fuzzy_matched_roster_full)>0 and len(id_matched_full)>0:
        temp = pl.concat([fuzzy_matched_roster_full,id_matched_full],how='diagonal')
        # rejoin matches to roster dataframe
        combined_roster = temp.unique()
    elif len(fuzzy_matched_roster_full)>0 and len(id_matched_full)==0:
        # rejoin matches to roster dataframe
        combined_roster = fuzzy_matched_roster_full.unique()
    elif len(fuzzy_matched_roster_full)==0 and len(id_matched_full)>0:
        combined_roster = id_matched_full.unique()
    elif len(fuzzy_matched_roster_full)==0 and len(id_matched_full)==0:
        combined_roster = pl.DataFrame()
    
    if len(fuzzy_matched_none) > 0:
        # rejoin matches to no match dataframe
        fuzzy_matched_none_full = (
            fuzzy_matched_none.with_columns(pl.col('submission_number').cast(pl.String)) # need to cast as str since it freaks out with null col joins
            .join(
                og_data,
                on='submission_number',
                how='left'
            )
        )
    else:
        fuzzy_matched_none_full = pl.DataFrame()

    if len(fuzzy_matched_review_df) > 0:
        # rejoin matches to matched review dataframe
        fuzzy_matched_review_df_full = (
            fuzzy_matched_review_df.with_columns(pl.col('submission_number').cast(pl.String)) # need to cast as str since it freaks out with null col joins
            .join(
                og_data,
                on='submission_number',
                how='left'
            )
        )
    else:
        fuzzy_matched_review_df_full = pl.DataFrame()
    
    if len(fuzzy_without_demo_df) > 0:
        fuzzy_without_demo_df_full = (
            fuzzy_without_demo_df.with_columns(pl.col('submission_number').cast(pl.String)) # need to cast as str since it freaks out with null col joins
            .join(
                og_data,
                on='submission_number',
                how='left'
            )
        )
    else:
        fuzzy_without_demo_df_full = pl.DataFrame()

    return MainResult(
        pull_res=pull_res,
        w_res=w_res,
        fuzzy_matched_review_df=fuzzy_matched_review_df, 
        fuzzy_without_demo_df=fuzzy_without_demo_df, 
        fuzzy_matched_none=fuzzy_matched_none, 
        fuzzy_matched_roster=fuzzy_matched_roster,
        fuzzy_matched_review_df_full=fuzzy_matched_review_df_full, 
        fuzzy_without_demo_df_full=fuzzy_without_demo_df_full, 
        fuzzy_matched_none_full= fuzzy_matched_none_full, 
        fuzzy_matched_roster_full=fuzzy_matched_roster_full,
        id_matched=id_matched,
        id_matched_full=id_matched_full,
        respnet_explode=respnet_explode,
        combined_roster=combined_roster
        # send_to_roster=send_to_roster
    )