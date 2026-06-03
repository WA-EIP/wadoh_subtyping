import polars as pl
import polars.selectors as cs
from wadoh_subtyping import transform as tf, qa
from dataclasses import dataclass

@dataclass
class WrangleResult:
    transformed_df: pl.DataFrame
    qa_issues: pl.DataFrame 
    no_qa_issues: pl.DataFrame 
    submissions_to_fuzzy: pl.DataFrame
    past_rostered: pl.DataFrame

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
            # i cant check on specimen collection date anymore cus some labs dont send one :)
            # so, we're assuming that submission_number can only be used once per lab test
            # left_on=['submission_number', 'SPECIMEN_COLLECTION_DTTM'],
            # right_on=['submission_number', 'submitted_collection_date'],
            on='submission_number',
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
            pl.when(
                ((~pl.col("PatientFirstName").str.contains(r"\d")) | (~pl.col("PatientLastName").str.contains(r"\d"))) &
                ((~pl.col("PatientFirstName").is_not_null()) & (~pl.col("PatientLastName").is_not_null())) &
                # this makes WA1087891 slip through, it doesn't have a birth date but does have a number. it should be sent
                # through to fuzzy matching
                # i had it this way because missing birthdates shouldnt be sent to matching. figure out a way to flag these ones for review
                ((pl.col('PatientBirthDate').is_null()))
            )
            .then(True)
            .otherwise(False)
            .alias('qa_missing_dob')
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
        # add records that have demographics but missing date of birth
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

    # remove this, look at all labs
    # main_labs = "Kadlec Regional Medical Center|Labcorp Portland|Laboratories NW / Multicare|Laboratories Northwest|Providence Reg Med Ctr - Everett|Providence Everett Medical Center|Multicare Deaconess Hospital"
    
    # if there are failed fuzzy matching, run checks to see if there are numbers in the last name/first name cols match to WDRS MRN,
    # and then check if SubmitterPatientNumber matches WDRS MRN, regardless of lab

    # save outputs of what matched via MRN matching to keep account of what happened when. 

    # also keep track of when there is no specimen collection date 

    # add the MRN matched to the roster/rest of the pipeline

    submissions = (
        no_qa_issues
        .unique(subset=['submission_number'])
        .with_columns(
            pl.when(
                # (pl.col('SubmitterName').str.contains(main_labs)) &
                # check if names columns have numbers in them.. if they do, send to ID matching
                ((pl.col("PatientFirstName").str.contains(r"\d")) | (pl.col("PatientLastName").str.contains(r"\d")))
                # this makes WA1087891 slip through, it doesn't have a birth date but does have a number. it should be sent
                # through to fuzzy matching
                # i had it this way because missing birthdates shouldnt be sent to matching. figure out a way to flag these ones for review
                # ((pl.col('PatientBirthDate').is_null()) & (pl.col("SubmitterPatientNumber").is_not_null())) 
            )
            .then(True)
            .otherwise(False)
            .alias('send_to_id_matching')
        )
    )
    # check for WA1087891 it should go through fuzzy matching, not id matching

    # only take the names to match. will join back to transformed_df to get the full data
    submissions_to_fuzzy = (
        submissions 
        # filter to get only the ones that should be fuzzy matched
        # .filter(~pl.col('send_to_id_matching'))
        .select([
            'submission_number',
            'internal_create_date',
            'PatientFirstName',
            'PatientLastName',
            'PatientBirthDate',
            'SpecimenDateCollected',
            'SpecimenDateReceived',
            'SubmitterPatientNumber'
        ])
    )

    return WrangleResult(
        transformed_df=transformed_df_filtered,
        qa_issues=qa_issues, 
        no_qa_issues=no_qa_issues, 
        submissions_to_fuzzy=submissions_to_fuzzy,
        past_rostered=old
    )

def combine_the_rosters(rematch,res, reviewed_roster):
    # If there are any records to roster or in rematch roster, combine them
    if len(rematch.rematch_combined_roster)>0 and len(res.combined_roster) > 0:

        combined = (pl.concat([res.combined_roster,rematch.rematch_combined_roster],how='diagonal'))

        print('rematch records found, append to roster')
        create_roster = (
            tf.create_roster(
                matched_and_transformed_df=combined,
                respnet=res.pull_res.respnet
            )
        )

        roster = (tf.dedup_roster(roster_inp=create_roster,reference_inp=res.pull_res.respnet))

    elif len(rematch.rematch_combined_roster)==0 and len(res.combined_roster) > 0:
        create_roster = (
            tf.create_roster(
                matched_and_transformed_df=res.combined_roster,
                respnet=res.pull_res.respnet
            )
        )

        roster = (tf.dedup_roster(roster_inp=create_roster,reference_inp=res.pull_res.respnet))

    elif len(rematch.rematch_combined_roster)==0 and len(res.combined_roster)==0:
        print('Exit: both rematch and normal roster are empty')

        roster = pl.DataFrame()

    # If there are any records in manually reviewed_roster, combine it with the main roster
    if len(roster) > 0 and len(reviewed_roster)>0:
        print("Add reviewed records to roster")
        final_roster = pl.concat([roster,reviewed_roster])

    elif len(roster)==0 and len(reviewed_roster)>0:
        print("Completed manual reviews record found, but no regular roster")
        final_roster = reviewed_roster

    elif len(roster) > 0 and len(reviewed_roster)==0:
        print("No review records to append to roster")
        final_roster = roster

    else:
        final_roster = pl.DataFrame()

    return final_roster