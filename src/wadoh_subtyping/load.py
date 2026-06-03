import polars as pl
from wadoh_subtyping import helpers
from wadoh_subtyping import rematch_funcs
from datetime import date
import os 

def write_phl(pull_res,w_res,main_res, manual_reviewed):
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
        pull_res.con.register(
            "qa_issues", # register the name of the df into duckdb's register
            helpers.align_to_duckdb( # there may be many many columns that we don't need to add to duckdb. just take the cols necessary
                w_res.qa_issues,
                pull_res.con.sql('SELECT * FROM qa_tbl LIMIT 0').pl().columns
            )
        )  
        print(f'{len(w_res.qa_issues)} QA issues found. Writing them to qa_tbl.')
       
        helpers.safe_insert(
            con=pull_res.con,
            table_name='qa_tbl',
            source_view='qa_issues',
            join_key='submission_number'
        )
        

    if len(pull_res.received_submissions_df) > 0:
        # append a copy of these records to received_submissions so we have a raw data snapshot
        pull_res.con.register(
            "received_submissions_df", # register the name of the df into duckdb's register
            helpers.align_to_duckdb( # there may be many many columns that we don't need to add to duckdb. just take the cols necessary
                pull_res.received_submissions_df,
                pull_res.con.sql('SELECT * FROM received_submissions LIMIT 0').pl().columns
            )
        )          
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

        pull_res.con.register(
            "no_match_df", # register the name of the df into duckdb's register
            helpers.align_to_duckdb( # there may be many many columns that we don't need to add to duckdb. just take the cols necessary
                no_match_df,
                pull_res.con.sql('SELECT * FROM no_match_tbl LIMIT 0').pl().columns
            )
        )  
        
        helpers.safe_insert(
            con=pull_res.con,
            table_name='no_match_tbl',
            source_view='no_match_df',
            join_key='submission_number'
        )

        pull_res.con.register(
            "fuzzy_matched_none", # register the name of the df into duckdb's register
            helpers.align_to_duckdb( # there may be many many columns that we don't need to add to duckdb. just take the cols necessary
                main_res.fuzzy_matched_none_full,
                pull_res.con.sql('SELECT * FROM fuzzy_matched_none_tbl LIMIT 0').pl().columns
            )
        )  

        # pull_res.con.register("fuzzy_matched_none", main_res.fuzzy_matched_none_full) 
        print('Appending records into duckdb fuzzy_no_match_tbl')
        
        helpers.safe_insert(
            con=pull_res.con,
            table_name='fuzzy_matched_none_tbl',
            source_view='fuzzy_matched_none',
            join_key='submission_number'
        )

    if len(main_res.fuzzy_matched_review_df_full) > 0:

        pull_res.con.register(
            "fuzzy_matched_review_df", # register the name of the df into duckdb's register
            helpers.align_to_duckdb( # there may be many many columns that we don't need to add to duckdb. just take the cols necessary
                main_res.fuzzy_matched_review_df_full,
                pull_res.con.sql('SELECT * FROM fuzzy_matched_review_tbl LIMIT 0').pl().columns
            )
        )  
        
        # pull_res.con.register("fuzzy_matched_review_df", main_res.fuzzy_matched_review_df_full) 
        print('Appending records into duckdb fuzzy_matched_review_tbl')
        # append a copy of these records to matched_review

        helpers.safe_insert(
            con=pull_res.con,
            table_name='fuzzy_matched_review_tbl',
            source_view='fuzzy_matched_review_df',
            join_key='submission_number'
        )
    if len(main_res.id_matched_full) > 0:
        # append a copy of these records to received_submissions so we have a raw data snapshot
        pull_res.con.register(
            "id_matched_df", # register the name of the df into duckdb's register
            helpers.align_to_duckdb( # there may be many many columns that we don't need to add to duckdb. just take the cols necessary
                main_res.id_matched_full,
                pull_res.con.sql('SELECT * FROM id_matched LIMIT 0').pl().columns
            )
        )  

        # pull_res.con.register("id_matched_df", main_res.id_matched_full) 
        
        helpers.safe_insert(
            con=pull_res.con,
            table_name='id_matched',
            source_view='id_matched_df',
            join_key='submission_number'
        )

    if len(main_res.fuzzy_matched_roster_full) > 0:

        # pull_res.con.register("roster_df", main_res.fuzzy_matched_roster_full)

        add_to_roster = pl.concat([main_res.fuzzy_matched_roster_full,main_res.id_matched_full],how='diagonal') 
        pull_res.con.register(
            "roster_df", # register the name of the df into duckdb's register
            helpers.align_to_duckdb( # there may be many many columns that we don't need to add to duckdb. just take the cols necessary
                add_to_roster,
                pull_res.con.sql('SELECT * FROM rostered_tbl LIMIT 0').pl().columns
            )
        )  
        # pull_res.con.register("roster_df",add_to_roster)

        print('Appending records into duckdb rostered_tbl')
        # append a copy of these records to no_match

        helpers.safe_insert(
            con=pull_res.con,
            table_name='rostered_tbl',
            source_view='roster_df',
            join_key='submission_number'
        )
    
    if len(manual_reviewed) > 0:

        pull_res.con.register(
            "reviewed_df", # register the name of the df into duckdb's register
            helpers.align_to_duckdb( # there may be many many columns that we don't need to add to duckdb. just take the cols necessary
                manual_reviewed,
                pull_res.con.sql('SELECT * FROM rostered_tbl LIMIT 0').pl().columns
            )
        ) 

        # pull_res.con.register("reviewed_df", manual_reviewed) 
        print('Appending records from completed manual review into duckdb rostered_tbl')

        helpers.safe_insert(
            con=pull_res.con,
            table_name='rostered_tbl',
            source_view='reviewed_df',
            join_key='submission_number'
        )

    if len(main_res.fuzzy_without_demo_df_full) > 0:

        pull_res.con.register(
            "fuzzy_without_demo_df", # register the name of the df into duckdb's register
            helpers.align_to_duckdb( # there may be many many columns that we don't need to add to duckdb. just take the cols necessary
                main_res.fuzzy_without_demo_df_full,
                pull_res.con.sql('SELECT * FROM fuzzy_without_demo_tbl LIMIT 0').pl().columns
            )
        )

        # pull_res.con.register("fuzzy_without_demo_df", main_res.fuzzy_without_demo_df_full) 
        fuzzy_without_demo_df = main_res.fuzzy_without_demo_df
        print('Appending records into duckdb fuzzy_without_demo_tbl')
        # append a copy of these records to no_match
        # self.con.sql(f"INSERT INTO fuzzy_without_demo_tbl SELECT * FROM fuzzy_without_demo_df_base")
        
        helpers.safe_insert(
            con=pull_res.con,
            table_name='fuzzy_without_demo_tbl',
            source_view='fuzzy_without_demo_df',
            join_key='submission_number'
        )

def combine_and_write(rematch,res, final_roster, manual_reviewed, manual, folder):
    try: 
        review_cols = [
                    'submission_number',
                    'internal_create_date',
                    'CASE_ID',
                    'first_name_clean',
                    'last_name_clean',
                    'first_name_clean_right',
                    'last_name_clean_right',
                    'submitted_dob',
                    'submitted_collection_date',
                    'reference_collection_date',
                    'business_day_count',
                    'match_ratio',
                    'reverse_match_ratio',
                    # 'matched',
                    # 'reverse_matched'
                ]
        # combine the review dfs if they are both filled
        if len(rematch.rematch_matched_review_df) > 0 and len(res.fuzzy_matched_review_df) > 0:
            review = (
                pl.concat([rematch.rematch_matched_review_df,res.fuzzy_matched_review_df_full])
                .unique(subset='submission_number')
                .select(review_cols)
            )
        elif len(rematch.rematch_matched_review_df) > 0 and len(res.fuzzy_matched_review_df) == 0:
            review = (
                rematch.rematch_matched_review_df
                .unique(subset='submission_number')
                .select(review_cols)
            )
        elif len(rematch.rematch_matched_review_df) == 0 and len(res.fuzzy_matched_review_df) > 0:
            review = (
                res.fuzzy_matched_review_df_full
                .unique(subset='submission_number')
                .select(review_cols)
            )
        elif len(rematch.rematch_matched_review_df) == 0 and len(res.fuzzy_matched_review_df) == 0:
            print('\nNothing to add to review table.')
            review = pl.DataFrame()
    except ValueError:
        print('review dataframe creation didnt work') 

    if len(review) > 0:
        # Get today's date as a string in YYYY-MM-DD format
        dir_name = f'{res.pull_res.net_drive}/qa/{date.today().strftime("%Y-%m-%d")}'

        # Check if the directory exists, and if not, create it
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
            print("Directory created.")
        else:
            print("Directory already exists.")
        
        print("\nWriting review data to QA folder")
        review_to_write = (
            review
            .with_columns(
                good_match=False,
                resolved=False,
                notes=None
            )
        )

        # write the review files to the y drive for manual review
        review_to_write.write_csv(f"{dir_name}/fuzzy_matched_review_{date.today()}.csv")

        res.pull_res.con.register(
            "review_to_write_df", # register the name of the df into duckdb's register
            helpers.align_to_duckdb( # there may be many many columns that we don't need to add to duckdb. just take the cols necessary
                review_to_write,
                res.pull_res.con.sql('SELECT * FROM manual_review_resp LIMIT 0').pl().columns
            )
        )

        # save a copy in the duckdb database to keep track
        # res.pull_res.con.register("review_to_write_df",review_to_write)
        
        helpers.safe_insert(
            con=res.pull_res.con,
            table_name='manual_review_resp',
            source_view='review_to_write_df',
            join_key='submission_number'
        )

    if len(manual_reviewed) > 0:
        print("\nWriting completed manual review to archive")
        # put the manual reviewed csv in an archive
        manual.write_csv(f"{folder}/archive/{date.today()}_reviewed.csv")

        # remove the previous from the completed folder
        csv_files = list(folder.glob("*.csv"))

        print(f"\nRemoving {len(csv_files)} CSV file(s) from completed_review folder")

        for f in csv_files:
            f.unlink()


    # write the review files also to a table to keep track of them
    if len(final_roster) > 0:

        final_roster_string_dates = (
            final_roster
            .with_columns(
                DOB=pl.col('DOB').dt.strftime("%m/%d/%Y"),
                SPECIMEN_COLLECTION_DTTM=pl.col('SPECIMEN_COLLECTION_DTTM').dt.strftime("%m/%d/%Y"),
                SPECIMEN_RECEIVED_DTTM=pl.col('SPECIMEN_RECEIVED_DTTM').dt.strftime("%m/%d/%Y")
            )
        )
        print('\nWriting roster to network drive') 
        final_roster_string_dates.write_csv(f"{res.pull_res.net_drive}/subtype_roster_{date.today()}.csv")

    if len(manual_reviewed) > 1:
        # need to save the manual_reviewed output to the rostered_table
        rost = res.pull_res.con.sql("SELECT * FROM rostered_tbl LIMIT 1").pl()
        (
            manual_reviewed
            .select(rost.columns)
            .sort(rost.columns)
        )

    print('\nWriting outputs to internal tables')
    write_phl(pull_res=res.pull_res, w_res=res.w_res, main_res=res, manual_reviewed=manual_reviewed)

    print('\nWriting rematch outputs to tables')
    rematch_funcs.write_rematch(rematch_res=rematch,pull_res=res.pull_res)

    # now write a cumulative file..
    cumulative_roster = res.pull_res.con.sql('SELECT * FROM rostered_tbl').pl()

    cumulative_roster_dedup = (
        cumulative_roster
        .unique(subset='submission_number')
        .select(['submission_number',
            'internal_create_date',
            'first_name_clean',
            'last_name_clean',
            'submitted_dob',
            'submitted_collection_date',
            'reference_collection_date',
            'CASE_ID'
        ])
    )

    cumulative_roster_dedup.write_csv(f"{res.pull_res.net_drive}/cumulative_subtype_roster.csv")