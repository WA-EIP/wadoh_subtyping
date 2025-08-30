import polars as pl
from datetime import date

def qa_wdrs_test_performed(wdrs_test_perf_output: str) -> pl.Expr:
    """ QA wdrs_test_performed

    Usage
    -----
    To be used in a .with_columns() statement
    
    Examples
    --------

    ```python
    from wadoh_subtyping import qa
    df = (
        df
        .with_columns(
            qa.qa_wdrs_test_performed(wdrs_test_perf_output=wdrs_test_perf_output)
        )
    )
    ``` 
    """
    
    return (
        pl.when(pl.col(wdrs_test_perf_output).is_null())
        .then(True)
        .otherwise(False)
        .alias('qa_wdrs_test_performed')
    
    )

def qa_test_performed_desc(test_perf_output: str):
    """ QA test_performed_desc

    Usage
    -----
    To be used in a .with_columns() statement
    
    Examples
    --------
    ```python
    from wadoh_subtyping import qa
    from wadoh_raccoon.utils import helpers

    df = (
        df
        .with_columns(
            qa.qa_test_performed_desc(test_perf_output=test_perf_output)
        )
    )
    ``` 
    """
    
    return (
        pl.when(pl.col(test_perf_output).is_null())
        .then(True)
        .otherwise(False)
        .alias('qa_test_performed_desc')
    )

def qa_wdrs_result(wdrs_res_output: str) -> pl.Expr:
    """ QA wdrs_result

    Usage
    -----
    To be used in a .with_columns() statement. This function will flag rows where there are unexpected test results.
    
    Parameters
    ----------
    wdrs_res_output: str
        wdrs result output from transformation functions

    Examples
    --------
    ```{python}
    #| echo: false
    {{< include "../_setup.qmd" >}}
    ```

    ```{python}
    from wadoh_subtyping import qa
    from wadoh_raccoon.utils import helpers
    import polars as pl
 
    wdrs = pl.DataFrame({
        "wdrs_res_output": [
           "G_FLU_ND",
            "G_FLU_A_D", 
            "Random Value", 
            None
        ]
    })


    df = (
        wdrs
        .with_columns(
            qa.qa_wdrs_result(wdrs_res_output='wdrs_res_output')
        )
    )
    helpers.gt_style(df_inp=df)
    ``` 
    """
    
    accepted_values = [
        "G_FLU_ND",
        "G_FLU_A_(09_PDM_H1N1)_D",
        "G_FLU_A_(H3)_D",
        "G_FLU_A_(H5)_D",
        "G_FLU_A_(H7)_D",
        "G_FLU_A_D",
        "G_FLU_B_(VICTORIA)_D",
        "G_FLU_B_(YAMAGATA)_D",
        "G_FLU_B",
        "RP"
    ]
    return (
        pl.when(
            (pl.col(wdrs_res_output).is_in(accepted_values).not_()) |
            (pl.col(wdrs_res_output).is_null())
        )
        .then(True)
        .otherwise(False)
        .alias('qa_wdrs_result')
    )

def qa_wdrs_result_summary(wdrs_res_sum_output: str) -> pl.Expr:
    """ QA wdrs_result_summary

    Usage
    -----
    To be used in a .with_columns() statement

    Parameters
    ----------
    wdrs_res_sum_output: str
        wdrs result summary column output from transformation functions
    
    Examples
    --------
    ```{python}
    #| echo: false
    {{< include "../_setup.qmd" >}}
    ```
    ```{python}
    from wadoh_subtyping import qa
    from wadoh_raccoon.utils import helpers 
    import polars as pl

    roster = pl.DataFrame({
        "wdrs_res_sum_output": [
            'G_POSITIVE',
            'G_NEGATIVE',
            None        ]
    })
    df = (
        roster
        .with_columns(
            qa.qa_wdrs_result_summary(wdrs_res_sum_output='wdrs_res_sum_output')
        )
    )
    
    helpers.gt_style(df_inp=df)
    ``` 
    """
    
    return (
        pl.when(
            (pl.col(wdrs_res_sum_output).is_in(['G_POSITIVE','G_NEGATIVE']).not_()) |
            (pl.col(wdrs_res_sum_output).is_null())
        )
        .then(True)
        .otherwise(False)
        .alias('qa_wdrs_result_summary')
    )

def qa_multiple_subtypes_deprecated(transformed_df_inp: pl.DataFrame):
    """ QA multiple_subtypes

    Usage
    -----
    To be used in a .with_columns() statement

    Parameters
    ----------
    transform

    
    Examples
    --------
    ```{python}
    #| echo: false
    {{< include "../_setup.qmd" >}}
    ```
    ```{python}
    from wadoh_subtyping import qa
    from wadoh_raccoon.utils import helpers 
    import polars as pl

    roster = pl.DataFrame({
        ""
    })

    df = (
        df
        .with_columns(
            qa.qa_multiple_subtypes(
                transformed_df_inp=transformed_df,
                wdrs_res_sum_output=wdrs_res_sum_output,
                test_res_output=test_res_output,
                wdrs_res_output=wdrs_res_output
            )
        )
    )
    ``` 
    """
        
    # filter out the regular and test subtypes
    # then select the Results that had them
    # select unique on PHLAccessionNumber and WDRS__RESULT to see if there are multiple tests that were 'detected' when there shouldn't have been
    check = (
        transformed_df_inp
        .filter((pl.col('WDRS_RESULT_SUMMARY').str.contains('G_POS')) & (pl.col('TEST_RESULT').str.contains('RP')).not_())
        .filter((pl.col('WDRS_RESULT').str.contains('G_FLU_A_D|G_FLU_B')).not_())
        .filter(pl.col('WDRS_RESULT').str.contains('G_'))
        .unique(subset=['submission_number','WDRS_RESULT'])
        # .select(['AnalyteSynonym', 'TestResult', 'Interpretation','WDRS_RESULT','submission_number'])
        .group_by('submission_number')
        .len(name="count")
        .filter(pl.col('count')>1)
        .sort(pl.col('count'),descending=True)
        .with_columns(
            qa_multiple_subtypes=True
        )
        .select(['submission_number','qa_multiple_subtypes'])
    )

    # join to transformed_df to get the records that have multiple subtypes
    df = (
        check
        .join(transformed_df_inp,how='left',on='submission_number')
    )

    return df 

def qa_multiple_subtypes(
        wdrs_res_sum_output: str,
        test_res_output: str,
        wdrs_res_output: str,
        transformed_df_inp: pl.DataFrame
    ):
    """ QA multiple_subtypes

    Usage
    -----
    To be used on a pl.DataFrame. Sometimes LIMS labels two different subtypes as `Detected`
    for the same specimen. For example, one specimen may be labeled as `H3` detected _and_ `H1N1`
    detected. This shouldn't happen. The `qa_multiple_subtypes2` function will flag these records
    for further review.

    Parameters
    ----------
    wdrs_res_sum_output: str
        wdrs result summary output col from transformation functions
    test_res_output: str
        test result output col from transformation functions
    wdrs_res_output: str
        wdrs result output col from transformation functions
    transformed_df_inp: pl.DataFrame
        transformed dataframe
    
    Examples
    --------
    ```{python}
    #| echo: false
    {{< include "../_setup.qmd" >}}
    ```
    
    Here is the transformed dataframe that contains a record with multiple subtypes 
    (when it should only have one subtype)
    ```{python}
    from wadoh_subtyping import qa
    from wadoh_raccoon.utils import helpers
    import polars as pl
 
    transformed_df = pl.DataFrame({
        "submission_number": ['200','200'],
        "wdrs_res_sum_output": [
            "G_POSITIVE",
            "G_POSITIVE"
        ],
        "test_res_output": [
            "Influenza A (09 Pdm H1N1) detected",
            "Influenza A (H3) detected"
        ],
        "wdrs_res_output": [
            "G_FLU_A_(09_PDM_H1N1)_D",
            "G_FLU_A_(H3)_D"
        ]
    })
    ```

    Apply the function

    ```{python}
    qa_mult_subtypes = (
        qa.qa_multiple_subtypes(
            transformed_df_inp=transformed_df,
            wdrs_res_sum_output='wdrs_res_sum_output',
            test_res_output='test_res_output',
            wdrs_res_output='wdrs_res_output'
        )
    )

    ```
    ```{python}
    #| echo: false
    from great_tables import GT, md, style, loc, google_font
    (
        helpers.gt_style(df_inp=qa_mult_subtypes.select(['test_res_output','qa_multiple_subtypes']),index_inp=False)
        .tab_style(
            style=[
                style.fill(color="#cbe2f9"),
                style.text(weight="bold")
            ],
            locations=loc.column_labels(columns=['qa_multiple_subtypes'])
        )
        .tab_style(
            style=[
                style.fill(color="#cbe2f9"),
                style.text(weight="bold")
            ],
            locations=loc.body(columns=['qa_multiple_subtypes'])
        )
    )

    ``` 
    
    So now we can use this flag and filter the records out:
    ```{python}
    apply_qa = (
        transformed_df
        .with_columns(
            pl.when(pl.col('submission_number').is_in(qa_mult_subtypes['submission_number']))
            .then(True)
            .otherwise(False)
            .alias('qa_multiple_subtypes')
        )
    )

    ```

    """
           

    subtype_list = [
        'G_FLU_A_(09_PDM_H1N1)_D',
        'G_FLU_A_(H3)_D',
        'G_FLU_A_(H5)_D',
        'G_FLU_A_(H7)_D',
        'G_FLU_B_(VICTORIA)_D',
        'G_FLU_B_(YAMAGATA)_D'
    ]

    df =  (
        transformed_df_inp
        .with_columns(
            pl.when(
                # when the result is positive and IS a subtype..
                (pl.col(wdrs_res_sum_output).str.contains('G_POSITIVE')) &
                # not an RP test
                (pl.col(test_res_output).str.contains('RP').not_()) &
                (pl.col(wdrs_res_output).is_in(subtype_list))
            )
            .then(True)
            .otherwise(False)
            .alias('temp_mult')
        )
        .filter(pl.col('temp_mult'))
        .unique(subset=['submission_number','temp_mult',wdrs_res_output])
        .group_by('submission_number')
        .len(name="count")
        .filter(pl.col('count')>1)
        .sort(pl.col('count'),descending=True)
        .with_columns(qa_multiple_subtypes=True)
        .select(['submission_number','qa_multiple_subtypes'])
    )

    # join back to transformed df to get all columns 
    joined_df = (
        df
        .join(transformed_df_inp,how='left',on='submission_number')
    )
    
    return joined_df
    