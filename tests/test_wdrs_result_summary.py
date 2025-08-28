import polars as pl
from polars.testing import assert_series_equal
import pytest
from wadoh_subtyping.transform import wdrs_result_summary


@pytest.fixture(scope='function')
def get_df():
    """
    Get the data
    """
    df = pl.DataFrame({
        "TestResult": [
            "Not Detected {52}",
            "Detected {51}",
            "Test {53}"
        ]
    })

    df_output = (
        df
        .with_columns(
            WDRS_RESULT_SUMMARY=wdrs_result_summary('TestResult')
        )
    )

    return df_output


# ---- test the function ---- #

# test with polars
def test_wdrs_result_summary_polars(get_df):
    """
    Test if the column names of the transformed dataframe
    match the columns of the expected outputs
    """
    df_output = get_df

    x = (
        pl.DataFrame({
            'WDRS_RESULT_SUMMARY': [
                'G_NEGATIVE',
                'G_POSITIVE',
                None
            ]
        })
    )

    assert_series_equal(df_output['WDRS_RESULT_SUMMARY'],x['WDRS_RESULT_SUMMARY'])
    