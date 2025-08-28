import polars as pl
from polars.testing import assert_series_equal
import pytest
from wadoh_subtyping.transform import wdrs_test_performed


@pytest.fixture(scope='function')
def get_df():
    """
    Get the data
    """
    df = pl.DataFrame({
        "ResultTextConclusion": [
            "Influenza A(2009 H1N1) virus detected by RT-PCR {65}", 
            "Influenza B/Victoria lineage detected by RT-PCR {91}", 
            "Inconclusive - sample below limit of detection of test {61}",
            "Influenza B virus detected by RT-PCR {59}"
        ]

    })

    df_output = (
        df
        .with_columns(
            WDRS_TEST_PERFORMED=wdrs_test_performed(wdrs_test_perf_col='ResultTextConclusion')
        )
    )

    return df_output


# ---- test the function ---- #

# test with polars
def test_wdrs_test_performed_polars(get_df):
    """
    Test if the column names of the transformed dataframe
    match the columns of the expected outputs
    """
    df_output = get_df

    x = (
        pl.DataFrame({
            'WDRS_TEST_PERFORMED': [
                'G_PCR',
                'G_PCR',
                None,
                'G_PCR'
            ]
        })
    )

    assert_series_equal(df_output['WDRS_TEST_PERFORMED'],x['WDRS_TEST_PERFORMED'])
    