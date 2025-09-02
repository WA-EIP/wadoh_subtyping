import polars as pl
from polars.testing import assert_series_equal
import pytest
from wadoh_subtyping.transform import submitting_lab


@pytest.fixture(scope='function')
def get_df():
    """
    Get the data
    """
    df = pl.DataFrame({
        "lab": [
            'Everett Clinic Laboratory',
            'some random lab',
            None
        ]
    })

    df_output = (
        df
        .with_columns(
            SUBMITTER=submitting_lab(submitter='lab')
        )
    )

    return df_output


# ---- test the function ---- #

# test with polars
def test_submitting_lab_polars(get_df):
    """
    Test if the column names of the transformed dataframe
    match the columns of the expected outputs
    """
    df_output = get_df

    x = (
        pl.DataFrame({
            'SUBMITTER': [
                '85742',
                '27782022',
                '27782022'
            ]
        })
    )

    assert_series_equal(df_output['SUBMITTER'],x['SUBMITTER'])
    