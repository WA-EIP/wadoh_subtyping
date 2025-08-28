import polars as pl
from polars.testing import assert_series_equal
import pytest
from wadoh_subtyping.transform import submitter_other


@pytest.fixture(scope='function')
def get_df():
    """
    Get the data
    """
    df = pl.DataFrame({
        "SUBMITTER": [
            '85740',
            '27782022',
            '27782022'
        ],
        "lab": [
            'Everett Clinic Laboratory',
            'some random lab',
            None
        ]
    })

    df_output = (
        df
        .with_columns(
            SUBMITTER_OTHER=submitter_other(submitting_lab='SUBMITTER',submitter='lab')
        )
    )

    return df_output


# ---- test the function ---- #

# test with polars
def test_submitter_other_polars(get_df):
    """
    Test if the column names of the transformed dataframe
    match the columns of the expected outputs
    """
    df_output = get_df

    x = (
        pl.DataFrame({
            'SUBMITTER_OTHER': [
                None,
                'some random lab',
                None
            ]
        })
    )

    assert_series_equal(df_output['SUBMITTER_OTHER'],x['SUBMITTER_OTHER'])
    