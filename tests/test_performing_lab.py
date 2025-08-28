import polars as pl
from polars.testing import assert_series_equal
import pytest
from wadoh_subtyping.transform import performing_lab


@pytest.fixture(scope='function')
def get_df():
    """
    Get the data
    """
    df = pl.DataFrame({
        "lab_name": [
            "Lab",
            "Lab2",
            "Lab3",
        ]
    })

    df_output = (
        df
        .with_columns(
            PERFORMING_LAB_ENTIRE_REPORT=performing_lab()
        )
    )

    return df_output


# ---- test the function ---- #

# test with polars
def test_performing_lab_polars(get_df):
    """
    Test if the column names of the transformed dataframe
    match the columns of the expected outputs
    """
    df_output = get_df

    x = (
        pl.DataFrame({
            'PERFORMING_LAB_ENTIRE_REPORT': [
                '81596',
                '81596',
                '81596'
            ]
        })
     )

    assert_series_equal(df_output['PERFORMING_LAB_ENTIRE_REPORT'],x['PERFORMING_LAB_ENTIRE_REPORT'])
    