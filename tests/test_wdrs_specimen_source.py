import polars as pl
from polars.testing import assert_series_equal
import pytest
from wadoh_subtyping.transform import wdrs_specimen_source


@pytest.fixture(scope='function')
def get_df():
    """
    Get the data
    """
    df = pl.DataFrame({
        "SpecimenSource": [
            "Respiratory: NP swab",
            "random",
            None
        ]

    })

    df_output = (
        df
        .with_columns(
            WDRS_SPECIMEN_SOURCE=wdrs_specimen_source(wdrs_spec_source_col='SpecimenSource')
        )
    )

    return df_output


# ---- test the function ---- #

# test with polars
def test_wdrs_specimen_source_polars(get_df):
    """
    Test if the column names of the transformed dataframe
    match the columns of the expected outputs
    """
    df_output = get_df

    x = (
        pl.DataFrame({
            'WDRS_SPECIMEN_SOURCE': [
                'G_NP_SWAB',
                None,
                None
            ]
        })
    )

    assert_series_equal(df_output['WDRS_SPECIMEN_SOURCE'],x['WDRS_SPECIMEN_SOURCE'])
    