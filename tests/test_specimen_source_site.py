import polars as pl
from polars.testing import assert_series_equal
import pytest
from wadoh_subtyping.transform import specimen_source_site


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
            SPECIMEN_SOURCE_SITE=specimen_source_site(spec_source_col='SpecimenSource')
        )
    )

    return df_output


# ---- test the function ---- #

# test with polars
def test_specimen_source_site_polars(get_df):
    """
    Test if the column names of the transformed dataframe
    match the columns of the expected outputs
    """
    df_output = get_df

    x = (
        pl.DataFrame({
            'SPECIMEN_SOURCE_SITE': [
                'Nasopharyngeal (NP) swab',
                None,
                None
            ]
        })
    )

    assert_series_equal(df_output['SPECIMEN_SOURCE_SITE'],x['SPECIMEN_SOURCE_SITE'])
    