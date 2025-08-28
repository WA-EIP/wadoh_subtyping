import polars as pl
from polars.testing import assert_series_equal
import pytest
from wadoh_subtyping.transform import specimen_type


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
            SPECIMEN_TYPE=specimen_type(spec_type_col='SpecimenSource')
        )
    )

    return df_output


# ---- test the function ---- #

# test with polars
def test_specimen_type_polars(get_df):
    """
    Test if the column names of the transformed dataframe
    match the columns of the expected outputs
    """
    df_output = get_df

    x = (
        pl.DataFrame({
            'SPECIMEN_TYPE': [
                'Swab-nasopharyngeal (NP)',
                None,
                None
            ]
        })
    )

    assert_series_equal(df_output['SPECIMEN_TYPE'],x['SPECIMEN_TYPE'])
    