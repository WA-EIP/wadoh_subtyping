import polars as pl
from polars.testing import assert_series_equal
import pytest
from wadoh_subtyping.transform import col_test_result


@pytest.fixture(scope='function')
def get_df():
    """
    Get the data
    """
    df = pl.DataFrame({
        "ResultTextConclusion": [
                "Influenza A virus detected by RT-PCR {58}",
                "Influenza A(2009 H1N1) virus detected by RT-PCR {65}", 
                "Influenza A(H3) virus detected by RT-PCR {64}", 
                "Influenza A(H5) virus detected by RT-PCR {70}",
                "Influenza A(H7) virus detected by RT-PCR",
                "Influenza B virus detected by RT-PCR {59}",
                "Influenza B/Victoria lineage detected by RT-PCR {91}",
                "Influenza B/Yamagata lineage detected by RT-PCR {91}",
                "Inconclusive - Invalid Result {68}",
                "Random"
            ]
    })

    df_output = (
        df
        .with_columns(
            TEST_RESULT=col_test_result(subtype='ResultTextConclusion')
        )
        .select('TEST_RESULT')
    )

    return df_output


# ---- test the function ---- #

# test with polars
def test_col_test_result_polars(get_df):
    """
    Test if the column names of the transformed dataframe
    match the columns of the expected outputs
    """
    df_output = get_df

    x = (
        pl.DataFrame({
            'TEST_RESULT': [
                'Influenza A detected',
                'Influenza A (09 Pdm H1N1) detected',
                'Influenza A (H3) detected',
                'Influenza A (H5) detected',
                'Influenza A (H7) detected',
                'Influenza B detected',
                'Influenza B (Victoria) detected',
                'Influenza B (Yamagata) detected',
                'Influenza not detected',
                'Random'

            ]
        })
    )

    assert_series_equal(df_output['TEST_RESULT'],x['TEST_RESULT'])
    