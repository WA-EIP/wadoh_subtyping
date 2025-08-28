import polars as pl
from polars.testing import assert_series_equal
import pytest
from wadoh_subtyping.transform import wdrs_result


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
            WDRS_RESULT=wdrs_result(subtype='ResultTextConclusion')
        )
    )

    return df_output


# ---- test the function ---- #

# test with polars
def test_wdrs_result_polars(get_df):
    """
    Test if the column names of the transformed dataframe
    match the columns of the expected outputs
    """
    df_output = get_df

    x = (
        pl.DataFrame({
            'WDRS_RESULT': [
                'G_FLU_A_D',
                'G_FLU_A_(09_PDM_H1N1)_D',
                'G_FLU_A_(H3)_D',
                'G_FLU_A_(H5)_D',
                'G_FLU_A_(H7)_D',
                'G_FLU_B',
                'G_FLU_B_(VICTORIA)_D',
                'G_FLU_B_(YAMAGATA)_D',
                'G_FLU_ND',
                'Random'
            ]
        })
    )

    assert_series_equal(df_output['WDRS_RESULT'],x['WDRS_RESULT'])
    