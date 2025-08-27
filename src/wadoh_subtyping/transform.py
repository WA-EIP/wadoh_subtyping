import polars as pl
import polars.selectors as cs
from wadoh_raccoon.utils import helpers
from datetime import date

def performing_lab() -> pl.Expr:
    """ Performing Lab

    Usage
    -----
    To be used within a .with_columns statement to produce new columns in a dataframe
    
    Examples
    --------
    Defaults to PHL. This needs to be a reference code for WDRS facility `WA State PHL (Public Health Laboratories)` which is `81594`

    ```{python}
    #| echo: false
    {{< include "../_setup.qmd" >}}
    ```
    ```{python}
    import polars as pl
    import polars.selectors as cs
    import wadoh_subtyping.transformations as tf
    import wadoh_subtyping.utils.helpers as helpers

    # Main DataFrame (df)
    df = pl.DataFrame({
        "FIRST_NAME": ["john", "a-lice", "BOb"]
    })

    df = (
        df
        .with_columns(
            PERFORMING_LAB_ENTIRE_REPORT = tf.performing_lab()
        )
    )

    helpers.gt_style(df_inp=df)

    
    ```


    """

    return pl.lit("81596").alias("PERFORMING_LAB_ENTIRE_REPORT")

def submitting_lab(submitter: str) -> pl.Expr:
    """ Submitting Lab

    Return a column with the value in the submitter column.
    - Note: The name for the submitter col is 'submitter' for all 3 data sources/pathways (e.g. PHL, ELR, MFT)')
    Name of column: SUBMITTING_LAB

    Usage
    -----
    The function can be called into a .with_columns() statement.

    Parameters
    ----------
    submitter: str
        name of submitter

    Examples
    --------
    ```{python}
    #| echo: false
    {{< include "../_setup.qmd" >}}
    ```
    **Note:** the submitting lab column uses reference codes from WDRS.
    There is a long list of reference codes, but not all labs have a reference code.
    Also, **LIMS may not have standard naming conventions for labs**. Therefore we need
    to map labs to their reference codes as best as we can, but then for labs that are not
    on the list, default their code to `27782022` and use the text field `SUBMITTER_OTHER`
    to fill in the lab name from LIMS


    ```{python}
    import polars as pl
    import polars.selectors as cs
    import wadoh_subtyping.transform as tf
    from wadoh_subtyping.utils import helpers

    # Main DataFrame (df)
    data = pl.DataFrame({
        "SubmittingLab": [
            "Labcorp - Seattle Cherry Hill",
            "Providence Reg Med Ctr - Everett",
            "University of Washington Medical Center",
            "Some Random Lab"
        ]
    })

    df = (
        data
        .with_columns(
            SUBMITTER = tf.submitting_lab(submitter="SubmittingLab"),
        )
        .with_columns(
            SUBMITTER_OTHER = tf.submitter_other(
                submitting_lab='SUBMITTER',
                submitter='SubmittingLab'
            )
        )
    )

    helpers.gt_style(df_inp=df)

    
    ```

    """
    # Create a dictionary with the provided values
    hospital_data = {
        "Cascade Valley Hospital": "80887",
        "Evergreen Health - Kirkland": "81993",
        "Harborview Medical Center": "82558",
        "Kaiser Permanente - Seattle": "2752312620",
        "Labcorp - Seattle Cherry Hill": "6130705570",
        "Laboratories Northwest": "87774",
        "Multicare Deaconess Hospital": "81546",
        "Overlake Hospital Medical Center": "84674",
        "Peace Hlth Lab SWMC - Vancouver": "85289",
        "Providence - Sacred Heart Medical Center": "85820",
        "Providence Everett Med Ctr Colby Campus": "85742",
        "Everett Clinic Laboratory": "85742",
        "Providence Reg Med Ctr - Everett": "85742",
        "Seattle Childrens Hosp": "86719",
        "Skagit Valley Reg. Med Center / Hospital": "6861256992",
        "St Francis Hospital Lab": "87277",
        "St Joseph Medical Center, Tacoma": "87300",
        "Swedish Hospital - Edmonds": "87489",
        "University of Washington Medical Center": "88215",
        "Valley Medical Center": "88319",
        "Kadlec Regional Medical Center": "83089",
        "Virginia Mason Medical Center": "88415"
    }

    return (
        pl.col(submitter).replace_strict(hospital_data,default='27782022')
    )

def submitter_other(submitting_lab: str,submitter: str) -> pl.Expr:
    """ Submitting Lab

    Usage
    -----
    The function can be called into a .with_columns() statement.

    Parameters
    ----------
    submitter: str
        name of transformed submitter column created by `tf.submitting_lab()`
    submitting_lab: str
        name of the lab

    Examples
    --------
    ```{python}
    #| echo: false
    {{< include "../_setup.qmd" >}}
    ```
    **Note:** the submitting lab column uses reference codes from WDRS.
    There is a long list of reference codes, but not all labs have a reference code.
    Also, **LIMS may not have standard naming conventions for labs**. Therefore we need
    to map labs to their reference codes as best as we can, but then for labs that are not
    on the list, default their code to `27782022` and use the text field `SUBMITTER_OTHER`
    to fill in the lab name from LIMS


    ```{python}
    import polars as pl
    import polars.selectors as cs
    import wadoh_subtyping.transform as tf
    from wadoh_subtyping.utils import helpers

    # Main DataFrame (df)
    data = pl.DataFrame({
        "SubmittingLab": [
            "Labcorp - Seattle Cherry Hill",
            "Providence Reg Med Ctr - Everett",
            "University of Washington Medical Center",
            "Some Random Lab"
        ]
    })

    df = (
        data
        .with_columns(
            SUBMITTER = tf.submitting_lab(submitter="SubmittingLab"),
        )
        .with_columns(
            SUBMITTER_OTHER = tf.submitter_other(
                submitting_lab='SUBMITTER',
                submitter='SubmittingLab'
            )
        )
    )

    helpers.gt_style(df_inp=df)


    ```

    """

    return (
        # combine Prov Everett labs to the same
        pl.when(pl.col(submitting_lab).str.contains('27782022'))
        .then(pl.col(submitter))

        .otherwise(None)
    )


def collection_date(df,col: str):
    """ Specimen Collection Date

    Convert collection date column to a standardized date format

    Usage
    -----
    The function can be called into a .with_columns() statement. It will reference the dataframe's status column and output a cleaned version of it.

    Parameters
    ----------
    col: str
        the sequence collection date column
    df: pl.DataFrame
        the dataframe of interest

    Examples
    --------
    ```{python}
    #| echo: false
    {{< include "../_setup.qmd" >}}
    ```

    For PHL/Template dates:

    ```{python}
    import polars as pl
    import wadoh_subtyping.transform as tf
    from wadoh_subtyping.utils import helpers

    df = pl.DataFrame({
        "submitted_date": [
            '11/7/2021',
            '12/10/2023',
            None
        ]
    })

    # how to apply the function
    df = (
        df
        .with_columns(
            SPECIMEN_COLLECTION_DTTM=tf.collection_date(df,col="submitted_date")
        )
    )

    # table output
    helpers.gt_style(df_inp=df)
    ```


    """
    return helpers.date_format(df,col)

def specimen_type(spec_type_col: str):
        """ Specimen Type

        
        Usage
        -----
        The function can be called into a .with_columns() statement. It will reference the dataframe's status column and output a cleaned version of it.

        Parameters
        ----------
        spec_type_col: str
            specimen type column
        
        Examples
        --------
        ```{python}
        #| echo: false
        {{< include "../_setup.qmd" >}}
        ```
        ```{python}
        import polars as pl
        import wadoh_subtyping.transform as tf
        from wadoh_subtyping.utils import helpers

        df = pl.DataFrame({
            "SpecimenSource": [
                "Resipiratory: nasal swab",
                "Nasopharyngeal Swab (NP)",
                "Conjunctival Swab"
            ]
        })

        # here's how to apply the function
        df = (
            df
            .with_columns(
                SPECIMEN_TYPE=tf.specimen_type(spec_type_col='SpecimenSource')
            )
        )

        # here's a table output
        helpers.gt_style(df_inp=df)
        ```
        """
        spec_type_map = {
            # Nasal Swabs >>> Nose
            'Nasal Swabs': 'Swab-nasal',
            'Respiratory: nasal swab': 'Swab-nasal',

            # Nasopharyngeal Swab (NP)
            'Nasopharyngeal Swab (NP)': 'Swab-nasopharyngeal (NP)', 
            'Respiratory: NP swab': 'Swab-nasopharyngeal (NP)',
            
            # Oropharyngeal Swab
            'Oropharyngeal Swab': 'Swab-oropharyngeal (OP)',
            
            # Sputum
            'Sputum': 'Sputum'

        }

        return(
            pl.col(spec_type_col).replace_strict(spec_type_map,default=None)
        )

def wdrs_specimen_type(wdrs_spec_type_col: str):
        """ WDRS Specimen Type 

        
        Usage
        -----
        The function can be called into a .with_columns() statement. It will reference the dataframe's status column and output a cleaned version of it.

        Parameters
        ----------
        wdrs_spec_type_col: str
            specimen type column
        
        Examples
        --------
        ```{python}
        #| echo: false
        {{< include "../_setup.qmd" >}}
        ```
        ```{python}
        import polars as pl
        import wadoh_subtyping.transform as tf
        from wadoh_subtyping.utils import helpers

        df = pl.DataFrame({
            "submitter": ["PHL", "PHL", "PHL"],
            "SpecimenSource": [
                "Resipiratory: nasal swab",
                "Nasopharyngeal Swab (NP)",
                "Conjunctival Swab"
            ]
        })

        # here's how to apply the function to a dataframe
        df = (
            df
            .with_columns(
                WDRS_SPECIMEN_TYPE=tf.wdrs_specimen_type(wdrs_spec_type_col='SpecimenSource')
            )
        )

        # here's a table of the results
        helpers.gt_style(df_inp=df)
        ```
        """
        wdrs_spec_type_map = {
            # Nasal Swabs >>> Nose
            'Nasal Swabs': 'G_SWAB_NASAL',
            'Respiratory: nasal swab': 'G_SWAB_NASAL',

            # Nasopharyngeal Swab (NP)
            'Nasopharyngeal Swab (NP)': 'G_SWAB-NP', 
            'Respiratory: NP swab': 'G_SWAB-NP',
            
            # Oropharyngeal Swab
            'Oropharyngeal Swab': 'G_SWAB-OP',
            
            # Sputum
            'Sputum': 'G_SPUTUM'

            # unsure what VTM or raw sludge maps to
            # note that if LIMS has different spelling or case, this mapping wont work. it will default to null
        }

        return(
            pl.col(wdrs_spec_type_col).replace_strict(wdrs_spec_type_map,default=None)
        )

def specimen_source_site(spec_source_col: str):
        """ WDRS Specimen Type 

        
        Usage
        -----
        The function can be called into a .with_columns() statement. It will reference the dataframe's status column and output a cleaned version of it.

        Parameters
        ----------
        spec_source_col: str
            specimen source column
        
        Examples
        --------
        ```{python}
        #| echo: false
        {{< include "../_setup.qmd" >}}
        ```
        ```{python}
        import polars as pl
        import wadoh_subtyping.transform as tf
        from wadoh_subtyping.utils import helpers

        df = pl.DataFrame({
            "submitter": ["PHL", "PHL", "PHL"],
            "SpecimenSource": [
                "Resipiratory: nasal swab",
                "Nasopharyngeal Swab (NP)",
                "Conjunctival Swab"
            ]
        })

        # here's how to apply the function to a dataframe
        df = (
            df
            .with_columns(
                SPECIMEN_SOURCE_SITE=tf.specimen_source_site(spec_source_col='SpecimenSource')
            )
        )

        # here's a table of the results
        helpers.gt_style(df_inp=df)
        ```
        """
        spec_source_map = {
            # Nasal Swabs >>> Nose
            'Nasal Swabs': 'Nose',
            'Respiratory: nasal wash': 'Nose',
            'Respiratory: nasal swab': 'Nose',

            # Nasopharyngeal Swab (NP)
            'Nasopharyngeal Swab (NP)': 'Nasopharyngeal (NP) swab', 
            'Respiratory: NP swab': 'Nasopharyngeal (NP) swab',
            
            # Oropharyngeal Swab
            'Oropharyngeal Swab': 'Oropharynx',
            
            # Conjunctival Swab Eye or S_OCULAR?
            'Conjunctival Swab': 'Eye',
            'Conjunctival swab': 'Eye',
            
            # Sputum
            'Sputum': 'Sputum'
        }

        return(
            pl.col(spec_source_col).replace_strict(spec_source_map,default=None)
        )

def wdrs_specimen_source(wdrs_spec_source_col: str):
        """ WDRS Specimen Source

        
        Usage
        -----
        The function can be called into a .with_columns() statement. It will reference the dataframe's status column and output a cleaned version of it.

        Parameters
        ----------
        wdrs_spec_source_col: str
            specimen source column
        
        Examples
        --------
        ```{python}
        #| echo: false
        {{< include "../_setup.qmd" >}}
        ```
        ```{python}
        import polars as pl
        import wadoh_subtyping.transform as tf
        from wadoh_subtyping.utils import helpers

        df = pl.DataFrame({
            "SpecimenSource": [
                "Resipiratory: nasal swab",
                "Nasopharyngeal Swab (NP)",
                "Conjunctival Swab"
            ]
        })

        # here's how to apply the function
        df = (
            df
            .with_columns(
                WDRS_SPECIMEN_SOURCE=tf.wdrs_specimen_source(wdrs_spec_source_col='SpecimenSource')
            )
        )

        # here's a table output
        helpers.gt_style(df_inp=df)
        ```
        """
        wdrs_spec_source_map = {
            # Nasal Swabs >>> Nose
            'Nasal Swabs': 'G_NOSE',
            'Respiratory: nasal wash': 'G_NOSE',
            'Respiratory: nasal swab': 'G_NOSE',

            # Nasopharyngeal Swab (NP)
            'Nasopharyngeal Swab (NP)': 'G_NP_SWAB', 
            'Respiratory: NP swab': 'G_NP_SWAB',
            
            # Oropharyngeal Swab
            'Oropharyngeal Swab': 'G_OROPHARYNX',
            
            # Conjunctival Swab Eye or S_OCULAR?
            'Conjunctival Swab': 'G_EYE',
            'Conjunctival swab': 'G_EYE',
            
            # Sputum
            'Sputum': 'G_SPUTUM'
        }

        return(
            pl.col(wdrs_spec_source_col).replace_strict(wdrs_spec_source_map,default=None)
        )

def wdrs_test_performed(wdrs_test_perf_col: str):
        """ WDRS Test Performed

        
        Usage
        -----
        The function can be called into a .with_columns() statement. It will reference the dataframe's status column and output a cleaned version of it.

        Parameters
        ----------
        wdrs_test_perf_col: str
            test performed column
        
        Examples
        --------
        ```{python}
        #| echo: false
        {{< include "../_setup.qmd" >}}
        ```
        ```{python}
        import polars as pl
        import wadoh_subtyping.transform as tf
        from wadoh_subtyping.utils import helpers

        df = pl.DataFrame({
            "ResultTextConclusion": [
                "Influenza A(2009 H1N1) virus detected by RT-PCR {65}", 
                "Influenza B/Victoria lineage detected by RT-PCR {91}", 
                "Inconclusive - sample below limit of detection of test {61}",
                "Influenza B virus detected by RT-PCR {59}"
            ]
        })
        
        # run the function
        df = (
            df
            .with_columns(
                WDRS_TEST_PERFORMED=tf.wdrs_test_performed(wdrs_test_perf_col='ResultTextConclusion')
            )
        )
        
        # here's a table of the results
        helpers.gt_style(df_inp=df)
        ```

        ## WDRS Variable Mapping

        Below are all distinct options in WDRS for the `WDRS_TEST_PERFORMED` variable and how the values map to LIMS data.

        ```{python}
        #| echo: false

        from great_tables import GT, md, style, loc, google_font


        wdrs = pl.DataFrame({
            "LIMS-ResultTextConclusion": [
                        "Inconclusive {50}",
                        "Influenza A virus detected by RT-PCR {58}",
                        "Influenza A(2009 H1N1) virus detected by RT-PCR {65}",
                        "Influenza A(H3) virus detected by RT-PCR {64}", 
                        "Influenza B virus detected by RT-PCR {59}",
                        ""
                    ],
            "WDRS_TEST_PERFORMED": [
                "Direct fluorescent antibody (DFA) / Immunohistochemistry (IHC)",
                "Manual Review", 
                "PCR/Nucleic Acid Test (NAT, NAAT, DNA)", 
                "Rapid test (rapid EIA)",
                "Viral culture (isolation)",
                "Other, specify"
            ]
        })

        (
            helpers.gt_style(
                df_inp=wdrs,
                add_striping_inp=False,
                index_inp=False
            )
            .tab_style(
                    style=[
                            style.fill(color="rgb(114, 219, 248)"),
                            style.text(weight="bold")
                        ],
                        locations=loc.body(rows=[2])
            )
            .tab_style(
                    style=[
                            style.fill(color="rgb(114, 219, 248)"),
                            style.text(weight="bold")
                        ],
                        locations=loc.body(columns='LIMS-ResultTextConclusion',rows=[1,2,3,4])
            )
            
            
            )
        ```
        """
        return(
            # pl.when((pl.col(wdrs_test_perf_col).is_null()))
            # .then(pl.lit("Unknown Type"))

            pl.when((pl.col(wdrs_test_perf_col).str.to_uppercase().str.contains("51|INCONCLUSIVE")))
            .then(None)

            .when((pl.col(wdrs_test_perf_col).str.to_uppercase().str.contains("RT-PCR")))
            # .then(pl.lit("PCR/Nucleic Acid Test (NAT, NAAT, DNA)"))
            .then(pl.lit("G_PCR"))

            # i need to confirm what the possible values for this stuff from phl
            # .when((pl.col(specimen_type).str.contains(r"(?!)soft tissue")))
            # .then(pl.lit("SOFT TISSUE SPECIMEN"))

            .otherwise(None)

        )

def col_test_performed_desc(wdrs_test_perf_col: str):
        """ WDRS Test Performed Description

        Usage
        -----
        The function can be called into a .with_columns() statement. It will reference the dataframe's status column and output a cleaned version of it.

        Parameters
        ----------
        wdrs_test_perf_col: str
            test performed column
        
        Examples
        --------
        ```{python}
        #| echo: false
        {{< include "../_setup.qmd" >}}
        ```
        ```{python}
        import polars as pl
        import wadoh_subtyping.transform as tf
        from wadoh_subtyping.utils import helpers

        df = pl.DataFrame({
            "ResultTextConclusion": [
                "Influenza A(2009 H1N1) virus detected by RT-PCR {65}", 
                "Influenza B/Victoria lineage detected by RT-PCR {91}", 
                "Inconclusive - sample below limit of detection of test {61}",
                "Influenza B virus detected by RT-PCR {59}"
            ]
        })
        
        # run the function
        df = (
            df
            .with_columns(
                TEST_PERFORMED_DESC=tf.col_test_performed_desc(wdrs_test_perf_col='ResultTextConclusion')
            )
        )
        
        # here's a table of the results
        helpers.gt_style(df_inp=df)
        ```

        ## WDRS Variable Mapping

        Below are all distinct options in WDRS for the `WDRS_TEST_PERFORMED` variable and how the values map to LIMS data.

        ```{python}
        #| echo: false

        from great_tables import GT, md, style, loc, google_font


        wdrs = pl.DataFrame({
            "LIMS-ResultTextConclusion": [
                        "Inconclusive {50}",
                        "Influenza A virus detected by RT-PCR {58}",
                        "Influenza A(2009 H1N1) virus detected by RT-PCR {65}",
                        "Influenza A(H3) virus detected by RT-PCR {64}", 
                        "Influenza B virus detected by RT-PCR {59}",
                        ""
                    ],
            "WDRS_TEST_PERFORMED": [
                "Direct fluorescent antibody (DFA) / Immunohistochemistry (IHC)",
                "Manual Review", 
                "PCR/Nucleic Acid Test (NAT, NAAT, DNA)", 
                "Rapid test (rapid EIA)",
                "Viral culture (isolation)",
                "Other, specify"
            ]
        })

        (
            helpers.gt_style(
                df_inp=wdrs,
                add_striping_inp=False,
                index_inp=False
            )
            .tab_style(
                    style=[
                            style.fill(color="rgb(114, 219, 248)"),
                            style.text(weight="bold")
                        ],
                        locations=loc.body(rows=[2])
            )
            .tab_style(
                    style=[
                            style.fill(color="rgb(114, 219, 248)"),
                            style.text(weight="bold")
                        ],
                        locations=loc.body(columns='LIMS-ResultTextConclusion',rows=[1,2,3,4])
            )
            
            
            )
        ```
        """
        return(
            # pl.when((pl.col(wdrs_test_perf_col).is_null()))
            # .then(pl.lit("Unknown Type"))

            pl.when((pl.col(wdrs_test_perf_col).str.to_uppercase().str.contains("51|INCONCLUSIVE")))
            .then(None)

            .when((pl.col(wdrs_test_perf_col).str.to_uppercase().str.contains("RT-PCR")))
            .then(pl.lit("PCR/Nucleic Acid Test (NAT, NAAT, DNA)"))

            # i need to confirm what the possible values for this stuff from phl
            # .when((pl.col(specimen_type).str.contains(r"(?!)soft tissue")))
            # .then(pl.lit("SOFT TISSUE SPECIMEN"))

            .otherwise(None)

        )


def uw_result(subtype: str):

        return(
            
            pl.when((pl.col(subtype).str.contains("None|60|61|62|50|68|66|66")))
            .then(pl.lit("G_FLU_ND"))

            .when((pl.col(subtype).str.contains("Influenza A H1N1 detected"))) 
            .then(pl.lit("G_FLU_A_(09_PDM_H1N1)_D"))

            .when((pl.col(subtype).str.contains("Influenza A H3N2 detected"))) 
            .then(pl.lit("G_FLU_A_(H3)_D"))
            
            .when((pl.col(subtype).str.contains("Influenza A\\(H5\\) virus detected by RT-PCR"))) 
            .then(pl.lit("G_FLU_A_(H5)_D"))

            .when((pl.col(subtype).str.contains("Influenza A\\(H7\\) virus detected by RT-PCR"))) 
            .then(pl.lit("G_FLU_A_(H7)_D"))

            .when((pl.col(subtype).str.contains("Influenza A virus detected by RT-PCR"))) 
            .then(pl.lit("G_FLU_A_D"))
            
            .when((pl.col(subtype).str.contains("Influenza B/Victoria lineage detected by RT-PCR"))) 
            .then(pl.lit("G_FLU_B_(VICTORIA)_D"))

            .when((pl.col(subtype).str.contains("Influenza B/Yamagata lineage detected by RT-PCR"))) 
            .then(pl.lit("G_FLU_B_(YAMAGATA)_D"))
            
            .when((pl.col(subtype).str.contains("Influenza B detected"))) 
            .then(pl.lit("G_FLU_B"))

            .otherwise(pl.col(subtype).str.strip_chars())

        )

def uw_col_test_result(subtype: str):

        return(
            
            
            pl.when((pl.col(subtype).str.contains("None|60|61|62|50|68|66|66")))
            .then(pl.lit("Influenza not detected"))

            .when((pl.col(subtype).str.contains("Influenza A H1N1 detected"))) 
            .then(pl.lit("Influenza A (09 Pdm H1N1) detected"))

            .when((pl.col(subtype).str.contains("Influenza A H3N2 detected"))) 
            .then(pl.lit("Influenza A (H3) detected"))
            
            .when((pl.col(subtype).str.contains("Influenza A\\(H5\\) virus detected by RT-PCR"))) 
            .then(pl.lit("Influenza A (H5) detected"))

            .when((pl.col(subtype).str.contains("Influenza A\\(H7\\) virus detected by RT-PCR"))) 
            .then(pl.lit("Influenza A (H7) detected"))

            .when((pl.col(subtype).str.contains("Influenza A virus detected by RT-PCR"))) 
            .then(pl.lit("Influenza A detected"))
            
            .when((pl.col(subtype).str.contains("Influenza B/Victoria lineage detected by RT-PCR"))) 
            .then(pl.lit("Influenza B (Victoria) detected"))

            .when((pl.col(subtype).str.contains("Influenza B/Yamagata lineage detected by RT-PCR"))) 
            .then(pl.lit("Influenza B (Yamagata) detected"))
            
            .when((pl.col(subtype).str.contains("Influenza B detected"))) 
            .then(pl.lit("Influenza B detected"))

            .otherwise(pl.col(subtype).str.strip_chars())

        )


def wdrs_result(subtype: str):
        """ WDRS Test Result

        
        Usage
        -----
        The function can be called into a .with_columns() statement. It will reference the dataframe's status column and output a cleaned version of it.

        Parameters
        ----------
        subtype: str
            subtype column
        
        
        Examples
        --------
        ```{python}
        #| echo: false
        {{< include "../_setup.qmd" >}}
        ```
        ```{python}
        import polars as pl
        import wadoh_subtyping.transform as tf
        from wadoh_subtyping.utils import helpers

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
                "Influenza A virus detected by RT-PCR; Subtype undetected {66}",
                "None"

            ]
        })

        df = (
            df
            .with_columns(
                WDRS_RESULT=tf.wdrs_result(
                    subtype='ResultTextConclusion'
                )
            )
        )

        helpers.gt_style(df_inp=df)
        ```

        ## WDRS Variable Mapping

        Below are all distinct options in WDRS for the `WDRS_RESULT` variable and how the values map to LIMS data.

        :::{.columns}

        ::: {.column width="50%"}

        ```{python}
        #| echo: false
        from great_tables import GT, md, style, loc, google_font

        phl = pl.DataFrame({
                "AnalyteSynonym": [
                    "2009pandemic Influenza A {1049}",
                    "H1 2009pandemic {1050}",
                    "H1",
                    "H3", 
                    "Influenza A(H5)a {1062}",
                    "Influenza A(H5)b {1063}",
                    "Influenza A(H7)Eu {1069}",
                    "B(Victoria) {1095}",
                    "B(Yamagata) {1094}",
                    "Influenza B",
                    None,
                    "Adenovirus",
                    "CPE {1052}",
                    "H275",
                    "I223",
                    "Parainfluenza Type 1",
                    "Parainfluenza Type 2",
                    "Parainfluenza Type 3",
                    "Respiratory Syncytial Virus",
                    "RP"
                ]
            })
        (
            helpers.gt_style(
                df_inp=phl,
                title="LIMS Test Results",
                subtitle="All distinct values found",
                add_striping_inp=False,
                index_inp=False


            )
            .tab_style(
                    style=[
                            style.fill(color="#440154FF"),
                            style.text(weight="bold"),
                            style.css(rule="color: rgb(244, 227, 243)")
                        ],
                        locations=loc.body(rows=[0,1])
            )
            .tab_style(
                    style=[
                            style.fill(color="#482777FF"),
                            style.text(weight="bold"),
                            style.css(rule="color: rgb(244, 227, 243)")
                        ],
                        locations=loc.body(rows=[3])
            )
            .tab_style(
                    style=[
                            style.fill(color="#3F4A89FF"),
                            style.text(weight="bold"),
                            style.css(rule="color: rgb(244, 227, 243)")
                        ],
                        locations=loc.body(rows=[4,5])
            )
            .tab_style(
                    style=[
                            style.fill(color="#26828EFF"),
                            style.text(weight="bold"),
                            style.css(rule="color: rgb(244, 227, 243)")
                        ],
                        locations=loc.body(rows=[6])
            )
            .tab_style(
                    style=[
                            style.fill(color="#3F9A58FF"),
                            style.text(weight="bold")
                        ],
                        locations=loc.body(rows=[7])
            )
            .tab_style(
                    style=[
                            style.fill(color="#5CC863FF"),
                            style.text(weight="bold")
                        ],
                        locations=loc.body(rows=[8])
            )
            .tab_style(
                    style=[
                            style.fill(color="#F0D800FF"),
                            style.text(weight="bold")
                        ],
                        locations=loc.body(rows=[9])
            )
            
        )
        ```
        :::

        ::: {.column width="50%"}

        ```{python}
        #| echo: false

        from great_tables import GT, md, style, loc, google_font


        wdrs = pl.DataFrame({
            "WDRS_RESULT": [
                "Influenza A (09 Pdm H1N1) detected",
                "Influenza A (H3) detected", 
                "Influenza A (H3N2) detected", 
                "Influenza A (H5) detected",
                "Influenza A (H7) detected",
                "Influenza A detected",
                "Influenza A, other detected",
                "Influenza B (Victoria) detected",
                "Influenza B (Yamagata) detected",
                "Influenza B, other detected",
                "Influenza not detected",
                "Specimen inadequate",
                "Manual review",
                "Other, specify"

            ]
        })

        (
            helpers.gt_style(
                df_inp=wdrs,
                title="WDRS Test Results",
                subtitle="All distinct values found",
                add_striping_inp=False,
                index_inp=False

            )
            .tab_style(
                    style=[
                            style.fill(color="#440154FF"),
                            style.text(weight="bold"),
                            style.css(rule="color: rgb(244, 227, 243)")
                        ],
                        locations=loc.body(rows=[0])
            )
            .tab_style(
                    style=[
                            style.fill(color="#482777FF"),
                            style.text(weight="bold"),
                            style.css(rule="color: rgb(244, 227, 243)")
                        ],
                        locations=loc.body(rows=[1,2])
            )
            .tab_style(
                    style=[
                            style.fill(color="#3F4A89FF"),
                            style.text(weight="bold"),
                            style.css(rule="color: rgb(244, 227, 243)")
                        ],
                        locations=loc.body(rows=[3])
            )
            .tab_style(
                    style=[
                            style.fill(color="#26828EFF"),
                            style.text(weight="bold"),
                            style.css(rule="color: rgb(244, 227, 243)")
                        ],
                        locations=loc.body(rows=[4])
            )
            .tab_style(
                    style=[
                            style.fill(color="#3F9A58FF"),
                            style.text(weight="bold")
                        ],
                        locations=loc.body(rows=[7])
            )
            .tab_style(
                    style=[
                            style.fill(color="#5CC863FF"),
                            style.text(weight="bold")
                        ],
                        locations=loc.body(rows=[8])
            )
            #    .tab_style(
            #         style=[
            #                 style.fill(color="#F0D800FF"),
            #                 style.text(weight="bold")
            #             ],
            #             locations=loc.body(rows=[9])
            #    )
            # .tab_style(
            #         style=[
            #                 style.fill(color="rgb(197, 196, 193)"),
            #                 style.text(weight="bold")
            #             ],
            #             locations=loc.body(rows=[5,6,9,10,11])
            # )
            
            )
        ```

        :::


        :::

        """
        # result_map = {
        #     'None': 'G_FLU_ND',
        #     'Influenza virus not detected by RT-PCR {60}': 'G_FLU_ND',
        #     'Inconclusive - sample below limit of detection of test {61}': 'G_FLU_ND',
        #     'Influenza A virus not detected by RT-PCR {62}': 'G_FLU_ND',
        #     'Influenza A virus detected by RT-PCR; Subtype undetected {66}': 'G_FLU_ND',
        #     'Inconclusive {50}': 'G_FLU_ND',
        #     'Inconclusive - Invalid Result {68}': 'G_FLU_ND',

        #     'Influenza A(2009 H1N1) virus detected by RT-PCR {65}': 'G_FLU_A_(09_PDM_H1N1)_D',
        #     'Influenza A(H3) virus detected by RT-PCR {64}': 'G_FLU_A_(H3)_D',
        #     'Influenza A(H5) virus detected by RT-PCR {69}': 'G_FLU_A_(H5)_D'
        # }

        return(
            
            pl.when((pl.col(subtype).str.contains("None|60|61|62|50|68|66|66")))
            .then(pl.lit("G_FLU_ND"))

            .when((pl.col(subtype).str.contains("Influenza A\\(2009 H1N1\\) virus detected by RT-PCR"))) 
            .then(pl.lit("G_FLU_A_(09_PDM_H1N1)_D"))

            .when((pl.col(subtype).str.contains("Influenza A\\(H3\\) virus detected by RT-PCR"))) 
            .then(pl.lit("G_FLU_A_(H3)_D"))
            
            .when((pl.col(subtype).str.contains("Influenza A\\(H5\\) virus detected by RT-PCR"))) 
            .then(pl.lit("G_FLU_A_(H5)_D"))

            .when((pl.col(subtype).str.contains("Influenza A\\(H7\\) virus detected by RT-PCR"))) 
            .then(pl.lit("G_FLU_A_(H7)_D"))

            .when((pl.col(subtype).str.contains("Influenza A virus detected by RT-PCR"))) 
            .then(pl.lit("G_FLU_A_D"))
            
            .when((pl.col(subtype).str.contains("Influenza B/Victoria lineage detected by RT-PCR"))) 
            .then(pl.lit("G_FLU_B_(VICTORIA)_D"))

            .when((pl.col(subtype).str.contains("Influenza B/Yamagata lineage detected by RT-PCR"))) 
            .then(pl.lit("G_FLU_B_(YAMAGATA)_D"))
            
            .when((pl.col(subtype).str.contains("Influenza B virus detected by RT-PCR"))) 
            .then(pl.lit("G_FLU_B"))

            .otherwise(pl.col(subtype).str.strip_chars())

        )

def col_test_result(subtype: str):
        """ WDRS Test Result

        
        Usage
        -----
        The function can be called into a .with_columns() statement. It will reference the dataframe's status column and output a cleaned version of it.

        Parameters
        ----------
        subtype: str
            subtype column
        
        
        Examples
        --------
        ```{python}
        #| echo: false
        {{< include "../_setup.qmd" >}}
        ```
        ```{python}
        import polars as pl
        import wadoh_subtyping.transform as tf
        from wadoh_subtyping.utils import helpers

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
                "Influenza A virus detected by RT-PCR; Subtype undetected {66}",
                "None"
            ]

            
        })

        df = (
            df
            .with_columns(
                TEST_RESULT=tf.col_test_result(
                    subtype='ResultTextConclusion'
                )
            )
        )

        helpers.gt_style(df_inp=df)
        ```

        ## WDRS Variable Mapping

        Below are all distinct options in WDRS for the `WDRS_RESULT` variable and how the values map to LIMS data.

        :::{.columns}

        ::: {.column width="50%"}

        ```{python}
        #| echo: false
        from great_tables import GT, md, style, loc, google_font

        phl = pl.DataFrame({
                "AnalyteSynonym": [
                    "2009pandemic Influenza A {1049}",
                    "H1 2009pandemic {1050}",
                    "H1",
                    "H3", 
                    "Influenza A(H5)a {1062}",
                    "Influenza A(H5)b {1063}",
                    "Influenza A(H7)Eu {1069}",
                    "B(Victoria) {1095}",
                    "B(Yamagata) {1094}",
                    "Influenza B",
                    None,
                    "Adenovirus",
                    "CPE {1052}",
                    "H275",
                    "I223",
                    "Parainfluenza Type 1",
                    "Parainfluenza Type 2",
                    "Parainfluenza Type 3",
                    "Respiratory Syncytial Virus",
                    "RP"
                ]
            })
        (
            helpers.gt_style(
                df_inp=phl,
                title="LIMS Test Results",
                subtitle="All distinct values found",
                add_striping_inp=False,
                index_inp=False


            )
            .tab_style(
                    style=[
                            style.fill(color="#440154FF"),
                            style.text(weight="bold"),
                            style.css(rule="color: rgb(244, 227, 243)")
                        ],
                        locations=loc.body(rows=[0,1])
            )
            .tab_style(
                    style=[
                            style.fill(color="#482777FF"),
                            style.text(weight="bold"),
                            style.css(rule="color: rgb(244, 227, 243)")
                        ],
                        locations=loc.body(rows=[3])
            )
            .tab_style(
                    style=[
                            style.fill(color="#3F4A89FF"),
                            style.text(weight="bold"),
                            style.css(rule="color: rgb(244, 227, 243)")
                        ],
                        locations=loc.body(rows=[4,5])
            )
            .tab_style(
                    style=[
                            style.fill(color="#26828EFF"),
                            style.text(weight="bold"),
                            style.css(rule="color: rgb(244, 227, 243)")
                        ],
                        locations=loc.body(rows=[6])
            )
            .tab_style(
                    style=[
                            style.fill(color="#3F9A58FF"),
                            style.text(weight="bold")
                        ],
                        locations=loc.body(rows=[7])
            )
            .tab_style(
                    style=[
                            style.fill(color="#5CC863FF"),
                            style.text(weight="bold")
                        ],
                        locations=loc.body(rows=[8])
            )
            .tab_style(
                    style=[
                            style.fill(color="#F0D800FF"),
                            style.text(weight="bold")
                        ],
                        locations=loc.body(rows=[9])
            )
            
        )
        ```
        :::

        ::: {.column width="50%"}

        ```{python}
        #| echo: false

        from great_tables import GT, md, style, loc, google_font


        wdrs = pl.DataFrame({
            "WDRS_RESULT": [
                "Influenza A (09 Pdm H1N1) detected",
                "Influenza A (H3) detected", 
                "Influenza A (H3N2) detected", 
                "Influenza A (H5) detected",
                "Influenza A (H7) detected",
                "Influenza A detected",
                "Influenza A, other detected",
                "Influenza B (Victoria) detected",
                "Influenza B (Yamagata) detected",
                "Influenza B, other detected",
                "Influenza not detected",
                "Specimen inadequate",
                "Manual review",
                "Other, specify"

            ]
        })

        (
            helpers.gt_style(
                df_inp=wdrs,
                title="WDRS Test Results",
                subtitle="All distinct values found",
                add_striping_inp=False,
                index_inp=False

            )
            .tab_style(
                    style=[
                            style.fill(color="#440154FF"),
                            style.text(weight="bold"),
                            style.css(rule="color: rgb(244, 227, 243)")
                        ],
                        locations=loc.body(rows=[0])
            )
            .tab_style(
                    style=[
                            style.fill(color="#482777FF"),
                            style.text(weight="bold"),
                            style.css(rule="color: rgb(244, 227, 243)")
                        ],
                        locations=loc.body(rows=[1,2])
            )
            .tab_style(
                    style=[
                            style.fill(color="#3F4A89FF"),
                            style.text(weight="bold"),
                            style.css(rule="color: rgb(244, 227, 243)")
                        ],
                        locations=loc.body(rows=[3])
            )
            .tab_style(
                    style=[
                            style.fill(color="#26828EFF"),
                            style.text(weight="bold"),
                            style.css(rule="color: rgb(244, 227, 243)")
                        ],
                        locations=loc.body(rows=[4])
            )
            .tab_style(
                    style=[
                            style.fill(color="#3F9A58FF"),
                            style.text(weight="bold")
                        ],
                        locations=loc.body(rows=[7])
            )
            .tab_style(
                    style=[
                            style.fill(color="#5CC863FF"),
                            style.text(weight="bold")
                        ],
                        locations=loc.body(rows=[8])
            )
            #    .tab_style(
            #         style=[
            #                 style.fill(color="#F0D800FF"),
            #                 style.text(weight="bold")
            #             ],
            #             locations=loc.body(rows=[9])
            #    )
            # .tab_style(
            #         style=[
            #                 style.fill(color="rgb(197, 196, 193)"),
            #                 style.text(weight="bold")
            #             ],
            #             locations=loc.body(rows=[5,6,9,10,11])
            # )
            
            )
        ```

        :::


        :::

        """

        return(
            
            
            pl.when((pl.col(subtype).str.contains("None|60|61|62|50|68|66|66")))
            .then(pl.lit("Influenza not detected"))

            .when((pl.col(subtype).str.contains("Influenza A\\(2009 H1N1\\) virus detected by RT-PCR"))) 
            .then(pl.lit("Influenza A (09 Pdm H1N1) detected"))

            .when((pl.col(subtype).str.contains("Influenza A\\(H3\\) virus detected by RT-PCR"))) 
            .then(pl.lit("Influenza A (H3) detected"))
            
            .when((pl.col(subtype).str.contains("Influenza A\\(H5\\) virus detected by RT-PCR"))) 
            .then(pl.lit("Influenza A (H5) detected"))

            .when((pl.col(subtype).str.contains("Influenza A\\(H7\\) virus detected by RT-PCR"))) 
            .then(pl.lit("Influenza A (H7) detected"))

            .when((pl.col(subtype).str.contains("Influenza A virus detected by RT-PCR"))) 
            .then(pl.lit("Influenza A detected"))
            
            .when((pl.col(subtype).str.contains("Influenza B/Victoria lineage detected by RT-PCR"))) 
            .then(pl.lit("Influenza B (Victoria) detected"))

            .when((pl.col(subtype).str.contains("Influenza B/Yamagata lineage detected by RT-PCR"))) 
            .then(pl.lit("Influenza B (Yamagata) detected"))
            
            .when((pl.col(subtype).str.contains("Influenza B virus detected by RT-PCR"))) 
            .then(pl.lit("Influenza B detected"))

            .otherwise(pl.col(subtype).str.strip_chars())

        )

def wdrs_result_summary(wdrs_res_col: str):
    """ wdrs result summary

    Usage
    -----
    to be used in a .with_columns() statement

    Parameters
    ----------
    wdrs_res_col: str
        wdrs result column

    Examples
    --------
    ```{python}
    #| echo: false
    {{< include "../_setup.qmd" >}}
    ```
    ```{python}
    import wadoh_subtyping.transform as tf
    from wadoh_subtyping.utils import helpers
    import polars as pl

    df = pl.DataFrame({
        "TestResult": [
            "Not Detected {52}",
            "Detected {51}",
            "Test {53}"
        ]
    })
    
    result = (
        df
        .with_columns(
            WDRS_RESULT_SUMMARY=tf.wdrs_result_summary(wdrs_res_col='TestResult')
        )
    )

    helpers.gt_style(df_inp=result)

    ```

    """
    return(
        pl.when((pl.col(wdrs_res_col).str.contains("51")))
        .then(pl.lit("G_POSITIVE"))

        .when((pl.col(wdrs_res_col).str.contains("52")))
        .then(pl.lit('G_NEGATIVE'))

        .otherwise(None)
    )

def transform(df: pl.DataFrame):
    """ Transform

    Usage
    -----
    This function calls all the transformation functions at once. To be used against a pl.DataFrame

    Examples
    --------
    
    ```python
    import wadoh_subtyping.transform as tf

    df = tf.transform(df=phl_df)
    ```

    """
    # config for the column names
    wdrs_res_col="TestResult"
    subtype="ResultTextConclusion"
    wdrs_test_perf_col="ResultTextConclusion"
    wdrs_spec_source_col="SpecimenSource"
    wdrs_spec_type_col="SpecimenSource"
    spec_type_col="SpecimenSource"
    collection_date_col="SpecimenDateCollected"
    date_received_col="SpecimenDateReceived"
    submitter="SubmitterName"
    spec_source_col="SpecimenSource"

    transformed_df = (
        df
        .with_columns(
            WDRS_RESULT = wdrs_result(subtype=subtype),
            WDRS_TEST_PERFORMED = wdrs_test_performed(wdrs_test_perf_col),
            WDRS_SPECIMEN_SOURCE = wdrs_specimen_source(wdrs_spec_source_col),
            SPECIMEN_SOURCE_SITE = specimen_source_site(spec_source_col),
            WDRS_SPECIMEN_TYPE = wdrs_specimen_type(wdrs_spec_type_col),
            SPECIMEN_TYPE = specimen_type(spec_type_col),
            SPECIMEN_COLLECTION_DTTM = collection_date(df=df,col=collection_date_col),
            SPECIMEN_RECEIVED_DTTM = collection_date(df=df,col=date_received_col),
            SUBMITTER = submitting_lab(submitter),
            WDRS_PERFORMING_ORG = performing_lab(),
            WDRS_RESULT_SUMMARY = wdrs_result_summary(wdrs_res_col),
            TEST_PERFORMED_DESC = col_test_performed_desc(wdrs_test_perf_col),
            TEST_RESULT = col_test_result(subtype=subtype),
            PERFORMING_LAB_ENTIRE_REPORT = performing_lab()
        )
        .with_columns(
            # the SUBMITTER col gets created above
            # we need the result of it to populate the OTHER column
            SUBMITTER_OTHER = submitter_other(submitting_lab='SUBMITTER',submitter=submitter),
            PatientBirthDate = helpers.date_format(df,'PatientBirthDate')
        )
    )
    
    return transformed_df

def dedup_roster(roster_inp: pl.DataFrame,reference_inp: pl.DataFrame):
    """ Dedup Roster

    Usage
    -----
    Use against a formatted roster to dedup records that already exist in WDRS

    Examples
    --------
    ```python
    import wadoh_subtyping.transform as tf

    final_roster = tf.dedup_roster(roster_inp=created_roster,reference_inp=wdrs_table)
    ```
    

    """
    df = (
        roster_inp
        .join(
            reference_inp.select(['CASE_ID','WDRS__RESULT','SPECIMEN_COLLECTION_DTTM']).rename({'WDRS__RESULT': 'WDRS__RESULT_in_wdrs','SPECIMEN_COLLECTION_DTTM': 'SPECIMEN_COLLECTION_DTTM_in_wdrs'}),
            left_on='CaseID',right_on='CASE_ID',how='left'
        )
        .with_columns(
            pl.when(
                # (pl.col('WDRS_RESULT') == 'G_FLU_A_D') &
                # (pl.col('WDRS__RESULT_in_wdrs') == 'Influenza A detected')
                (pl.col('TEST_RESULT').str.strip_chars() == pl.col('WDRS__RESULT_in_wdrs').str.strip_chars()) &
                (pl.col('SPECIMEN_COLLECTION_DTTM')==pl.col('SPECIMEN_COLLECTION_DTTM_in_wdrs'))
            )
            .then(True)
            .otherwise(False)
            .alias('qa_duplicate_tests')
        )
        .filter(pl.col('qa_duplicate_tests').not_())
    )
    
    if len(df.filter(pl.col('qa_duplicate_tests'))) > 0:
        print('Exisitng case in WDRS found, removing from roster')

    df = df.select(~cs.contains('in_wdrs','qa_')).unique()

    return df

def create_roster(matched_and_transformed_df: pl.DataFrame, respnet: pl.DataFrame):
    """ Create Roster

    Usage
    -----
    Apply to a transformed and matched pl.DataFrame

    Examples
    --------

    ```python
    import wadoh_subtyping.transformations as tf

    create_roster = tf.create_roster(matched_and_transformed_df=fuzzy_matched,respnet=wdrs_table) 
    ```
    """
    # filters
    # note, some cases only have "not detected" results. these filters will kick them out
    roster = (
        matched_and_transformed_df
        # select only positve subtypes
        .filter((pl.col('WDRS_RESULT_SUMMARY').str.contains('G_POSITIVE')) & (pl.col('WDRS_RESULT') != 'RP'))
        # .filter((pl.col('WDRS_RESULT') != 'RP'))
        # join to original respnet table to get all columns
        .join(respnet,on='CASE_ID',how='left')
        .unique()

        # now get all the columns so we can import it to WDRS
        # .join(respnet.slice(0,0),how='left',on='CASE_ID') -- this is trash. just use select to create new cols
        .with_columns(
            CaseID=pl.col('CASE_ID'),
            CODE=pl.lit('FLUHP'),
            PARTY_EXTERNAL_ID=pl.col('EXTERNAL_ID'),
            FIRST_NAME=pl.col('FIRST_NAME'),
            MIDDLE_NAME=pl.col('MIDDLE_NAME'),
            LAST_NAME=pl.col('LAST_NAME'),
            SUFFIX=None,
            DOB=pl.col('BIRTH_DATE'),
            SSN=None,
            GENDER=None,
            PHONE=None,
            STREET1=None,
            CITY=None,
            STATE=None,
            POSTAL_CODE=None,
            ContactPointCounty=None,
            REPORTING_COUNTY=None,
            RACE_AMERICAN_INDIAN_OR_ALASKA_NATIVE=None,
            RACE_ASIAN=None,
            RACE_BLACK_OR_AFRICAN_AMERICAN=None,
            RACE_NATIVE_HAWIAIIAN_OR_OTHER_PACIFIC_ISLANDER=None,
            RACE_WHITE=None,
            RACE_OTHER_RACE=None,
            RACE_OTHER_RACE_SPECIFY=None,
            RACE_DECLINED=None,
            RACE_UNKNOWN=None,
            RACE_AI_OR_AN=None,
            RACE_NH_OR_PI=None,
            RACE_EXTENDED=None,
            ETHNICITY=None,
            LANGUAGE=None,
            LANGUAGE_OTHER_SPECIFY=None,
            PERFORMING_LAB_ENTIRE_REPORT=pl.col('WDRS_PERFORMING_ORG'),
            PERFORMING_LAB_ENTIRE_REPORT_OTHER=None,
            WDRS_LAB_RACE=None,
            WDRS_LAB_RACE_SPECIFY=None,
            WDRS_LAB_ETHNICITY=None,
            ORDERING_PROVIDER=None,
            ORDERING_FACILITY=None,
            SPECIMEN_COLLECTION_DTTM=pl.col('SPECIMEN_COLLECTION_DTTM'),
            SPECIMEN_RECEIVED_DTTM=pl.col('SPECIMEN_RECEIVED_DTTM'),
            SPECIMEN_ID_ACCESSION_NUM_MANUAL=None,
            WDRS_SPECIMEN_TYPE=pl.col('WDRS_SPECIMEN_TYPE'),
            SPECIMEN_TYPE=pl.col('SPECIMEN_TYPE'),
            WDRS_SPECIMEN_SOURCE=pl.col('WDRS_SPECIMEN_SOURCE'),
            SPECIMEN_SOURCE_SITE=pl.col('SPECIMEN_SOURCE_SITE'),
            WDRS_TEST_PERFORMED=pl.col('WDRS_TEST_PERFORMED'),
            TEST_PERFORMED_DESC=pl.col('TEST_PERFORMED_DESC'),
            WDRS_RESULT=pl.col('WDRS_RESULT'),
            TEST_RESULT=pl.col('TEST_RESULT'),
            WDRS_RESULT_SUMMARY=pl.col('WDRS_RESULT_SUMMARY'),
            SUBMITTER=pl.col('SUBMITTER'),
            SUBMITTER_OTHER=pl.col('SUBMITTER_OTHER'),
            USER_LAB_REPORT_NOTE=None
            # pl.lit("Updated event via GCD RESPNET Lab Result (add lab) roster").alias('Case.Note')
        )
        .with_columns(
            pl.lit(f"Updated event via GCD RESPNET Lab Result (add lab) roster on {date.today()} from PHL subtyping").alias('Case.Note')
        )
        .select([
            'CaseID',
            'CODE',
            'PARTY_EXTERNAL_ID',
            'FIRST_NAME',
            'MIDDLE_NAME',
            'LAST_NAME',
            'SUFFIX',
            'DOB',
            'SSN',
            'GENDER',
            'PHONE',
            'STREET1',
            'CITY',
            'STATE',
            'POSTAL_CODE',
            'ContactPointCounty',
            'REPORTING_COUNTY',
            'RACE_AMERICAN_INDIAN_OR_ALASKA_NATIVE',
            'RACE_ASIAN',
            'RACE_BLACK_OR_AFRICAN_AMERICAN',
            'RACE_NATIVE_HAWIAIIAN_OR_OTHER_PACIFIC_ISLANDER',
            'RACE_WHITE',
            'RACE_OTHER_RACE',
            'RACE_OTHER_RACE_SPECIFY',
            'RACE_DECLINED',
            'RACE_UNKNOWN',
            'RACE_AI_OR_AN',
            'RACE_NH_OR_PI',
            'RACE_EXTENDED',
            'ETHNICITY',
            'LANGUAGE',
            'LANGUAGE_OTHER_SPECIFY',
            'PERFORMING_LAB_ENTIRE_REPORT',
            'PERFORMING_LAB_ENTIRE_REPORT_OTHER',
            'WDRS_LAB_RACE',
            'WDRS_LAB_RACE_SPECIFY',
            'WDRS_LAB_ETHNICITY',
            'ORDERING_PROVIDER',
            'ORDERING_FACILITY',
            'SPECIMEN_COLLECTION_DTTM',
            'SPECIMEN_RECEIVED_DTTM',
            'SPECIMEN_ID_ACCESSION_NUM_MANUAL',
            'WDRS_SPECIMEN_TYPE',
            'SPECIMEN_TYPE',
            'WDRS_SPECIMEN_SOURCE',
            'SPECIMEN_SOURCE_SITE',
            'WDRS_TEST_PERFORMED',
            'TEST_PERFORMED_DESC',
            'WDRS_RESULT',
            'TEST_RESULT',
            'WDRS_RESULT_SUMMARY',
            'SUBMITTER',
            'SUBMITTER_OTHER',
            'USER_LAB_REPORT_NOTE',
            'Case.Note'
        ])
        .unique(subset=[
            'CaseID',
            'SPECIMEN_COLLECTION_DTTM',
            'SPECIMEN_RECEIVED_DTTM',
            'WDRS_RESULT',
            'WDRS_RESULT_SUMMARY'
        ])
    )
 
    return roster
