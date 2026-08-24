# wadoh_subtyping
A Python package for transforming and processing RESPNET subtyping data

See our github page for function references and docs https://nw-page.github.io/wadoh_raccoon/

## Install

```bash
uv add wadoh_subtyping
```
or 

```bash
uv pip install wadoh_subtyping
```

## Usage

This package transforms and matches data from LIMS to data in the Washington Disease Reporting System (WDRS). It checks for matches on demographics or a medical record number, and then joins the subtyping information in LIMS to the hospital data in WDRS. 

The transformations are specific to LIMS and WDRS column names. 

See [Reference](https://nw-page.github.io/wadoh_raccoon/reference/) for more details on fuzzy matching in our wadoh_raccoon package.
