# wadoh_subtyping
A Python package for transforming influenza subtyping data from LIMS and linking it to hospitalization case data in WDRS at Washington State DOH.

See our github page for function references and docs [https://nw-page.github.io/wadoh_raccoon/](https://wa-eip.github.io/wadoh_subtyping/)

## Install

```bash
uv add wadoh_subtyping
```
or 

```bash
uv pip install wadoh_subtyping
```

## Usage

This package transforms and matches subtyping metadata from LIMS to hospitalization case data in the Washington Disease Reporting System (WDRS). It checks for matches on demographics or a medical record number, and then joins the subtyping information to influenza hospitalization case data and uploads it into WDRS.

The transformations are specific to LIMS and WDRS column names. 

See [Reference](https://nw-page.github.io/wadoh_raccoon/reference/) for more details on fuzzy matching in our wadoh_raccoon package.
