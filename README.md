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

### How to run it

There is a script called `main.py` in the root of this repo. It acts as a trigger script that pulls in all the functions and executes them. The package uses [uv](https://docs.astral.sh/uv/) as a package manager. To run the process, run:

```bash
uv run main.py
```

### Inputs and Outputs

The main inputs are LIMS data pulls (a dataset containing subtyping information) and WDRS data pulls (a dataset containing influzena case data). The outputs are:

- `roster.csv` the case data from WDRS joined with the influenza subtype data in LIMS. This file is uploaded into WDRS.

It also outputs many internal tables for keeping track of all the data.

- `fuzzy_matched` all the records that matched on fuzzy matching
- `fuzzy_unmatched` all the records that did not match fuzzy matching. These are manually reviewed to see if there is a match that fuzzy matching didn't catch.
- `received_submissions` all data snapshotted for that run with a date stamp and identifier saved
- `id_matched` all the records that were matched based on medical record number
- `qa_tbl` all the records that had qa issues

uv will install the packages and execute the code.

### Package structure

`main.py` is a trigger script that runs all the code. It is a small script that doesn't do any transformations or matching, it just runs the code. 

`src/` is a folder that contains all the source code. In source you will find `.py` scripts that contain all of the functions:

- `extract.py` runs data pulls. 
- `transform.py` contains all the column transformations we do. Each column has one transformation.
- `wrangle.py` runs the transformations on datasets and joins datasets together based on either fuzzy matching or identifier matching
- `load.py` writes the output tables and roster csv
- `qa.py` scans for qa errors
- `helpers.py` contains utility functions like aligning columns for joins or getting azure secrets for data pulls
- `rematch_funcs.py` contains functions for the rematch process. The pipeline will try to rematch any data that previously failed to find a match

`tests/` folder contains tests for all the transformation functions to make sure that nothing breaks when there is an update to the code base.

