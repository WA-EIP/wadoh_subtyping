from wadoh_subtyping import extract, load, matching, wrangle, rematch_funcs
from wadoh_subtyping import helpers


def main(write_data=False):

    # run data pulls
    pull_res = extract.run_pulls()

    # run transformations
    w_res = wrangle.wrangle_phl(pull_res)

    # run matching
    res = matching.match_phl(pull_res,w_res)

    # run the rematch process
    rematch = rematch_funcs.rematch(pull_res=res.pull_res)

    # pull records that have been reviewed and completed review
    reviewed_roster, manual_reviewed, manual, folder = extract.pull_manual_reviewed_files(pull_res)

    # combined rosters from rematch, regular, and manually reviewed
    final_roster = wrangle.combine_the_rosters(rematch,res, reviewed_roster)
    
    if write_data:

        load.combine_and_write(rematch,res, final_roster, manual_reviewed, manual, folder)

        if len(final_roster) > 0: 
            # now match and fill in subtypes into redcap
            final_json, final, project = helpers.fill_redcap_subtypes(roster_inp=final_roster,pull_res=res.pull_res)

            # import to redcap
            project.import_records(
                to_import=final_json,
                import_format='json'
            )

# run the script and write the data
# use main() if you want to run the script without writing any data
if __name__ == "__main__":
    main(write_data=True)
