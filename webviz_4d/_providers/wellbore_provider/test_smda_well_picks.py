import os
import pandas as pd
import argparse

from webviz_4d._providers.wellbore_provider._provider_impl_file import (
    ProviderImplFile,
)


def main():
    description = "Check wellpicks in SMDA"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("field_name", help="Enter field name")
    args = parser.parse_args()
    print(args)

    field = args.field_name.upper()
    omnia_env = ".omniaapi"
    home = os.path.expanduser("~")
    env_path = os.path.expanduser(os.path.join(home, omnia_env))

    smda_provider = ProviderImplFile(env_path, "SMDA")

    # Get wellbore uuids and names for all smda (drilled) wellbores in a selected field
    all_wellbores = smda_provider.get_smda_wellbores(field, None)
    smda_wellbores = all_wellbores[all_wellbores["completion_date"].isnull() == False]

    # Get all (STAT) wellbore picks in a selected field
    wellbore_picks = smda_provider.get_wellbore_picks(
        field_name=field, interpreter="STAT"
    )

    wellbore_picks_df = wellbore_picks.dataframe.sort_values(
        by=["unique_wellbore_identifier", "md"]
    )

    # Get all different pick types (but remove the None option)
    wellbore_pick_types = wellbore_picks_df.pick_type.unique()
    pick_types = list(filter(lambda item: item is not None, wellbore_pick_types))

    status = True

    while status:
        print("")
        print("Possible pick types are:")
        print(pick_types)
        pick_type = input("Enter wanted type:\n")

        if not pick_type in pick_types:
            print("ERROR:", pick_type, "is not a valid type")
            exit()

        selected_picks = wellbore_picks_df[wellbore_picks_df["pick_type"] == pick_type]
        wellbore_pick_names = selected_picks.pick_identifier.unique()

        print("")
        print("Possible selections are:")
        print(sorted(wellbore_pick_names))
        pick_name = input("Select pick name:\n")

        if not pick_name in wellbore_pick_names:
            print("ERROR:", pick_name, "is not a valid pick name")
            exit()

        selected_pick_names = [pick_name]

        for wellbore_pick in selected_pick_names:
            missing_picks = []

            for _index, row in smda_wellbores.iterrows():
                wellbore_name = row["unique_wellbore_identifier"]

                try:
                    selected_pick = wellbore_picks_df[
                        (
                            wellbore_picks_df["unique_wellbore_identifier"]
                            == wellbore_name
                        )
                        & (wellbore_picks_df["pick_identifier"] == wellbore_pick)
                        & (wellbore_picks_df["obs_no"] == 1)
                    ]
                    md_value = selected_pick["md"].to_list()[0]
                except:
                    missing_picks.append(wellbore_name)

        if len(missing_picks) > 0:
            print("Wellbores without picks for", pick_name)

            for name in missing_picks:
                print("  ", name)
        else:
            print("No wellbores without picks for", pick_name)

        valg = input("Continue (Y/N)\n")

        if valg.upper() != "Y":
            exit()


if __name__ == "__main__":
    main()
