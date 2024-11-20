import os
import time
import argparse

from webviz_4d._providers.wellbore_provider._provider_impl_file import (
    ProviderImplFile,
)


def main():
    description = "Testing extraction of data from SMDA"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("field_name", help="Enter field name")
    args = parser.parse_args()
    print(args)

    omnia_env = ".omniaapi"
    home = os.path.expanduser("~")
    env_path = os.path.expanduser(os.path.join(home, omnia_env))

    smda_provider = ProviderImplFile(env_path, "SMDA")
    # pozo_provider = ProviderImplFile(env_path, "POZO")

    field = args.field_name.upper()
    print("Field:", field)

    # Get metadata for all drilled wells
    # print("\nGet metadata for all drilled wells")
    # start = time.time()
    # metadata = smda_provider.drilled_wellbore_metadata(
    #     field=field,
    # )
    # end = time.time()
    # print("Extract wellbore metadata:", end - start)

    # if metadata:
    #     for i in range(0, len(metadata.dataframe.purpose)):
    #         print(
    #             i,
    #             metadata.dataframe.unique_wellbore_identifier[i],
    #             metadata.dataframe.purpose[i],
    #             metadata.dataframe.status[i],
    #             metadata.dataframe.parent_wellbore[i],
    #         )

    # Get well picks for selected drilled wells
    # print("\nGet well picks for selected drilled wells ...")
    # start = time.time()
    # wellbore_picks = smda_provider.get_wellbore_picks(
    #     field_name=field,
    #     wellbore_name="NO 16/2-D-41",
    #     pick_identifiers=["Draupne Fm. Top", "Draupne Fm. Base"],
    # )
    # end = time.time()
    # print("Extract well picks:", end - start)
    # if wellbore_picks:
    #     print(wellbore_picks.dataframe)

    # Get well picks (STAT) for all drilled wells on a selected field
    # print("\nGet well picks (STAT) for all drilled wells on a selected field ...")
    # start = time.time()
    # wellbore_picks = smda_provider.get_wellbore_picks(
    #     field_name=field,
    #     pick_identifiers=["Draupne Fm. Top"],
    #     interpreter="STAT",
    # )
    # end = time.time()
    # print("Extract well picks:", end - start)
    # if wellbore_picks:
    #     print(wellbore_picks.dataframe)

    # Get planned wells for a selected field
    print("\nGet planned wells for a selected field ...")
    start = time.time()
    planned_metadata = smda_provider.planned_wellbore_metadata(
        field=field,
    )
    end = time.time()
    print("Get planned wells:", end - start)

    if planned_metadata:
        df = planned_metadata.dataframe
        for index, row in df.iterrows():
            print(
                index,
                row["unique_wellbore_identifier"],
                row["design_name"],
                row["status"],
            )

    # Get all planned wellbore trajectories for a selected field
    start = time.time()
    all_planned_trajectories = smda_provider.planned_trajectories(
        planned_metadata.dataframe,
    )
    end = time.time()
    print("Extract all planned trajectories:", end - start)

    df = all_planned_trajectories.dataframe
    wellbore_name = None

    if not df.empty:
        wellbores = df["unique_wellbore_identifier"].unique()
        print("  ", len(wellbores), "wellbore trajectories")
        wellbore_name = wellbores[0]

    # Get trajectoriy data for a selected planned wellbore
    print("\nGet trajectoriy data for a selected planned wellbore")
    start = time.time()
    wellbore_trajectory = smda_provider.planned_wellbore_trajectory(
        wellbore_name=wellbore_name,
    )
    end = time.time()
    print("Extract one trajectory:", end - start)

    if wellbore_trajectory:
        print(wellbore_name)
        for i in range(0, len(wellbore_trajectory.x_arr)):
            print(
                i,
                wellbore_trajectory.x_arr[i],
                wellbore_trajectory.y_arr[i],
                wellbore_trajectory.z_arr[i],
                wellbore_trajectory.md_arr[i],
            )

    # Get all drilled wellbore trajectories for a selected field
    print("\nGet all drilled wellbore trajectories for a selected field")
    start = time.time()
    all_trajectories = smda_provider.drilled_trajectories(
        field_name=field,
    )
    end = time.time()
    print("Extract all trajectories:", end - start)

    df = all_trajectories.dataframe
    wellbore_name = None

    if not df.empty:
        wellbores = df["unique_wellbore_identifier"].unique()
        print("  ", len(wellbores), "wellbore trajectories")
        wellbore_name = wellbores[0]

    # Get trajectoriy data for a selected wellbore
    print("\nGet trajectoriy data for a selected wellbore")
    start = time.time()
    wellbore_trajectory = smda_provider.drilled_wellbore_trajectory(
        wellbore_name=wellbore_name,
        md_min=1000,
        md_max=2000,
    )
    end = time.time()
    print("Extract one trajectory:", end - start)

    if wellbore_trajectory:
        print(wellbore_name, wellbore_trajectory.coordinate_system)
        for i in range(0, len(wellbore_trajectory.x_arr)):
            print(
                i,
                wellbore_trajectory.x_arr[i],
                wellbore_trajectory.y_arr[i],
                wellbore_trajectory.z_arr[i],
                wellbore_trajectory.md_arr[i],
            )


if __name__ == "__main__":
    main()
