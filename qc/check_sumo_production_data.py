import os
import numpy as np
import pandas as pd
import argparse
import polars as pl
import polars.selectors as cs
from datetime import datetime
from pprint import pprint
from dataclasses import dataclass
from fmu.sumo.explorer import Explorer

from webviz_4d._datainput._sumo import get_sumo_case
from webviz_4d._providers.wellbore_provider._provider_impl_file import ProviderImplFile
from webviz_4d._providers.wellbore_provider._smda import extract_data
from webviz_4d._datainput.well import load_smda_metadata, load_pdm_info


@dataclass
class WellProductionData:
    eclipse_well_name: str
    well_uwi: str
    well_uuid: str
    mlt_name: str
    oil_production_volume: float
    gas_production_volume: float
    condensate_production_volume: float
    water_production_volume: float
    water_injection_volume: float
    gas_injection_volume: float
    co2_injection_volume: float


prod_fluid_keys = {
    "oil_production_volume": "WOPTH",
    "gas_production_volume": "WGPTH",
    "water_production_volume": "WWPTH",
    "gas_injection_volume": "WIPTH",
    "water_injection_volume": "WIPTH",
}

undefined_well_names = []


wrong_short_names = {"2B3H": "B3H"}


def get_short_wellname(wellname):
    """Well name on a short name form where blockname and spaces are removed.

    This should cope with both North Sea style and Haltenbanken style.
    E.g.: '31/2-G-5 AH' -> 'G-5AH', '6472_11-F-23_AH_T2' -> 'F-23AHT2'
    Stolen from Xtgeo
    """
    newname = []
    first1 = False
    first2 = False
    for letter in wellname:
        if first1 and first2:
            newname.append(letter)
            continue
        if letter in ("_", "/"):
            first1 = True
            continue
        if first1 and letter == "-":
            first2 = True
            continue

    xname = "".join(newname)
    xname = xname.replace("_", "")
    return xname.replace(" ", "")


def get_volume(selected_rows, ecl_name, volume_type):
    volume = np.nan
    try:
        column_name = prod_fluid_keys.get(volume_type) + ":" + ecl_name
        column = selected_rows[column_name]
        volume = column[1] - column[0]
    except:
        # print("WARNING: No volumes found:", ecl_name, volume_type)
        pass

    return volume


def get_smda_wells_info(field_name):
    omnia_env = ".omniaapi"
    home = os.path.expanduser("~")
    env_path = os.path.expanduser(os.path.join(home, omnia_env))
    smda_provider = ProviderImplFile(env_path, "SMDA")

    drilled_wells_info = load_smda_metadata(smda_provider, field_name)

    well_short_names = []

    for index, row in drilled_wells_info.iterrows():
        well_name = row["unique_wellbore_identifier"]
        short_name = get_short_wellname(well_name).replace("-", "")
        well_short_names.append(short_name)

    extended_wells_info = drilled_wells_info.copy()
    extended_wells_info["well_short_name"] = well_short_names
    extended_wells_info.sort_values(by=["well_short_name"], inplace=True)

    # print()
    # print(extended_wells_info)

    return extended_wells_info


def get_wellbore_aliases(token, api, wellbores):
    wellbore_aliases = []
    endpoint = api + "wellbore-alias?"

    filter = "&wellbore_name_set=multilateral%20well%20name"
    mlt_aliases_df = extract_data(token, endpoint, filter)

    mlt_aliases_list = mlt_aliases_df["unique_wellbore_identifier"].to_list()
    wellbore_names = wellbores["unique_wellbore_identifier"].to_list()

    for wellbore in wellbore_names:
        mlt_alias = ""

        if wellbore in mlt_aliases_list:
            mlt_alias = mlt_aliases_df[
                mlt_aliases_df["unique_wellbore_identifier"] == wellbore
            ]
            alias = mlt_alias["alias"].values[0]

        wellbore_aliases.append(alias)

    return wellbore_aliases


def get_smda_name(wells_info, ecl_name):
    well_uwi = ""
    well_uuid = ""
    mlt_name = ""

    status = False

    truncated_ecl_name = ecl_name.replace("_", "").replace("-", "")

    try:
        selected_well = wells_info[wells_info["well_short_name"] == truncated_ecl_name]
        well_uwi = selected_well["unique_wellbore_identifier"].values[0]
        well_uuid = selected_well["uuid"].values[0]
        mlt_name = selected_well["mlt_name"].values[0]
        status = True
    except:
        pass

    if not status:
        try:
            correct_short_name = wrong_short_names.get(ecl_name)

            if correct_short_name:
                selected_well = wells_info[
                    wells_info["well_short_name"] == correct_short_name
                ]
                well_uwi = selected_well["unique_wellbore_identifier"].values[0]
                well_uuid = selected_well["uuid"].values[0]
                mlt_name = selected_well["mlt_name"].values[0]
                status = True
        except:
            pass
    if not status:
        try:
            mlt_wells = wells_info.dropna()
            for indx, row in mlt_wells.iterrows():
                mlt_short_wellname = row["mlt_short_name"].replace("-", "")

                if mlt_short_wellname in truncated_ecl_name:
                    well_uwi = row["unique_wellbore_identifier"]
                    well_uuid = row["uuid"]
                    mlt_name = row["mlt_name"]
                    status = row["status"]

                    if status != "plugged":
                        break
        except:
            pass

    return well_uwi, well_uuid, mlt_name


def get_sumo_prod_data(summary_df, drilled_wells_info, interval_dates):
    """Extract historical production volumes for all wells for a selected 4D interval
    Input data:
        summary_df:     Summary table from a selected realization
        interval_dates: 4D interval dates (t0, t1)
    Outpout data:
        prod_volumes:   List containing WellProductionData
    """

    well_volumes = []

    # Check input data
    if len(summary_df) == 0:
        print("WARNING: Summary data is empty")
        return well_volumes

    try:
        for date_string in interval_dates:
            datetime_object = datetime.strptime(date_string, "%Y-%m-%d")

            if datetime_object > datetime.now():
                raise Exception("4D interval date must be a historical date")
    except Exception as e:
        print("ERROR:", e)
        return well_volumes

    # Select the 2 rows correspondig to the 2 input dates
    prod_data = summary_df.with_columns(pl.col("DATE").cast(pl.Date).cast(pl.String))
    selected_rows = prod_data.filter(pl.col("DATE").is_in(interval_dates))

    if len(selected_rows) < 2:
        print("ERROR: Selected interval is not correct")
        return well_volumes

    # Create a list of eclipse well names with production-/injection-volumes
    ecl_names = []

    for key, value in prod_fluid_keys.items():
        prod_inj_columns = selected_rows.select(cs.starts_with(value))
        for column in prod_inj_columns.iter_columns():
            vector_name = column.name
            ecl_name = vector_name.split(":")[-1]
            ecl_names.append(ecl_name)

    unique_ecl_names = sorted(list(set(ecl_names)))
    print()
    print("Number of eclipse production-/injection wells:", len(unique_ecl_names))
    print()

    for ecl_name in unique_ecl_names:
        status = False
        well_uwi, well_uuid, mlt_name = get_smda_name(drilled_wells_info, ecl_name)

        if well_uwi == "":
            undefined_well_names.append(ecl_name)

        oil_production_volume = get_volume(
            selected_rows, ecl_name, "oil_production_volume"
        )
        if oil_production_volume > 0:
            status = True

        gas_production_volume = get_volume(
            selected_rows, ecl_name, "gas_production_volume"
        )
        if gas_production_volume > 0:
            status = True

        water_production_volume = get_volume(
            selected_rows, ecl_name, "water_production_volume"
        )
        if water_production_volume > 0:
            status = True

        gas_injection_volume = get_volume(
            selected_rows, ecl_name, "gas_injection_volume"
        )
        if gas_injection_volume > 0:
            status = True

        water_injection_volume = get_volume(
            selected_rows, ecl_name, "water_injection_volume"
        )
        if gas_injection_volume > 0:
            status = True

        if status:
            well_prod_data = WellProductionData(
                eclipse_well_name=ecl_name,
                well_uwi=well_uwi,
                well_uuid=well_uuid,
                mlt_name=mlt_name,
                oil_production_volume=oil_production_volume,
                gas_production_volume=gas_production_volume,
                condensate_production_volume=np.nan,
                water_production_volume=water_production_volume,
                water_injection_volume=water_injection_volume,
                gas_injection_volume=gas_injection_volume,
                co2_injection_volume=np.nan,
            )

            well_volumes.append(well_prod_data)

    return well_volumes


def main():
    description = "Extract SUMO production data"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("sumo_name")
    parser.add_argument("time0")
    parser.add_argument("time1")
    args = parser.parse_args()

    sumo_exp = Explorer(env="prod")

    my_case = get_sumo_case(sumo_exp, args.sumo_name)
    print(f"{my_case.name}: {my_case.uuid}")

    # Some case info
    print(my_case.field)
    print(my_case.status)
    print(my_case.user)
    print("Loading production data from SUMO ...")

    iteration = [it.name for it in my_case.iterations][0]

    tables = my_case.tables.filter(
        iteration=iteration, realization=0, tagname="summary"
    )
    table = tables[0]
    ecl_table = table.to_arrow()
    summary_df = pl.from_arrow(ecl_table)

    drilled_wells_info = get_smda_wells_info(my_case.field)
    print(drilled_wells_info)

    omnia_env = ".omniaapi"
    home = os.path.expanduser("~")
    env_path = os.path.expanduser(os.path.join(home, omnia_env))
    pdm_provider = ProviderImplFile(env_path, "PDM")
    # pdm_wells_info = load_pdm_info(pdm_provider, my_case.field)
    # print(pdm_wells_info)

    interval_dates = [args.time0, args.time1]

    well_volumes = get_sumo_prod_data(summary_df, drilled_wells_info, interval_dates)
    branch_volumes = []

    # Find all possible well branches
    for production_data in well_volumes:
        wellbore_name = production_data.well_uwi
        mlt_name = production_data.mlt_name

        possible_branches = drilled_wells_info[
            drilled_wells_info["mlt_name"] == mlt_name
        ]

        if not possible_branches.empty:
            for indx, row in possible_branches.iterrows():
                uwi = row["unique_wellbore_identifier"]

                if uwi != wellbore_name:
                    branch_prod_data = WellProductionData(
                        eclipse_well_name=production_data.eclipse_well_name,
                        well_uwi=row["unique_wellbore_identifier"],
                        well_uuid=row["uuid"],
                        mlt_name=production_data.mlt_name,
                        oil_production_volume=production_data.oil_production_volume,
                        gas_production_volume=production_data.gas_production_volume,
                        condensate_production_volume=production_data.condensate_production_volume,
                        water_production_volume=production_data.water_production_volume,
                        water_injection_volume=production_data.water_injection_volume,
                        gas_injection_volume=production_data.gas_injection_volume,
                        co2_injection_volume=production_data.co2_injection_volume,
                    )
                    branch_volumes.append(branch_prod_data)

    print()
    print("Interval:", interval_dates[1], "-", interval_dates[0])
    pprint(well_volumes)
    print()

    pprint(branch_volumes)
    print()

    print(
        "Selected interval:",
        interval_dates,
        ":",
        len(well_volumes),
        "wells",
    )
    print()

    # Production overview
    wellbore_names = []
    mlt_names = []
    eclipse_names = []
    oil_volumes = []
    gas_volumes = []
    water_volumes = []

    for well in well_volumes:
        wellbore_names.append(well.well_uwi)
        eclipse_names.append(well.eclipse_well_name)
        mlt_names.append(well.mlt_name)
        oil_volumes.append(well.oil_production_volume)
        gas_volumes.append(well.gas_production_volume)
        water_volumes.append(well.water_production_volume)

    for well in branch_volumes:
        wellbore_names.append(well.well_uwi)
        eclipse_names.append(well.eclipse_well_name)
        mlt_names.append(well.mlt_name)
        oil_volumes.append(well.oil_production_volume)
        gas_volumes.append(well.gas_production_volume)
        water_volumes.append(well.water_production_volume)

    production_overview = pd.DataFrame(
        list(
            zip(
                wellbore_names,
                eclipse_names,
                mlt_names,
                oil_volumes,
                gas_volumes,
                water_volumes,
            )
        ),
        columns=["UWI", "Eclipse_name", "MLT name", "Oil", "Gas", "Water"],
    )
    pd.set_option("display.max_rows", None)
    production_overview.sort_values(by="UWI", inplace=True)
    print(production_overview)
    production_overview.to_csv(my_case.name + ".csv")

    print()
    print("Unrecognized eclipse wells:", len(undefined_well_names))
    print(undefined_well_names)


if __name__ == "__main__":
    main()
