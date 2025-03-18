import argparse
import polars as pl
import polars.selectors as cs
from datetime import datetime
from pprint import pprint
from fmu.sumo.explorer import Explorer

from webviz_4d._datainput._sumo import get_sumo_case


producer_types = {"oil": ["oil", "gas", "water"], "gas": ["gas", "water"]}
production_limit_values = {"oil": 0, "gas": 0}

prod_fluid_keys = {
    "oil": "WOPTH",
    "gas": "WGPTH",
    "water": "WWPTH",
}


def get_production_data(summary_df, interval_dates, producer_type):
    """Extract historical production volumes for all wells for a selected 4D interval
    Input data:
        summary_df:     Summary table from a selected realization
        interval_dates: 4D interval dates (t0, t1)
        producer_type:  Main fluid type(oil/gas/condensate ...)
    Outpout data:
        prod_volumes:   Dict object
    """

    prod_volumes = {}

    # Check input data
    if len(summary_df) == 0:
        print("WARNING: Summary data is empty")
        return prod_volumes

    try:
        for date_string in interval_dates:
            datetime_object = datetime.strptime(date_string, "%Y-%m-%d")

            if datetime_object > datetime.now():
                raise Exception("4D interval date maut be a historical date")
    except Exception as e:
        print("ERROR:", e)
        return prod_volumes

    if producer_type not in list(producer_types.keys()):
        print("ERROR: Unknown fluid type:", producer_type)
        return prod_volumes

    # Select the 2 rows correspondig to the 2 input dates
    prod_data = summary_df.with_columns(pl.col("DATE").cast(pl.Date).cast(pl.String))
    selected_rows = prod_data.filter(pl.col("DATE").is_in(interval_dates))

    # Select all the columns containg the main fluid type
    prod_key = prod_fluid_keys.get(producer_type)
    prod_columns = selected_rows.select(cs.starts_with(prod_key))

    volume_limit = production_limit_values.get(producer_type)

    selected_producer_fluds = producer_types.get(producer_type)
    additional_producer_types = selected_producer_fluds[1:]

    # Iterate through all main fluid type columns and extract interval volumes
    for column in prod_columns.iter_columns():
        vector_name = column.name
        ecl_name = vector_name.split(":")[-1]
        well_name = ecl_name.replace("_", "")
        fluid_volume = column[1] - column[0]

        well_volumes = []

        # Select only wells with an interval production volume above the wanted production limit
        if fluid_volume > volume_limit:
            key_volume = {producer_type: fluid_volume}
            well_volumes.append(key_volume)

            # Iterate through the additional fluid types for the selected producer type and extract interval volumes for the additional fluids
            for fluid in additional_producer_types:
                additional_column_name = prod_fluid_keys.get(fluid) + ":" + ecl_name
                additional_column = selected_rows.select(pl.col(additional_column_name))
                additional_volume = additional_column.item(
                    1, 0
                ) - additional_column.item(0, 0)

                additional_key_volume = {fluid: additional_volume}
                well_volumes.append(additional_key_volume)

            # Create a dictionary with the interval volumes for all wells and all relevant fluids
            prod_volumes[well_name] = well_volumes

    return prod_volumes


def main():
    description = "Check SUMO production data"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("sumo_name")
    args = parser.parse_args()

    sumo_exp = Explorer(env="prod")

    my_case = get_sumo_case(sumo_exp, args.sumo_name)
    print(f"{my_case.name}: {my_case.uuid}")

    # Some case info
    print(my_case.field)
    print(my_case.status)
    print(my_case.user)
    print("Loading production data from SUMO")

    iteration = [it.name for it in my_case.iterations][0]

    tables = my_case.tables.filter(
        iteration=iteration, realization=0, tagname="summary"
    )
    table = tables[0]
    ecl_table = table.to_arrow()
    summary_df = pl.from_arrow(ecl_table)

    interval_dates = ["2019-10-01", "2020-10-01"]
    producer_type = "oil"

    oil_well_interval_volumes = get_production_data(
        summary_df, interval_dates, producer_type
    )

    print()
    print("Interval:", interval_dates[1], "-", interval_dates[0])
    pprint(oil_well_interval_volumes)


if __name__ == "__main__":
    main()
