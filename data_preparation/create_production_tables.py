import os
import pandas as pd
import argparse
from webviz_4d._datainput import common


def write_data(prod_df, fluid, scale, file_object):
    """ Wite production data to file (fluid) """
    columns = list(prod_df)

    for _index, row in prod_df.iterrows():
        well_name = row["PDM well name"]
        well_name = well_name.replace(" ", "")

        for column in columns:
            if "-" in column:
                interval = column[0:7] + column[10:18]
                value = row[column] / scale
                print(well_name, interval, value)
                file_object.write(well_name + "," + interval + ",")
                file_object.write("%d" % value)
                file_object.write("," + fluid + "\n")


# Main program
def main():
    """ Create production data tables """
    description = "Create production data tables"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "config_file", help="Enter path to the WebViz-4D configuration file"
    )

    args = parser.parse_args()
    print(description)
    print(args)

    config_file = args.config_file
    config_file = common.read_config(config_file)

    production_directory = common.get_config_item(config_file, "production_data")
    production_directory = common.get_full_path(production_directory)

    production_table_file = os.path.join(
        production_directory, "production_fluid_table.csv"
    )
    injection_table_file = os.path.join(
        production_directory, "injection_fluid_table.csv"
    )

    bore_oil_file = os.path.join(production_directory, "BORE_OIL_VOL.csv")
    bore_gas_file = os.path.join(production_directory, "BORE_GAS_VOL.csv")
    bore_water_file = os.path.join(production_directory, "BORE_WAT_VOL.csv")

    print("Loading oil volumes from file", bore_oil_file)
    bore_oil = pd.read_csv(bore_oil_file)

    print("Loading gas volumes from file", bore_gas_file)
    bore_gas = pd.read_csv(bore_gas_file)

    print("Loading water volumes from file", bore_water_file)
    bore_water = pd.read_csv(bore_water_file)

    with open(production_table_file, "w") as file_object:
        file_object.write("Well_name,4D_interval,Volumes,Fluid\n")
        fluid = "Oil_[Sm3]"
        write_data(bore_oil, fluid, 1, file_object)

        fluid = "Gas_[kSm3]"
        write_data(bore_gas, fluid, 1000, file_object)

        fluid = "Water_[Sm3]"
        write_data(bore_water, fluid, 1, file_object)

    print("Production volumes table stored to file", production_table_file)

    inject_gas_file = os.path.join(production_directory, "BORE_GI_VOL.csv")
    inject_water_file = os.path.join(production_directory, "BORE_WI_VOL.csv")

    print("Loading injected gas volumes from file", inject_gas_file)
    inject_gas = pd.read_csv(inject_gas_file)

    print("Loading injected water volumes from file", inject_water_file)
    inject_water = pd.read_csv(inject_water_file)

    with open(injection_table_file, "w") as file_object:
        file_object.write("Well_name,4D_interval,Volumes,Fluid\n")
        fluid = "Injected_Gas_[kSm3]"
        write_data(inject_gas, fluid, 1000, file_object)

        fluid = "Injected_Water_[Sm3]"
        write_data(inject_water, fluid, 1, file_object)

    print("Injection volumes table stored to file", injection_table_file)


if __name__ == "__main__":
    main()
