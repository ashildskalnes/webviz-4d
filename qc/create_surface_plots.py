import os
import glob
import xtgeo

def main():
    home = os.path.expanduser("~")
    path= os.path.join(home, "Downloads")

    file_types = {"rms": "irap_binary", "map": "ijxyz" }

    for file_type in file_types.keys():
        horizon_files = glob.glob(path + "/*." + file_type + "*")

        for horizon_file in horizon_files:
            print("Loading file:", horizon_file,  "...")
            surface = xtgeo.surface_from_file(horizon_file, file_types[file_type])
            plot_file = horizon_file + ".jpg"
            surface.quickplot(title=horizon_file, filename=plot_file)
            print("  - QC plot created: ",plot_file)


if __name__ == '__main__':
    main()