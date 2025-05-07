import segyio
import numpy as np
from shutil import copyfile


def main():
    input_file = (
        "/scratch/auto4d/uservolumes/ashska/EQ24205DZC24B-KPSDM-RAW-FULL-0535-TIME.sgy"
    )
    output_file = (
        "/scratch/auto4d/uservolumes/ashska/EQ19231DZC23A-KPSDM-NOMASK-TIME.sgy"
    )

    copyfile(input_file, output_file)

    one = np.empty(376)
    one[:] = 1

    with segyio.open(output_file, "r+") as src:

        # set all non-zero sample values to 1
        for i in range(0, src.tracecount - 1):
            trace = src.trace[i]

            if sum(trace) != 0:
                src.trace[i] = one
            else:
                src.trace[i] = src.trace[i]
            # print(i, src.trace[i][0])


if __name__ == "__main__":
    main()
