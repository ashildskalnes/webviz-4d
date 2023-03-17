import numpy as np
import pandas as pd
from scipy.interpolate import interp1d

import xtgeo
import xtgeo.cxtgeo._cxtgeo as _cxtgeo

import matplotlib.pyplot as plt


def create_fence_dataframe(fence, start_md):
    x_fence = []
    y_fence = []
    md_fence = []
    tvd_fence = []
    dist_fence = []

    i = 0
    for item in fence:
        x_val = item[0]
        y_val = item[1]
        tvd_val = item[2]
        hor_dist = item[3]

        if i == 0:
            md_val = start_md
        else:
            a = np.array((x_val, y_val, tvd_val))
            b = np.array((x_fence[-1], y_fence[-1], tvd_fence[-1]))
            md_val = np.linalg.norm(a - b) + md_fence[-1]  # Calculate MD along fence

        x_fence.append(x_val)
        y_fence.append(y_val)
        md_fence.append(md_val)
        tvd_fence.append(tvd_val)
        dist_fence.append(hor_dist)
        i = i + 1

    fence_df = pd.DataFrame()
    fence_df["x"] = x_fence
    fence_df["y"] = y_fence
    fence_df["md"] = md_fence
    fence_df["tvd"] = tvd_fence
    fence_df["hor_dist"] = dist_fence

    return fence_df


data_dir = "/private/ashska/development/my_forks/fields/johan_sverdrup/"
wellbore_file = data_dir + "well_data/16_2-G-3_H.w"
surface_file = (
    data_dir
    + "default_config/fmu_data/draupne_fm_1_top--depth_structural_model--mean.gri"
)

pd.set_option("display.max_rows", None)

wellbore = xtgeo.well_from_file(wellbore_file, mdlogname="MD")
print("Wellbore trajectory")
print(wellbore.dataframe[["MD", "Z_TVDSS"]])
x_well = wellbore.dataframe["X_UTME"].values
y_well = wellbore.dataframe["Y_UTMN"].values
md_well = wellbore.dataframe["MD"].values
tvd_well = wellbore.dataframe["Z_TVDSS"].values

surface = xtgeo.surface_from_file(surface_file)

# Extract first well crossings with the surface
surface_picks = wellbore.get_surface_picks(surface)
surf_pick_md = surface_picks.dataframe["MD"].values[0]
surf_pick_tvd = surface_picks.dataframe["Z_TVDSS"].values[0]
print("")
print("Surface pick md, tvd:", surf_pick_md, surf_pick_tvd)

# Find md value in trajectory closest to the selectd tvd_value
tvd_value = 0
ind = min(range(len(tvd_well)), key=lambda i: abs(tvd_well[i] - tvd_value))

selected_row = wellbore.dataframe.iloc[ind]
start_md = selected_row["MD"]

if ind == 0:
    tvdmin = 0
else:
    tvdmin = selected_row["Z_TVDSS"]

# Extract fence
nextend = 0
sampling = 20
fence = wellbore.get_fence_polyline(sampling=sampling, nextend=nextend, tvdmin=tvdmin)
surface_line = surface.get_randomline(fence, hincrement=sampling)

x_surface = []
y_surface = []

for item in surface_line:
    x_surface.append(item[0])
    y_surface.append(item[1])

fence_df = create_fence_dataframe(fence, start_md)
x_fence = fence_df["x"].values
y_fence = fence_df["y"].values
md_fence = fence_df["md"].values
tvd_fence = fence_df["tvd"].values
dist_fence = fence_df["hor_dist"].values

print("")
print("Fence")
print(fence_df)

# Create horizontal interpolator (from md to horizontal distance)
dist_interp = interp1d(md_fence[nextend:], dist_fence[nextend:])

if not np.isnan(surf_pick_md):
    try:
        surf_dist_est = dist_interp(surf_pick_md) - dist_fence[0]
    except ValueError as error:
        print(error)
        surf_dist_est = None

fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(20, 10))
fig.suptitle(wellbore.name + " tvdmin= " + str(tvdmin))
ax1.plot(x_well, y_well, color="black", marker="*", label="trajectory")
ax1.plot(x_fence, y_fence, color="red", marker="o", label="fence")
ax1.legend()
ax1.grid(True)
ax1.set_title("Map view")
ax1.set_xlabel("X_UTM")
ax1.set_ylabel("Y_UTM")

ax2.plot(md_well, tvd_well, color="black", marker="*", label="trajectory")
ax2.plot(md_fence, tvd_fence, color="red", marker="o", label="fence")
ax2.legend()
ax2.grid(True)
ax2.set_title("TVD versus MD")
ax2.set_xlabel("MD [m]")
ax2.set_ylabel("TVD [m]")
ax2.set_ylim([tvd_fence[-1], tvd_fence[0]])

ax3.plot(
    dist_fence[nextend:],
    tvd_fence[nextend:],
    color="black",
    marker="*",
    label="Well trajectory",
)
ax3.plot(x_surface[nextend:], y_surface[nextend:], label="Surface")

if surf_dist_est is not None:
    ax3.plot(
        surf_dist_est,
        surf_pick_tvd,
        "*",
        label="Surface pick",
    )

ax3.grid(True)
ax3.set_title("Fence")
ax3.set_xlabel("Horizontal distance [m] ")
ax3.set_ylabel("TVD [m]")
ax3.set_ylim([tvd_fence[-1], tvd_fence[0]])
ax3.legend()
plt.show()
