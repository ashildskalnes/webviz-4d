import numpy as np
import xtgeo
import matplotlib.pyplot as plt


def load_cube_data(cube_path: str) -> xtgeo.Cube:
    return xtgeo.cube_from_file(cube_path)


def get_xline(cube: xtgeo.Cube, xline: int) -> np.ndarray:
    idx = np.where(cube.xlines == xline)
    return cube.values[:, idx, :][:, 0, 0].T


def get_iline(cube: xtgeo.Cube, iline: int) -> np.ndarray:
    idx = np.where(cube.ilines == iline)
    return cube.values[idx, :, :][0, 0, :].T


def get_zslice(cube: xtgeo.Cube, zslice: float) -> np.ndarray:
    idx = np.where(cube.zslices == zslice)
    return cube.values[:, :, idx][:, :, 0, 0].T


def plot_zslice(ax, data, ilines, xlines, title=None, percentile=None):
    extent = [
        min(ilines),
        max(ilines),
        min(xlines),
        max(xlines),
    ]

    if title:
        ax.set_title(title)
    ax.set_xlabel("Inline")
    ax.set_ylabel("Crossline")

    im = ax.imshow(
        data, cmap="seismic", extent=extent, vmin=-percentile, vmax=percentile
    )
    # plt.colorbar(im, ax=ax)
