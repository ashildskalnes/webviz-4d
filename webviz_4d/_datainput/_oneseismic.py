import numpy as np
import json
import matplotlib.pyplot as plt
import requests
from requests_toolbelt.multipart.decoder import MultipartDecoder


class OneseismicClient:
    def __init__(self, host, vds, sas):
        self.host = host
        self.vds = vds
        self.sas = sas

    def get_slice(self, direction, lineno):
        response = requests.post(
            f"{self.host}/slice",
            headers={"Content-Type": "application/json"},
            data=json.dumps(
                {
                    "direction": direction,
                    "lineno": lineno,
                    "vds": self.vds,
                    "sas": self.sas,
                }
            ),
        )
        if not response.ok:
            raise RuntimeError(response.text)
        parts = MultipartDecoder.from_response(response).parts

        meta = json.loads(parts[0].content)
        shape = (meta["y"]["samples"], meta["x"]["samples"])

        return meta, np.ndarray(shape, "f4", parts[1].content)

    def get_fence(self, xs, ys, system, interpolation="nearest"):
        coordinates = list(zip(xs, ys))
        response = requests.post(
            f"{self.host}/fence",
            headers={"Content-Type": "application/json"},
            data=json.dumps(
                {
                    "coordinates": coordinates,
                    "coordinateSystem": system,
                    "vds": self.vds,
                    "sas": self.sas,
                    "interpolation": interpolation,
                }
            ),
        )
        if not response.ok:
            raise RuntimeError(response.text)
        parts = MultipartDecoder.from_response(response).parts

        meta = json.loads(parts[0].content)
        data = parts[1].content

        return np.ndarray(meta["shape"], meta["format"], data), self.get_metadata()

    def get_metadata(self):
        """Get metadata from the OpenVDS volume"""
        response = requests.post(
            f"{self.host}/metadata",
            headers={"Content-Type": "application/json"},
            data=json.dumps({"vds": self.vds, "sas": self.sas}),
        )
        if not response.ok:
            raise RuntimeError(response.text)
        return response.json()


def plotslice(ax, data, meta, title=None, percentile=None):
    extent = [
        meta["y"]["min"],
        meta["y"]["max"],
        meta["x"]["min"],
        meta["x"]["max"],
    ]

    if title:
        ax.set_title(title)
    ax.set_xlabel(f"{meta['y']['annotation']}")
    ax.set_ylabel(f"{meta['x']['annotation']}")

    im = ax.imshow(
        data.T, cmap="seismic", extent=extent, vmin=-percentile, vmax=percentile
    )
    # plt.colorbar(im, ax=ax)
