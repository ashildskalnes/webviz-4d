import math
import numpy as np


# From Jon Magne Aagaard (AspenTech)
# Important:
# If the resqml frame was left handed you need to shuffle any data around accordingly:
# if not right_handed:
#   a = np.reshape(a,(nj,ni))  # swap dims
#   a = np.flip(a,0)

# Dump from what I get from the open-etp-rest-api service (as part of a python dict)
# As you see I'm using the Point3dLatticeArray object here.
# the arguments iinfo and jinfo is the two dictionaries in the 'Offset' list.
# The originx and originy is just 'Coordinate1' and 'Coordinate2'.
# As one of you pointed out we have a thirds coordinate/dimension which we don't use here.

# 'Points': {'SupportingGeometry': {'Origin': {'Coordinate1': -10194.87109375,
#              'Coordinate2': 6634.9404296875,
#              'Coordinate3': 0,
#              '$type': 'resqml20.Point3d'},
#   'Offset': [{'Offset': {'Coordinate1': 0.00017903980381225274,
#                          'Coordinate2': -0.9999999839723742,
#                          'Coordinate3': 0,
#                          '$type': 'resqml20.Point3d'},
#               'Spacing': {'Value': 82.12829721475912,
#                           'Count': 89,
#                           '$type': 'resqml20.DoubleConstantArray'},
#               '$type': 'resqml20.Point3dOffset'},
#              {'Offset': {'Coordinate1': 0.9999999839723729,
#                          'Coordinate2': 0.00017903981083079037,
#                          'Coordinate3': 0,
#                          '$type': 'resqml20.Point3d'},
#               'Spacing': {'Value': 83.24235421976209,
#                           'Count': 125,
#                           '$type': 'resqml20.DoubleConstantArray'},
#               '$type': 'resqml20.Point3dOffset'}],
#   '$type': 'resqml20.Point3dLatticeArray'},
# }


def get_angle(self, dx, dy):
    # get angle to a vector. Returns an angle in [-180, +180]
    # From Jon Magne Aagaard (AspenTech)
    if dx == 0.0 and dy == 0.0:  # NOSONAR This is perfectly fine here
        return 0.0

    r = math.sqrt(dx * dx + dy * dy)
    if dx > 0:  # 1st or 4th quadrant
        return math.degrees(math.asin(dy / r))
    if dy > 0:  # 2nd quadrant
        return math.degrees(math.pi - math.asin(dy / r))

    return math.degrees(math.pi + math.asin(-dy / r))  # 3rd quadrant


def get_incs_and_angle(self, iinfo, jinfo, originx, originy, nj):
    ##########################################################
    # Returns the grid size, spacing and rotation of a grid2d.
    # We will assume a valid 2d grid
    # From Jon Magne Aagaard (AspenTech)
    ##########################################################
    i_offs = iinfo["Offset"]
    j_offs = jinfo["Offset"]
    i_spacing = iinfo["Spacing"]
    j_spacing = jinfo["Spacing"]

    iinc = i_spacing["Value"]
    jinc = j_spacing["Value"]
    idir = np.array([i_offs["Coordinate1"], i_offs["Coordinate2"]])
    jdir = np.array([j_offs["Coordinate1"], j_offs["Coordinate2"]])

    cross = np.cross(idir, jdir)
    angle = self.get_angle(idir[0], idir[1])

    # If the i j makes up a left handed system we just move the origin
    # and later shuffle the values accordingly
    right_handed = cross > 0
    if not right_handed:
        jdir_unit = jdir / np.linalg.norm(jdir)
        off = jdir_unit * (nj - 1) * jinc
        new_origin = np.array([originx, originy]) + off
        originx = float(new_origin[0])
        originy = float(new_origin[1])

    return (iinc, jinc, angle, originx, originy, right_handed)
