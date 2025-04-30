"""Init file for the water companies module."""

from .anglian_water import AnglianWater
from .northumbrian_water import NorthumbrianWater
from .severn_trent import SevernTrentWater
from .southern_water import SouthernWater
from .southwest_water import SouthWestWater
from .thames_water import ThamesWater
from .united_utilities import UnitedUtilities
from .welsh_water import WelshWater
from .wessex_water import WessexWater
from .yorkshire_water import YorkshireWater

__all__ = [
    "ThamesWater",
    "WelshWater",
    "SouthernWater",
    "AnglianWater",
    "WessexWater",
    "SouthWestWater",
    "UnitedUtilities",
    "YorkshireWater",
    "NorthumbrianWater",
    "SevernTrentWater",
]
