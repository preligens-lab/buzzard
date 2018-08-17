from typing import *

import numpy as np

from buzzard._footprint import Footprint

class ActorBuilder(object):

    def __init__(self, raster):
        self._raster = raster
        self._queries = {}

    @property
    def address(self):
        return '/Raster{}/Builder'.format(self._raster.uid)

    def receive_those_cache_tiles_are_ready(self, query_key: int, cache_fps: Set[Footprint]):
        """Receive message: Those cache tiles are ready for that query

        Store the information
        """
        pass

class _Query(object):

    def __init__(self, infos):
        self.infos = infos
        self.set_of_cache_fp_ready = set()
