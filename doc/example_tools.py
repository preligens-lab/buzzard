import datetime
import os
import glob

import buzzard as buzz
import matplotlib.pyplot as plt

class Timer():

    def __enter__(self):
        self._start = datetime.datetime.now()
        return self

    def __exit__(self, *_):
        self._stop = datetime.datetime.now()
        pass

    def __str__(self):
        dt = self._stop - self._start
        dt = dt.total_seconds()
        return '{:.2}s'.format(dt)

def list_cache_files_path_in_dir(cache_dir):
    s = os.path.join(cache_dir, '*_[0123456789abcdef]*.tif')
    return glob.glob(s)

def show_several_images(*args):

    for title, fp, arr in args:
        plt.imshow(arr, extent=fp.extent)
        plt.show()
