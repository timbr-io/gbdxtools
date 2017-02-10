from functools import partial, wraps
from itertools import groupby
from collections import defaultdict
from contextlib import contextmanager
from xml.etree import cElementTree as ET
import os.path
import signal
signal.signal(signal.SIGPIPE, signal.SIG_IGN)
import warnings
warnings.filterwarnings('ignore')

try:
    from io import BytesIO
except ImportError:
    from StringIO import cStringIO as BytesIO

import requests

from shapely.geometry import box
import rasterio
from rasterio.io import MemoryFile
import dask
import dask.array as da
import dask.bag as db
from dask.delayed import delayed
import numpy as np
from scipy.sparse import dok_matrix
idaho_id = "59f87923-7afa-4588-9f59-dc5ad8b821b0"
import threading
threaded_get = partial(dask.threaded.get, num_workers=4)

import pycurl
_curl_pool = defaultdict(pycurl.Curl)

from gbdxtools.ipe.vrt import get_vrt


@delayed
def load_url(url, bands=8):
    thread_id = threading.current_thread().ident
    _curl = _curl_pool[thread_id]
    buf = BytesIO()
    _curl.setopt(_curl.URL, url)
    _curl.setopt(_curl.WRITEDATA, buf)
    _curl.setopt(pycurl.NOSIGNAL, 1)
    _curl.perform()

    with MemoryFile(buf.getvalue()) as memfile:
      try:
          with memfile.open(driver="GTiff") as dataset:
              arr = dataset.read()
      except (TypeError, rasterio.RasterioIOError) as e:
          print("Errored on {} with {}".format(url, e))
          arr = np.zeros([bands,256,256], dtype=np.float32)
          _curl.close()
          del _curl_pool[thread_id]
    return arr

class IpeImage(da.Array):
    def __init__(self, idaho_id, bounds=None, node="TOAReflectance", **kwargs):
        self._idaho_id = idaho_id
        self._bounds = bounds
        self._node_id = node
        self._level = 0
        self._tile_size = kwargs.get('tile_size', 256)
        with open(self.vrt) as f:
            self._vrt = f.read()
        self._cfg = self._config_dask(bounds=bounds)
        super(IpeImage, self).__init__(**self._cfg)
        self._cache = None
        
    @property
    def vrt(self):
        return get_vrt(self._idaho_id, node=self._node_id, level=self._level)

    def read(self, bands=None):
        if self._cache is not None:
            arr = self._cache
        else:
            print 'fetching data'
            arr = self.compute(get=threaded_get)
            self._cache = arr
        if bands is not None:
            arr = arr[bands, ...]
        return arr

    def aoi(self, bounds):
        return IpeImage(self._idaho_id, bounds=bounds)

    def geotiff(self, path, dtype=None):
        print self.shape
        arr = self.read()
        with self.open() as src:
            meta = src.meta.copy()
            meta.update({'driver': 'GTiff'})

            if dtype is not None:
                meta.update({'dtype': dtype})

            with rasterio.open(path, "w", **meta) as dst:
                dst_data = src.read()
                if dtype is not None:
                    dst_data = dst_data.astype(dtype)
                dst.write(dst_data)
        return path
    
    @contextmanager
    def open(self, *args, **kwargs):
        with rasterio.open(self.vrt, *args, **kwargs) as src:
            yield src

    def _config_dask(self, bounds=None):
        with self.open() as src:
            nbands = len(src.indexes)
            px_bounds = None
            if bounds is not None:
                window = src.window(*bounds)
                px_bounds = self._pixel_bounds(window, src.block_shapes)
            urls = self._collect_urls(self._vrt, px_bounds=px_bounds)
            cfg = {"shape": tuple([nbands] + [self._tile_size*len(urls[0]), self._tile_size*len(urls)]), 
                   "dtype": src.dtypes[0], 
                   "chunks": tuple([len(src.block_shapes)] + [self._tile_size, self._tile_size])}
            self._meta = src.meta
        img = self._build_array(urls, bands=nbands, chunks=cfg["chunks"], dtype=cfg["dtype"])
        cfg["name"] = img.name
        cfg["dask"] = img.dask
        
        return cfg
    
    def _build_array(self, urls, bands=8, chunks=(1,256,256), dtype=np.float32):
        buf = da.concatenate(
            [da.concatenate([da.from_delayed(load_url(url, bands=bands), chunks, dtype) for u, url in enumerate(row)],
                        axis=1) for r, row in enumerate(urls)], axis=2)

        return buf    
    
    def _collect_urls(self, xml, px_bounds=None):
        root = ET.fromstring(xml)
        if px_bounds is not None:
            target = box(*px_bounds)
            for band in root.findall("VRTRasterBand"):
                for source in band.findall("ComplexSource"):
                    rect = source.find("DstRect")
                    xmin, ymin = int(rect.get("xOff")), int(rect.get("yOff"))
                    xmax, ymax = xmin + int(rect.get("xSize")), ymin + int(rect.get("ySize"))
                    if not box(xmin, ymin, xmax, ymax).intersects(target):
                        band.remove(source)

        urls = list(set(item.text for item in root.iter("SourceFilename")
                    if item.text.startswith("http://")))
        chunks = []
        for url in urls:
            head, _ = os.path.splitext(url)
            head, y = os.path.split(head)
            head, x = os.path.split(head)
            head, key = os.path.split(head)
            y = int(y)
            x = int(x)
            chunks.append((x, y, url))

        grid = [[rec[-1] for rec in sorted(it, key=lambda x: x[1])]
                for key, it in groupby(sorted(chunks, key=lambda x: x[0]), lambda x: x[0])]
        return grid

    def _pixel_bounds(self, window, block_shapes, preserve_blocksize=False):
        if preserve_blocksize:
            window = rasterio.windows.round_window_to_full_blocks(window, block_shapes)
        roi = window.flatten()
        return [roi[0], roi[1], roi[0] + roi[2], roi[1] + roi[3]]

if __name__ == '__main__':
    img = IpeImage('58e9febc-0b04-4143-97fb-95436fcf3ed6', bounds=[-95.06904982030392, 29.7187207124839, -95.06123922765255, 29.723901202069023])
    with img.open() as src:
        assert isinstance(src, rasterio.DatasetReader)

    rgb = img[[4,2,1], ...] # should not fetch
    assert isinstance(rgb, da.Array)
    print rgb.shape
  
    data = img.read(bands=[4,2,1]) # should fetch
    assert isinstance(data, np.ndarray)
    assert len(data.shape) == 3
    assert data.shape[0] == 3
