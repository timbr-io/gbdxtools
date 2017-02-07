from functools import partial, wraps
from itertools import groupby
from collections import defaultdict
from contextlib import contextmanager
from xml.etree import cElementTree as ET
import os.path
import signal
signal.signal(signal.SIGPIPE, signal.SIG_IGN)

import requests

import rasterio
import dask
import dask.array as da
import dask.bag as db
from dask.delayed import delayed
import numpy as np
from scipy.sparse import dok_matrix
idaho_id = "59f87923-7afa-4588-9f59-dc5ad8b821b0"
threaded_get = partial(dask.threaded.get, num_workers=8)

import pycurl
_curl_pool = defaultdict(pycurl.Curl)

class IpeImage(da.Array):
    def __init__(self, idaho_id, bounds=None, node="TOAReflectance", **kwargs):
        self._idaho_id = idaho_id
        self._node_id = node
        self._level = 0
        self._vrt = requests.get(self.vrt).content
        self._cfg = self._config_dask()
        
        print self._cfg["chunks"]
        
        super(IpeImage, self).__init__(**self._cfg)
        print self._cfg["chunks"]
        
    @property
    def vrt(self):
        return "http://idaho.timbr.io/{idaho_id}/{node}/{level}.vrt".format(idaho_id=self._idaho_id, 
                                                                            node=self._node_id,
                                                                            level=self._level)
    
    @delayed
    def load_url(self, url, bands=8):
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
                arr = np.zeros([bands] + self._cfg["chunks"], dtype=self._cfg["dtype"])
                _curl.close()
                del _curl_pool[thread_id]
        return arr
    
    def read(self, bands=None):
        return self.compute(get=threaded_get)
    
    @contextmanager
    def open(self, *args, **kwargs):
        with rasterio.open(self.vrt, *args, **kwargs) as src:
            yield src

    def _config_dask(self, aoi=None):
        with self.open() as src:
            nbands = len(src.indexes)
            cfg = {"shape": tuple([nbands] + list(src.shape)), "dtype": src.dtypes[0], 
                   "chunks": tuple([len(src.block_shapes)] + list(src.block_shapes[0]))}
            self._meta = src.meta
        urls = self._collect_urls(self._vrt)
        img = self._build_array(urls, bands=nbands, chunks=[nbands]+list(cfg["chunks"]), dtype=cfg["dtype"])
        cfg["name"] = img.name
        cfg["dask"] = img.dask
        
        return cfg
    
    def _build_array(self, urls, bands=8, chunks=(1,256,256), dtype=np.float32):
        total = sum([len(x) for x in urls])
        buf = da.concatenate(
            [da.concatenate([da.from_delayed(self.load_url(url, bands=bands), chunks, dtype) for u, url in enumerate(row)],
                        axis=1) for r, row in enumerate(urls)], axis=2)
        return buf
    
    
    def _collect_urls(self, xml, aoi=None):
        root = ET.fromstring(xml)
        if aoi is not None:
            target = box(*aoi.bounds)
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

