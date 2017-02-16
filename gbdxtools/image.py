"""
GBDX Catalog Image Interface.

Contact: chris.helm@digitalglobe.com
"""
from __future__ import print_function
import xml.etree.cElementTree as ET
from contextlib import contextmanager
import os

from shapely.wkt import loads
from shapely.geometry import box
import rasterio
import gdal

from gbdxtools.ipe.vrt import get_cached_vrt, put_cached_vrt, vrt_cache_key, IDAHO_CACHE_DIR
from gbdxtools.ipe.error import NotFound


band_types = {
  'MS': 'WORLDVIEW_8_BAND',
  'Panchromatic': 'PAN',
  'Pan': 'PAN'
}

class Image(object):
    """ 
      Strip Image Class 
      Collects metadata on all image parts, groupd pan and ms bands from idaho
    """

    def __init__(self, interface):
        self.interface = interface

    def __call__(self, cat_id, band_type="MS", node="TOAReflectance", **kwargs):
        self.cat_id = cat_id
        self._band_type = band_types[band_type]
        self._node = node
        self._pansharpen = kwargs.get('pansharpen', False)
        self._acomp = kwargs.get('acomp', False)
        if self._pansharpen:
            self._node = 'Pansharpened'
        self._level = kwargs.get('level', 0)
        self._fetch_metadata()
        return self

    @contextmanager
    def open(self, *args, **kwargs):
        """ A rasterio based context manager for reading the full image VRT """
        with rasterio.open(self.vrt, *args, **kwargs) as src:
            yield src

    @property
    def vrt(self):
        try:
            vrt = get_cached_vrt(self.cat_id, self._node, self._level)
        except NotFound:
            vrt = os.path.join(IDAHO_CACHE_DIR, vrt_cache_key(self.cat_id, self._node, self._level)) 
            _tmp_vrt = gdal.BuildVRT('/vsimem/merged.vrt', self._collect_vrts(), separate=False)
            gdal.Translate(vrt, _tmp_vrt, format='VRT')
        return vrt 

    def aoi(self, bbox, band_type='MS', **kwargs):
        try:
            band_type = band_types[band_type]
        except:
            print('band_type ({}) not supported'.format(band_type))
            return None
        _area = box(*bbox)
        intersections = {}
        for part in self.metadata['properties']['parts']:
            for key, item in part.iteritems():
                geom = box(*item['bounds'])
                if geom.intersects(_area):
                    intersections[key] = item

        if not len(intersections.keys()):
            print('Failed to find data within the given BBOX')
            return None

        pansharpen = kwargs.get('pansharpen', self._pansharpen)
        if self._node == 'Pansharpened' and pansharpen:
            md = intersections['WORLDVIEW_8_BAND']
            pan = intersections['PAN']
            return self.interface.ipeimage(md['imageId'], bbox=bbox, pan=pan, node='Pansharpened', **kwargs)
        elif band_type in intersections:
            md = intersections[band_type]
            return self.interface.ipeimage(md['imageId'], bbox=bbox, **kwargs)
        else:
            print('band_type ({}) did not find a match in this image'.format(band_type))
            return None

    def _fetch_metadata(self):
        props = self.interface.catalog.get(self.cat_id)['properties']
        f = loads(props['footprintWkt'])
        geom = f.__geo_interface__
        idaho = self.interface.idaho.get_images_by_catid(self.cat_id)
        parts = self.interface.idaho.describe_images(idaho)[self.cat_id]['parts']
        idaho = {k['identifier']: k for k in idaho['results']}
        props['bounds'] = f.bounds
        props['parts'] = []
        for p, info in parts.iteritems():
            part = {}
            for key, img in info.iteritems():
                if img['id'] in idaho:
                    part[key] = idaho[img['id']]['properties']
                    part[key]['bounds'] = loads(idaho[img['id']]['properties']['footprintWkt']).bounds
            props['parts'].append(part)

        self.metadata = {'properties': props, 'geometry': geom} 

    def _collect_vrts(self):
        vrts = []
        for part in self.metadata['properties']['parts']:
            if self._node == 'Pansharpened':
                md = part['WORLDVIEW_8_BAND']
                pan = part['PAN']
                img = self.interface.ipeimage(md['imageId'], pan=pan, node=self._node)
            else:
                md = part[self._band_type]
                img = self.interface.ipeimage(md['imageId'])
            vrts.append(img.vrt)
        return vrts



if __name__ == '__main__': 
    from gbdxtools import Interface
    import json
    import rasterio
    gbdx = Interface()

    cat_id = '104001001838A000'
    img = gbdx.image(cat_id, pansharpen=True)

    #print(json.dumps(img.metadata, indent=4))
    vrt = img.vrt
    with rasterio.open(vrt) as src:
        print(src.meta)
  

    #aoi = img.aoi([-95.06904982030392, 29.7187207124839, -95.06123922765255, 29.723901202069023])
    #with aoi.open() as src:
    #    assert isinstance(src, rasterio.DatasetReader)

