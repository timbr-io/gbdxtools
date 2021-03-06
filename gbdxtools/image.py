"""
GBDX IDAHO Interface.

Contact: nate.ricklin@digitalglobe.com
"""
from __future__ import print_function
from __future__ import division
from builtins import str
from builtins import object
from past.utils import old_div

from shapely.wkt import loads
import codecs
import json
import os
import requests

from gbdxtools.catalog import Catalog

class Image(object):
    """ 
      Strip Image Class 
      Collects metadata on all image parts, groupd pan and ms bands from idaho

      
    """

    def __init__(self, interface):
        self.interface = interface
        self.gbdx_connection = self.interface.gbdx_connection
        self.logger = interface.logger

    def __call__(self, cat_id):
        self.cat_id = cat_id
        self._fetch_metadata()
        return self

    def _fetch_metadata(self):
        props = self.interface.catalog.get(self.cat_id)['properties']
        f = loads(props['footprintWkt'])
        geom = f.__geo_interface__
        del props['footprintWkt']
        idaho = self.interface.idaho.get_images_by_catid(self.cat_id)
        parts = self.interface.idaho.describe_images(idaho)[self.cat_id]['parts']
        idaho = {k['identifier']: k for k in idaho['results']}
        
        props['parts'] = []
        for p, info in parts.iteritems():
            part = {}
            for key, img in info.iteritems():
                if img['id'] in idaho:
                    part[key] = idaho[img['id']]['properties']
                    part[key]['bounds'] = loads(idaho[img['id']]['properties']['footprintWkt']).bounds
            props['parts'].append(part)

        self.metadata = {
            'properties': props,
            'geometry': geom
        }

    def vrt(self):
        print('Create a vrt from image parts')
        # look for a vrt on disk else create one
        # return path the vrt

    def _generate_vrt(self):
        cols = str(self.darr.shape[-1])
        rows = str(self.darr.shape[1])
        (minx, miny, maxx, maxy) = rasterio.windows.bounds(self._roi, self._src.transform)
        affine = [c for c in rasterio.transform.from_bounds(minx, miny, maxx, maxy, int(cols), int(rows))]
        transform = [affine[2], affine[0], 0.0, affine[5], 0.0, affine[4]]

        vrt = ET.Element("VRTDataset", {"rasterXSize": cols, "rasterYSize": rows})
        ET.SubElement(vrt, "SRS").text = str(self._src.crs['init']).upper()
        ET.SubElement(vrt, "GeoTransform").text = ", ".join(map(str, transform))
        for i in self._src.indexes:
            band = ET.SubElement(vrt, "VRTRasterBand", {"dataType": self._src.dtypes[i-1].title(), "band": str(i)})
            src = ET.SubElement(band, "SimpleSource")
            ET.SubElement(src, "SourceFilename").text = "HDF5:{}://{}_{}_{}".format(self._filename, self._gid, self.node, self.level)
            ET.SubElement(src, "SourceBand").text =str(i)
            ET.SubElement(src, "SrcRect", {"xOff": "0", "yOff": "0",
                                           "xSize": cols, "ySize": rows})
            ET.SubElement(src, "DstRect", {"xOff": "0", "yOff": "0",
                                           "xSize": cols, "ySize": rows})

            ET.SubElement(src, "SourceProperties", {"RasterXSize": cols, "RasterYSize": rows,
                                                    "BlockXSize": "128", "BlockYSize": "128", "DataType": self._src.dtypes[i-1].title()})
        vrt_str = ET.tostring(vrt)

        with open(self.vrt, "w") as f:
            f.write(vrt_str)

        return self.vrt

    def aoi(self, bbox=None, geometry=None, pansharpen=False):
        if bbox is None:
            print('Missing either a bbox or a geometry to define an AOI')
            return None
        else:
            return AOI(self, bbox)
            #W, S, E, N = (-95.06904982030392, 29.7187207124839, -95.06123922765255, 29.723901202069023)
            #chip_geo = 'houston_geo.tif'
            #self.interface.idaho.get_chip(coordinates=[W, S, E, N], catid = catid, chip_type='PAN', filename=chip_geo)


class AOI(object):
    def __init__(self, image, bbox):
      self.image = image
      self.bbox = bbox
      self.metadata = metadata
       

if __name__ == '__main__': 
    from gbdxtools import Interface
    import json
    gbdx = Interface()

    cat_id = '104001001838A000'
    img = gbdx.image(cat_id)

    #print(json.dumps(img.metadata, indent=4))
    img.vrt()
    img.aoi()
