"""
Catalog 2 vectorServices convertor.

Goal:
    Make a function that takes catalog/v1 API json and converts to VectorServices json.  Also
    translate the results back to catalog/v1 spec.

Not Supported:
    - timestamp query in filters
    - implied traversal in filters
    - string matching:  "name like 'D%'"

Explicitly Supported:
    - single clause <, >, <=, >=, =, !=, == filters
    - identifier field in filters
    - sensorPlatformName mapping
    - multiple clauses in one filter, but not with negation operators (!=, <>)

Differences:
    - integer fields will not work in VS if value is set as double.  Double works fine either way
    - sunAzimuth value seems not correct in VS.
    - return floats & ints instead of stringified values

TODO for migration:
	- add 'available' flag or compute it
	- figure out products
	- add 'ordered' flag or compute it
"""

import json
from pygeoif import geometry
from geomet import wkt

fieldmap = {
    'sunElevation':       'AVSUNELEV_dbl',
    'targetAzimuth':      'AVTARGETAZ_dbl',
    'sensorPlatformName': 'PLATFORM', # field value is different too
    'browseURL':          'BROWSEURL',
    'sunAzimuth':         'AVSUNAZIM_dbl',
    'cloudCover':         'CLOUDCOVER_int',
    'multiResolution':    'AVMULTIRES_dbl',
    #'vendorName':          None,
    'offNadirAngle':      'MXOFFNADIR_int',  # AVOFFNADIR_int
    'panResolution':      'AVPANRES_dbl',
    'catalogID':          'CATALOGID',
    'identifier':         'CATALOGID',
    'imageBands':         'IMAGEBANDS'
    #'timestamp':          'ACQDATE',
}

# map sensorPlatformName values
sensormap = {
    'WORLDVIEW01': 'WV01',
    'WORLDVIEW02': 'WV02',
    'WORLDVIEW03': 'WV03',
    'QUICKBIRD02': 'QB02',
    'GEOEYE01': 'GE01'
}
sensormap_inv = {
	'WV01': 'WORLDVIEW01',
	'WV02': 'WORLDVIEW02',
	'WV03': 'WORLDVIEW03',
	'QB02': 'QUICKBIRD02',
	'GE01': 'GEOEYE01'
}

def catalog2vs(input_json):
    """
    Take input json that looks like this:
    { 
        "searchAreaWkt":"wktstring",
        "startDate":"2004-01-01T00:00:00.000Z",
        "endDate":"2015-04-30T23:59:59.999Z",
        "filters":[ 
            "_acquisition.productLevel = 'LV1B'",
            "sensorPlatformName = 'WORLDVIEW02'",
            "targetAzimuth < 250",
            "(targetAzimuth < 250 OR targetAzimuth > 255)"
        ],
        "tagResults":false,
        "types":[ 
        "Acquisition"
        ]
    }
    and make an elastic search vector-services query out of it.

    Date works like this:   
        item_date:[now-1M TO now]
        item_date:[2016-01-01 TO 2016-02-01]
        item_date:[2016-01-01 TO now]

    """
    
    input_dict = json.loads(input_json)

    # convert wkt to search bounds
    try:
        if not input_dict.get('searchAreaWkt'):
            left, lower, right, upper = -180, -90, 180, 90
        else:
            searchAreaWkt = input_dict.get('searchAreaWkt')
            search_area_polygon = geometry.from_wkt(searchAreaWkt)
            left, lower, right, upper = search_area_polygon.bounds
    except:
        raise Exception('Unable to parse WKT string.')

    # figure out the date bounds
    if not input_dict.get('startDate') and not input_dict.get('endDate'):
        datestr = ''
    elif input_dict.get('startDate') and not input_dict.get('endDate'):
        datestr = 'item_date:[%s TO now]' % input_dict.get('startDate')
    elif not input_dict.get('startDate') and input_dict.get('endDate'):
        datestr = 'item_date:[now-20y TO %s]' % input_dict.get('endDate')
    else:
        datestr = 'item_date:[%s TO %s]' % (input_dict.get('startDate'), input_dict.get('endDate'))

    # figure out types:
    item_type_str = 'item_type:(' + ' OR '.join(input_dict.get('types')) + ')'

    # figure out filters
    filter_strs = []
    # attempt to parse each filter in the input dict
    for filt in input_dict.get('filters'):
        # replace each fieldname according to the fieldname map
        for fieldname in list(fieldmap.keys()):
            #if filt.strip().startswith(fieldname):
            if fieldname in filt:
                new_filter = filt.strip().replace(fieldname,'attributes.' + fieldmap[fieldname] + ':')
                
                # Replace sensor values
                if fieldname == 'sensorPlatformName':
                    for sensorname in list(sensormap.keys()):
                        new_filter = new_filter.replace(sensorname, sensormap[sensorname])

                # now remove any whitespace around <, >, <=, >= because ES doesn't like it
                for operator in ['<','>','<=','>=','=','==']:
                    if operator in new_filter:
                        new_filter = operator.join(x.strip() for x in new_filter.split(operator))

                # Handle negation operators.  If we find one, just remove it and stick a 
                # NOT in front of the filter.
                for operator in ['<>','!=']:
                    if operator in new_filter:
                        new_filter = 'NOT ' + new_filter.replace(operator,'')

                filter_strs.append( '(' + new_filter + ')')

    filter_strs.append(datestr)
    # Skip item type for now.  Use platformname instead
    #filter_strs.append(item_type_str)
    filter_strs.append('ingest_source:\\"DG Catalog\\"')

    filter_strs = [i for i in filter_strs if i]  # keep only non-None items
    query = ' AND '.join(filter_strs)
    return query


def vsResult2CatalogResult(record):
    """
    Convert a vector object received from the VectorService into the json/python representation
    of catalog/v1.

    graph edges are omitted because they are not known.
    available is always = False, because it is not known.
    owner is empty, because it is not known.
    
    """
    new_type = ''
    vendorname = ''
    for item_type in record['properties']['item_type']:
    	if item_type in list(sensormap.values()):
    		new_type = 'DigitalGlobeAcquisition'
    		vendorname = 'DigitalGlobe'

    # Convert potential multipolygon to polygon
    geojson = record['geometry']
    p = geometry.from_wkt(wkt.dumps(geojson))
    if p._type == 'MultiPolygon':
    	footprintWkt = p.geoms[0].wkt
    elif p._type == 'Polygon':
    	footprintWkt = p.wkt

    new_record = {
      "identifier": record['properties']['attributes']['CATALOGID'],
      "owner": "",
      "type": new_type,
      "properties": {
        "sunElevation": record['properties']['attributes']['AVSUNELEV_dbl'],
        "targetAzimuth": record['properties']['attributes']['AVTARGETAZ_dbl'],
        "sensorPlatformName": sensormap_inv[record['properties']['attributes']['PLATFORM']],
        "browseURL": record['properties']['attributes']['BROWSEURL'],
        "sunAzimuth": record['properties']['attributes']['AVSUNAZIM_dbl'],
        "footprintWkt": footprintWkt,
        "cloudCover": record['properties']['attributes']['CLOUDCOVER_int'],
        "available": "false",
        "multiResolution": record['properties']['attributes']['AVMULTIRES_dbl'],
        "vendorName": vendorname,
        "offNadirAngle": record['properties']['attributes']['MXOFFNADIR_int'],
        "panResolution": record['properties']['attributes']['AVPANRES_dbl'],
        "catalogID": record['properties']['attributes']['CATALOGID'],
        "imageBands": record['properties']['attributes']['IMAGEBANDS'],
        "timestamp": record['properties']['item_date']
      }
    }

    return new_record


def vsResult2CatalogResult_new_schema(record):
    """
    Convert a vector object received from the VectorService into the json/python representation
    of catalog/v1.

    graph edges are omitted because they are not known.
    available is always = False, because it is not known.
    owner is empty, because it is not known.
    
    """
    new_type = ''
    vendorname = ''
    for item_type in record['properties']['item_type']:
        if item_type in list(sensormap.values()):
            new_type = 'DigitalGlobeAcquisition'
            vendorname = 'DigitalGlobe'

    # Convert potential multipolygon to polygon
    geojson = record['geometry']
    p = geometry.from_wkt(wkt.dumps(geojson))
    if p._type == 'MultiPolygon':
        footprintWkt = p.geoms[0].wkt
    elif p._type == 'Polygon':
        footprintWkt = p.wkt

    new_record = {
      "identifier": record['properties']['attributes']['catalogID'],
      "owner": "",
      "type": new_type,
      "properties": {
        "sunElevation": record['properties']['attributes']['sunElevation_dbl'],
        "targetAzimuth": record['properties']['attributes']['targetAzimuth_dbl'],
        "sensorPlatformName": sensormap_inv[record['properties']['attributes']['sensorPlatformName']],
        "browseURL": record['properties']['attributes']['browseURL'],
        "sunAzimuth": record['properties']['attributes']['sunAzimuth_dbl'],
        "footprintWkt": footprintWkt,
        "cloudCover": record['properties']['attributes']['cloudCover_int'],
        "available": "false",
        "multiResolution": record['properties']['attributes']['multiResolution_dbl'],
        "vendorName": vendorname,
        "offNadirAngle": record['properties']['attributes']['offNadirAngle_int'],
        "panResolution": record['properties']['attributes']['panResolution_dbl'],
        "catalogID": record['properties']['attributes']['catalogID'],
        "imageBands": record['properties']['attributes']['imageBands'],
        "timestamp": record['properties']['item_date']
      }
    }

    return new_record






