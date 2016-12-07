'''
Authors: Donnie Marino, Kostas Stamatiou
Contact: dmarino@digitalglobe.com

Unit tests for the gbdxtools.Catalog class
'''

from gbdxtools import Interface
from gbdxtools.catalog import Catalog
from auth_mock import get_mock_gbdx_session
import vcr
import unittest
import os

"""
How to use the mock_gbdx_session and vcr to create unit tests:
1. Add a new test that is dependent upon actually hitting GBDX APIs.
2. Decorate the test with @vcr appropriately, supply a yaml file path to gbdxtools/tests/unit/cassettes
    note: a yaml file will be created after the test is run

3. Replace "dummytoken" with a real gbdx token after running test successfully
4. Run the tests (existing test shouldn't be affected by use of a real token).  This will record a "cassette".
5. Replace the real gbdx token with "dummytoken" again
6. Edit the cassette to remove any possibly sensitive information (s3 creds for example)
"""


class TestCatalogv2(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        mock_gbdx_session = get_mock_gbdx_session(token="dummytoken")
        cls.gbdx = Interface(gbdx_connection=mock_gbdx_session)

    def test_init(self):
        c = Catalog(self.gbdx)
        self.assertTrue(isinstance(c, Catalog))

    @vcr.use_cassette(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'cassettes', 'test_catalogv2_get_address_coords.yaml'), filter_headers=['authorization'], decode_compressed_response=True)
    def test_catalogv2_get_address_coords(self):
        c = Catalog(self.gbdx)
        lat, lng = c.get_address_coords('Boulder, CO')
        self.assertTrue(lat == 40.0149856)
        self.assertTrue(lng == -105.2705456)

    @vcr.use_cassette(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'cassettes', 'test_catalogv2_get_record.yaml'), filter_headers=['authorization'], decode_compressed_response=True)
    def test_catalogv2_get_record(self):
        c = Catalog(self.gbdx)
        catid = '1040010019B4A600'
        record = c.get(catid)

        self.assertEqual(record['identifier'], '1040010019B4A600')
        self.assertTrue(isinstance(record['type'], list))
        self.assertEqual(record['type'][0], 'DigitalGlobeAcquisition')

        self.assertTrue('inEdges' not in record)
        self.assertTrue('owner' not in record)

    @vcr.use_cassette(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'cassettes', 'test_catalogv2_get_record_with_relationships.yaml'), filter_headers=['authorization'], decode_compressed_response=True)
    def test_catalogv2_get_record_with_relationships(self):
        c = Catalog(self.gbdx)
        catid = '1040010019B4A600'
        try:
            record = c.get(catid, includeRelationships=True)
        except Exception:
            assert True
        else:
            raise Exception('failed test')


    @vcr.use_cassette(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'cassettes', 'test_catalogv2_search_point.yaml'), filter_headers=['authorization'], decode_compressed_response=True)
    def test_catalogv2_search_point(self):
        c = Catalog(self.gbdx)
        lat = 40.0149856
        lng = -105.2705456
        results = c.search_point(lat, lng)

        self.assertEqual(len(results), 408)

    @vcr.use_cassette(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'cassettes', 'test_catalogv2_search_address.yaml'), filter_headers=['authorization'], decode_compressed_response=True)
    def test_catalogv2_search_address(self):
        c = Catalog(self.gbdx)
        results = c.search_address('Boulder, CO')

        self.assertEqual(len(results), 408)



    @vcr.use_cassette(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'cassettes', 'test_catalogv2_search_wkt_only.yaml'), filter_headers=['authorization'], decode_compressed_response=True)
    def test_catalogv2_wkt_only(self):
        c = Catalog(self.gbdx)
        results = c.search(searchAreaWkt="POLYGON ((30.1 9.9, 30.1 10.1, 29.9 10.1, 29.9 9.9, 30.1 9.9))")
        assert len(results) == 434

    @vcr.use_cassette(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'cassettes', 'test_catalogv2_search_wkt_and_startDate.yaml'), filter_headers=['authorization'], decode_compressed_response=True)
    def test_catalogv2_search_wkt_and_startDate(self):
        c = Catalog(self.gbdx)
        results = c.search(searchAreaWkt="POLYGON ((30.1 9.9, 30.1 10.1, 29.9 10.1, 29.9 9.9, 30.1 9.9))",
                           startDate='2012-01-01T00:00:00.000Z')
        assert len(results) == 343

    @vcr.use_cassette(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'cassettes', 'test_catalogv2_search_wkt_and_endDate.yaml'), filter_headers=['authorization'], decode_compressed_response=True)
    def test_catalogv2_search_wkt_and_endDate(self):
        c = Catalog(self.gbdx)
        results = c.search(searchAreaWkt="POLYGON ((30.1 9.9, 30.1 10.1, 29.9 10.1, 29.9 9.9, 30.1 9.9))",
                           endDate='2012-01-01T00:00:00.000Z')
        assert len(results) == 91

    @vcr.use_cassette(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'cassettes', 'test_catalogv2_search_startDate_and_endDate_only_more_than_one_week_apart.yaml'), filter_headers=['authorization'], decode_compressed_response=True)
    def test_catalogv2_search_startDate_and_endDate_only_more_than_one_week_apart(self):
        c = Catalog(self.gbdx)

        try:
            results = c.search(startDate='2004-01-01T00:00:00.000Z',
                               endDate='2012-01-01T00:00:00.000Z')
        except Exception as e:
            pass
        else:
            raise Exception('failed test')


    @vcr.use_cassette(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'cassettes', 'test_catalogv2_search_startDate_and_endDate_only_less_than_one_week_apart.yaml'), filter_headers=['authorization'], decode_compressed_response=True)
    def test_catalogv2_search_startDate_and_endDate_only_less_than_one_week_apart(self):
        c = Catalog(self.gbdx)

        results = c.search(startDate='2008-01-01T00:00:00.000Z',
                               endDate='2008-01-03T00:00:00.000Z')

        assert len(results) == 984


    @vcr.use_cassette(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'cassettes', 'test_catalogv2_search_filters1.yaml'), filter_headers=['authorization'], decode_compressed_response=True)
    def test_catalogv2_search_filters1(self):
        c = Catalog(self.gbdx)

        filters = [  
                        "(sensorPlatformName = 'WORLDVIEW01' OR sensorPlatformName ='QUICKBIRD02')",
                        "cloudCover < 10",
                        "offNadirAngle < 26",
                        "(offNadirAngle > 24 AND offNadirAngle < 26)"
                    ]

        results = c.search(startDate='2008-01-01T00:00:00.000Z',
                           endDate='2012-01-03T00:00:00.000Z',
                           filters=filters,
                           searchAreaWkt="POLYGON ((30.1 9.9, 30.1 10.1, 29.9 10.1, 29.9 9.9, 30.1 9.9))")

        for result in results:
            assert result['properties']['sensorPlatformName'] in ['WORLDVIEW01','QUICKBIRD02']
            assert float(result['properties']['cloudCover']) < 10
            assert float(result['properties']['offNadirAngle']) == 25

    @vcr.use_cassette(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'cassettes', 'test_catalogv2_search_filters2.yaml'), filter_headers=['authorization'], decode_compressed_response=True)
    def test_catalogv2_search_filters2(self):
        c = Catalog(self.gbdx)

        filters = [  
                    "sensorPlatformName = 'WORLDVIEW03'"
                  ]

        results = c.search(filters=filters,
                           searchAreaWkt="POLYGON ((30.1 9.9, 30.1 10.1, 29.9 10.1, 29.9 9.9, 30.1 9.9))")

        for result in results:
            assert result['properties']['sensorPlatformName'] in ['WORLDVIEW03']

    @vcr.use_cassette(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'cassettes', 'test_catalogv2_search_types1.yaml'), filter_headers=['authorization'], decode_compressed_response=True)
    def test_catalogv2_search_types1(self):
        c = Catalog(self.gbdx)

        types = [ "LandsatAcquisition" ]

        results = c.search(types=types,
                           searchAreaWkt="POLYGON ((30.1 9.9, 30.1 10.1, 29.9 10.1, 29.9 9.9, 30.1 9.9))")

        for result in results:
            assert result['type'] == 'LandsatAcquisition'

    @vcr.use_cassette(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'cassettes', 'test_catalogv2_search_huge_aoi.yaml'), filter_headers=['authorization'], decode_compressed_response=True)
    def test_catalogv2_search_huge_aoi(self):
        """
        Search an AOI the size of utah, broken into multiple smaller searches
        """
        c = Catalog(self.gbdx)

        results = c.search(searchAreaWkt = "POLYGON((-113.88427734375 40.36642741921034,-110.28076171875 40.36642741921034,-110.28076171875 37.565262680889965,-113.88427734375 37.565262680889965,-113.88427734375 40.36642741921034))")
        
        assert len(results) == 3694

    @vcr.use_cassette(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'cassettes', 'test_catalogv2_get_data_location_DG.yaml'), filter_headers=['authorization'], decode_compressed_response=True)
    def test_catalogv2_get_data_location_DG(self):
        c = Catalog(self.gbdx)
        s3path = c.get_data_location(catalog_id='1030010045539700')
        assert s3path == 's3://receiving-dgcs-tdgplatform-com/055158926010_01_003/055158926010_01'

    @vcr.use_cassette(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'cassettes', 'test_catalogv2_get_data_location_Landsat.yaml'), filter_headers=['authorization'], decode_compressed_response=True)
    def test_catalogv2_get_data_location_Landsat(self):
        c = Catalog(self.gbdx)
        s3path = c.get_data_location(catalog_id='LC81740532014364LGN00')
        assert s3path == 's3://landsat-pds/L8/174/053/LC81740532014364LGN00'

    @vcr.use_cassette(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'cassettes', 'test_catalogv2_get_data_location_nonexistent_catid.yaml'), filter_headers=['authorization'], decode_compressed_response=True)
    def test_catalogv2_get_data_location_nonexistent_catid(self):
        c = Catalog(self.gbdx)
        s3path = c.get_data_location(catalog_id='nonexistent_asdfasdfasdfdfasffds')
        assert s3path == None

    @vcr.use_cassette(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'cassettes', 'test_catalogv2_get_data_location_catid_with_no_data.yaml'), filter_headers=['authorization'], decode_compressed_response=True)
    def test_catalogv2_get_data_location_catid_with_no_data(self):
        c = Catalog(self.gbdx)
        s3path = c.get_data_location(catalog_id='1010010011AD6E00')
        assert s3path == None
