import unittest
from unittest.mock import patch, create_autospec
from geopandas import GeoDataFrame
import pandas as pd
from pyplaces import FoursquareOpenPlaces, OvertureMaps
from pyplaces._pyplaces_datasource import pyplaces_datasource 


class TestPyPlacesDatasource(unittest.TestCase):
    def setUp(self):
        self.mock_subclass = create_autospec(spec=pyplaces_datasource,instance=True)
        self.user_optional_valid = {
            "columns_overture": ["confidence"],
            "filters": ("confidence", "<", 0.9),
            "distance": 1000,
            "unit": "km"
        }
        self.user_provided_valid = {
            "address": "204 Hemenway Street",
            "bbox": (-71.11088398563271, 42.32145496739517, -71.0987557723746, 42.33045192410487)
        }
        self.user_optional_invalid = {
            "columns": ["sfdsfd"],
            "filters_invalid_op": ("confidence", "(", 0.9),
            "filters_invalid_field":("fdscds","<",0.9),
            "filters_invalid_value":("confidence", "<", "a"),
            "unit": "sfd"
        }
        self.user_provided_invalid = {
            "address": "fdlksjfld",
            "bbox": (-181, -91, 181, 91)
        }
    
    def test_invalid_operator(self):
        with self.assertRaises(Exception):
            self.mock_subclass.from_address(
                address=self.user_provided_valid["address"],
                filters=self.user_optional_invalid["filters_invalid_op"]
            )
            
            
    def test_invalid_unit(self):
        with self.assertRaises(Exception):
            self.mock_subclass.from_address(
                address=self.user_provided_valid["address"],
                unit=self.user_optional_invalid["unit"]
            )
    
    def test_invalid_point(self):
        with self.assertRaises(Exception):
            self.mock_subclass.from_address(
                address=(-181, 91)
            )
    
    def test_invalid_address(self):
        with self.assertRaises(Exception):
            self.mock_subclass.from_place(
                address=self.user_provided_invalid["address"]
            )
        with self.assertRaises(Exception):
            self.mock_subclass.from_address(
                address=self.user_provided_invalid["address"]
            )
    
    def test_invalid_bbox(self):
        with self.assertRaises(Exception):
            self.mock_subclass.from_bbox(
                bbox=self.user_provided_invalid["bbox"]
            )
            
            
    #TODO dataset specific
    # def test_invalid_value(self):
    #     with self.assertRaises(Exception):
    #         self.mock_subclass.foursquare_places_from_address(
    #             address=self.user_provided_valid["address"],
    #             filters=self.user_optional_invalid["filters_invalid_value"]
    #         )
            
    # def test_invalid_field(self):
    #     with self.assertRaises(Exception):
    #         self.mock_subclass.foursquare_places_from_address(
    #             address=self.user_provided_valid["address"],
    #             filters=self.user_optional_invalid["filters_invalid_field"]
    #         )
    
    # def test_invalid_column(self):
    #     with self.assertRaises(Exception):
    #         self.mock_subclass.from_address(
    #             address=self.user_provided_valid["address"],
    #             columns=self.user_optional_invalid["columns"]
    #         )


class TestFoursquarePlaces(unittest.TestCase):
    
    def setUp(self):
        self.fsq_places = FoursquareOpenPlaces()
        # Set up test data
        self.user_optional_valid = {
            "columns_overture": ["confidence"],
            "filters": ("confidence", "<", 0.9),
            "distance": 1000,
            "unit": "km"
        }
        self.user_provided_valid = {
            "address": "204 Hemenway Street",
            "bbox": (-71.11088398563271, 42.32145496739517, -71.0987557723746, 42.33045192410487)
        }
        self.user_optional_invalid = {
            "columns": ["sfdsfd"],
            "filters_invalid_op": ("confidence", "(", 0.9),
            "filters_invalid_field":("fdscds","<",0.9),
            "filters_invalid_value":("confidence", "<", "a"),
            "unit": "sfd"
        }
        self.user_provided_invalid = {
            "address": "fdlksjfld",
            "bbox": (-181, -91, 181, 91)
        }
        self.releases = {
            "invalid_release":"10-23-3453",
            "valid_fsq":"2025-02-06",
            "valid_overture":"2025-01-22.0"
        }
        
        self.places_schema = ["fsq_place_id","name","latitude","longitude","address","locality","region","postcode","admin_region",
                            "post_town","po_box","country","date_created","date_refreshed","tel","website","email","facebook_id","instagram",
                            "fsq_category_ids","fsq_category_labels","placemaker_url","geom","bbox"]
        self.places_schema = self.places_schema.sort()
        self.categories_schema = ["category_id","category_level","category_name","category_label","level1_caregory_id","level1_category_name",
                                "level2_caregory_id","level2_category_name","level3_caregory_id","level3_category_name","level4_caregory_id",
                                "level4_category_name","level5_caregory_id","level5_category_name","level6_caregory_id","level6_category_name"]
        self.categories_schema = self.categories_schema.sort()
    
    
    def test_check_release(self):
        with self.assertRaises(ValueError):  # Or whatever exception type is expected
            self.fsq_places.check_release(release=self.releases["invalid_release"])
        
        try:
            self.fsq_places.check_release(release=self.releases["valid_fsq"])
        except Exception as e:
            self.fail(f"check_release with correct release raised exception {e} unexpectedly!")
    
    def test_valid_inputs(self):
        #categories
        with patch.object(self.fsq_places, 'get_categories', return_value=GeoDataFrame):
            result = self.fsq_places.get_categories()
            # columns = result.columns.to_list().sort()
            print(result.columns)
            self.assertIsInstance(result, GeoDataFrame)
            self.assertEqual(columns, self.categories_schema)
        
        #from address
        with patch.object(self.fsq_places, 'foursquare_places_from_address', return_value=GeoDataFrame): 
            result = self.fsq_places.foursquare_places_from_address(
                address=self.user_provided_valid["address"],
                distance=self.user_optional_valid["distance"],
                unit=self.user_optional_valid["unit"]
            )
            columns = result.columns.to_list().sort()
            self.assertIsInstance(result, GeoDataFrame)
            self.assertEqual(columns, self.places_schema)
        
        #from place
        with patch.object(self.fsq_places, 'foursquare_places_from_place', return_value=GeoDataFrame):
            result = self.fsq_places.foursquare_places_from_place(
                address=self.user_provided_valid["address"]
            )
            columns = result.columns.to_list().sort()
            self.assertIsInstance(result, GeoDataFrame)
            self.assertEqual(columns, self.places_schema)
        #from bbox
        with patch.object(self.fsq_places, 'foursquare_places_from_bbox', return_value=GeoDataFrame):
            result = self.fsq_places.foursquare_places_from_bbox(
                bbox=self.user_provided_valid["bbox"]
            )
            columns = result.columns.to_list().sort()
            self.assertIsInstance(result, GeoDataFrame)
            self.assertEqual(columns, self.places_schema)


class TestOvertureMaps(unittest.TestCase):
    
    def setUp(self):
        self.overture_maps = OvertureMaps()
        # Set up test data
        self.user_provided_valid = {
            "bbox": (-71.11088398563271, 42.32145496739517, -71.0987557723746, 42.33045192410487)
        }
        self.releases = {
            "invalid_release":"10-23-3453",
            "valid_overture":"2025-01-22.0"
        }
        
        self.places_schema = ['id', 'geometry', 'bbox', 'version', 'sources', 'names', 'categories',
                                'confidence', 'websites', 'socials', 'emails', 'phones', 'brand','addresses']
        self.addresses_schema = ['id', 'geometry', 'bbox', 'version', 'sources', 'names', 'categories',
                                'confidence', 'websites', 'socials', 'emails', 'phones', 'brand','addresses']
        self.buildings_schema = ['id', 'geometry', 'bbox', 'version', 'sources', 'level', 'subtype',
                                'class', 'height', 'names', 'has_parts', 'is_underground', 'num_floors',
                                'num_floors_underground', 'min_height', 'min_floor', 'facade_color',
                                'facade_material', 'roof_material', 'roof_shape', 'roof_direction',
                                'roof_orientation', 'roof_color', 'roof_height']
        self.buildings_part_schema = ['id', 'geometry', 'bbox', 'version', 'sources', 'level', 'height',
                                    'names', 'is_underground', 'num_floors', 'num_floors_underground',
                                    'min_height', 'min_floor', 'facade_color', 'facade_material',
                                    'roof_material', 'roof_shape', 'roof_direction', 'roof_orientation',
                                    'roof_color', 'roof_height', 'building_id']
        self.transportation_segment_schema = ['id', 'geometry', 'bbox', 'version', 'sources', 'subtype', 'class',
                                                'names', 'connectors', 'routes', 'subclass', 'subclass_rules',
                                                'access_restrictions', 'level_rules', 'destinations',
                                                'prohibited_transitions', 'road_surface', 'road_flags', 'speed_limits',
                                                'width_rules']
        self.transportation_connector_schema = ['id', 'geometry', 'bbox', 'version', 'sources']
        self.addresses_schema = self.addresses_schema.sort()
        self.places_schema = self.places_schema.sort()
        self.buildings_schema = self.buildings_schema.sort()
        self.buildings_part_schema = self.buildings_part_schema.sort()
        self.transportation_segment_schema = self.transportation_segment_schema.sort()
        self.transportation_connector_schema = self.transportation_connector_schema.sort()
    
    def test_check_release(self):
        with self.assertRaises(ValueError):  
            self.overture_maps.check_release(release=self.releases["invalid_release"])
        try:
            self.overture_maps.check_release(release=self.releases["valid_overture"])
        except ValueError as e:
            self.fail(f"check_release with correct release raised exception {e} unexpectedly!")
    
    def test_overture_addresses(self):
        with patch.object(self.overture_maps, 'overture_addresses_from_bbox', return_value=GeoDataFrame):
            result = self.overture_maps.overture_addresses_from_bbox(
                bbox=self.user_provided_valid["bbox"]
            )
            columns = result.columns.to_list().sort()
            self.assertIsInstance(result, GeoDataFrame)
            self.assertEqual(columns, self.addresses_schema)
    
    # def test_overture_base(self):
    #     with patch.object(self.overture_maps, 'overture_base_from_bbox', return_value=GeoDataFrame): 
    #         result = self.overture_maps.overture_base_from_bbox(
    #             bbox=self.user_provided_valid["bbox"]
    #         )
    #         columns = result.columns.to_list().sort()
    #         self.assertIsInstance(result, GeoDataFrame)
    #         self.assertEqual(columns, self.)
    
    def test_overture_places(self):
        with patch.object(self.overture_maps, 'overture_places_from_bbox', return_value=GeoDataFrame):
            result = self.overture_maps.overture_places_from_bbox(
                bbox=self.user_provided_valid["bbox"]
            )
            columns = result.columns.to_list().sort()
            self.assertIsInstance(result, GeoDataFrame)
            self.assertEqual(columns, self.addresses_schema)
    
    def test_overture_buildings(self):
        # Test building
        with patch.object(self.overture_maps, 'overture_buildings_from_bbox', return_value=GeoDataFrame):
            result = self.overture_maps.overture_buildings_from_bbox(
                bbox=self.user_provided_valid["bbox"]
            )
            columns = result.columns.to_list().sort()
            self.assertIsInstance(result, GeoDataFrame)
            self.assertEqual(columns, self.buildings_schema)
        
        # Test building part
        with patch.object(self.overture_maps, 'overture_buildings_from_bbox', return_value=GeoDataFrame):
            result = self.overture_maps.overture_buildings_from_bbox(
                bbox=self.user_provided_valid["bbox"],
                building_part=True
            )
            columns = result.columns.to_list().sort()
            self.assertIsInstance(result, GeoDataFrame)
            self.assertEqual(columns, self.buildings_part_schema)
    
    def test_overture_transportation(self):
        # Test connector
        with patch.object(self.overture_maps, 'overture_transportation_from_bbox', return_value=GeoDataFrame):
            result = self.overture_maps.overture_transportation_from_bbox(
                bbox=self.user_provided_valid["bbox"]
            )
            columns = result.columns.to_list().sort()
            self.assertIsInstance(result, GeoDataFrame)
            self.assertEqual(columns, self.transportation_segment_schema)
        
        # Test segment
        with patch.object(self.overture_maps, 'overture_transportation_from_bbox', return_value=GeoDataFrame):
            result = self.overture_maps.overture_transportation_from_bbox(
                bbox=self.user_provided_valid["bbox"],
                connector=True
            )
            columns = result.columns.to_list().sort()
            self.assertIsInstance(result, GeoDataFrame)
            self.assertEqual(columns, self.transportation_connector_schema)


if __name__ == '__main__':
    unittest.main()