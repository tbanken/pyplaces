import pytest
from pyplaces import foursquare_open_places as fsq, overture_maps as ov
from pyplaces._errors import PyArrowError,UnsupportedOperatorError


user_optional_valid = {"columns":["confidence","names"],"filters":("confidence",">",0.9),
                    "distance":1,"unit":"km"}
user_provided_valid = {"address":"204 Hemenway Street","place":"Jamaica Plain, MA","bbox":(-71.11088398563271, 42.32145496739517, -71.0987557723746, 42.33045192410487)}
user_optional_invalid = {"columns":["sfdsfd"],"filters_invalid_op":("confidence","(",0.9),"filters_invalid_field":("fdscds","<",0.9),
            "filters_invalid_value":("confidence", "<", "a"),"unit":"sfd","release":"2025-10-23"}
user_provided_invalid = {"address":"fdlksjfld","bbox":(-181,-91, 181, 91),"point":(-181, -91)}

def test_foursquare_places():
    
    places_schema = ['fsq_place_id', 'name', 'latitude', 'longitude', 'address', 'locality',
                    'region', 'postcode', 'admin_region', 'post_town', 'po_box', 'country',
                    'date_created', 'date_refreshed', 'date_closed', 'tel', 'website',
                    'email', 'facebook_id', 'instagram', 'twitter', 'fsq_category_ids',
                    'fsq_category_labels', 'placemaker_url', 'geom', 'bbox']
    places_schema.sort()
    categories_schema = ["category_id","category_level","category_name","category_label","level1_category_id","level1_category_name",
                            "level2_category_id","level2_category_name","level3_category_id","level3_category_name","level4_category_id",
                            "level4_category_name","level5_category_id","level5_category_name","level6_category_id","level6_category_name"]
    categories_schema.sort()
    
    #check that categories schema is correct
    
    with pytest.raises(ValueError):
        #assert invalid release
        fsq.foursquare_places_from_address(user_provided_valid["address"],release=user_optional_invalid["release"])
        #assert invalid unit
        fsq.foursquare_places_from_address(user_provided_valid["address"],unit=user_optional_invalid["unit"])
        
        
        #TODO not sure about these
        #assert invalid point
        fsq.foursquare_places_from_address(user_optional_invalid["point"])
        #assert invalid address
        fsq.foursquare_places_from_place(user_provided_invalid["address"])
        #assert invalid bbox
        fsq.foursquare_places_from_bbox(user_provided_invalid["bbox"])
    with pytest.raises(PyArrowError):
        #assert invalid column
        fsq.foursquare_places_from_address(user_provided_valid["address"],user_optional_invalid["columns"])
        #assert invalid field
        fsq.foursquare_places_from_address(user_provided_valid["address"],filters=user_optional_invalid["filters_invalid_field"])
        #assert invalid value
        fsq.foursquare_places_from_address(user_provided_valid["address"],filters=user_optional_invalid["filters_invalid_value"])
    with pytest.raises(UnsupportedOperatorError):
        #assert invalid operator
        fsq.foursquare_places_from_address(user_provided_valid["address"],filters=user_optional_invalid["filters_invalid_op"])

    #assert correct case, all valid inputs. check that schema is correct for each
    cols1=fsq.foursquare_places_from_address(user_provided_valid["address"]).columns.to_list()
    cols2 = fsq.foursquare_places_from_place(user_provided_valid["place"]).columns.to_list()
    cols3 = fsq.foursquare_places_from_bbox(user_provided_valid["bbox"]).columns.to_list()
    cols4 = fsq.get_categories().columns.to_list()
    cols1.sort()
    cols2.sort()
    cols3.sort()
    cols4.sort()
    assert cols1 == places_schema
    assert cols2 == places_schema
    assert cols3 == places_schema
    assert cols4 == categories_schema


def test_overture_maps():
    
    places_schema = ['id', 'geometry', 'bbox', 'version', 'sources', 'names', 'categories',
                            'confidence', 'websites', 'socials', 'emails', 'phones', 'brand','addresses']
    addresses_schema = ['id', 'geometry', 'bbox', 'country', 'postcode', 'street', 'number',
                        'unit', 'address_levels', 'postal_city', 'version', 'sources']
    buildings_schema = ['id', 'geometry', 'bbox', 'version', 'sources', 'level', 'subtype',
                            'class', 'height', 'names', 'has_parts', 'is_underground', 'num_floors',
                            'num_floors_underground', 'min_height', 'min_floor', 'facade_color',
                            'facade_material', 'roof_material', 'roof_shape', 'roof_direction',
                            'roof_orientation', 'roof_color', 'roof_height']
    buildings_part_schema = ['id', 'geometry', 'bbox', 'version', 'sources', 'level', 'height',
                                'names', 'is_underground', 'num_floors', 'num_floors_underground',
                                'min_height', 'min_floor', 'facade_color', 'facade_material',
                                'roof_material', 'roof_shape', 'roof_direction', 'roof_orientation',
                                'roof_color', 'roof_height', 'building_id']
    transportation_segment_schema = ['id', 'geometry', 'bbox', 'version', 'sources', 'subtype', 'class',
                                            'names', 'connectors', 'routes', 'subclass', 'subclass_rules',
                                            'access_restrictions', 'level_rules', 'destinations',
                                            'prohibited_transitions', 'road_surface', 'road_flags', 'speed_limits',
                                            'width_rules']
    transportation_connector_schema = ['id', 'geometry', 'bbox', 'version', 'sources']
    addresses_schema.sort()
    places_schema.sort()
    buildings_schema.sort()
    buildings_part_schema.sort()
    transportation_segment_schema.sort()
    transportation_connector_schema.sort()
    
    #assert correct schema for all of these
    cols1 = ov.overture_addresses_from_bbox(user_provided_valid["bbox"]).columns.to_list()
    # ov.overture_base_from_bbox(user_provided_valid["bbox"])
    cols2 = ov.overture_places_from_bbox(user_provided_valid["bbox"]).columns.to_list()
    #building and building part
    cols3 = ov.overture_buildings_from_bbox(user_provided_valid["bbox"]).columns.to_list()
    cols4 = ov.overture_buildings_from_bbox(user_provided_valid["bbox"],building_part=True).columns.to_list()
    #connector and segment
    cols5 = ov.overture_transportation_from_bbox(user_provided_valid["bbox"]).columns.to_list()
    cols6 = ov.overture_transportation_from_bbox(user_provided_valid["bbox"],connector=True).columns.to_list()
    
    cols1.sort()
    cols2.sort()
    cols3.sort()
    cols4.sort()
    cols5.sort()
    cols6.sort()
    assert cols1 == addresses_schema
    assert cols2 == places_schema
    assert cols3 == buildings_schema
    assert cols4 == buildings_part_schema
    assert cols5 == transportation_segment_schema
    assert cols6 == transportation_connector_schema