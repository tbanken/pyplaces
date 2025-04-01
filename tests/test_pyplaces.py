from pyplaces import FoursquareOpenPlaces, OvertureMaps

overture_maps = OvertureMaps()

fsq_places = FoursquareOpenPlaces()

# hardcoded = {"prefix":"","main_path":"","region":""}
user_optional_valid = {"columns_overture":["confidence"],"filters":("confidence","<",0.9),"distance":1000,"unit":"km"}
user_provided_valid = {"address":"204 Hemenway Street","bbox":(-71.11088398563271, 42.32145496739517, -71.0987557723746, 42.33045192410487)}
user_optional_invalid = {"columns":["sfdsfd"],"filters_invalid_op":("confidence","(",0.9),"unit":"sfd"}
user_provided_invalid = {"address":"fdlksjfld","bbox":(-181,-91, 181, 91)}

def test_foursquare_places():
    fsq_places.check_release() #incorrect release
    fsq_places.check_release() #correct release
    
    fsq_places.get_categories() #check that categories schema is correct
    
    #assert invalid column
    fsq_places.foursquare_places_from_address()
    #assert invalid operator
    fsq_places.foursquare_places_from_address()
    #assert invalid unit
    fsq_places.foursquare_places_from_address()
    #assert invalid point
    fsq_places.foursquare_places_from_address()
    #assert invalid address
    fsq_places.foursquare_places_from_place()
    #assert invalid bbox
    fsq_places.foursquare_places_from_bbox()
    
    
    #assert correct case, all valid inputs
    fsq_places.foursquare_places_from_address()
    fsq_places.foursquare_places_from_place()
    fsq_places.foursquare_places_from_bbox()


def test_overture_maps():
    overture_maps.check_release() #incorrect release
    overture_maps.check_release() #correct release
    
    #assert correct schema
    overture_maps.overture_addresses_from_bbox()
    overture_maps.overture_base_from_bbox()
    overture_maps.overture_places_from_bbox()
    #building and building part
    overture_maps.overture_buildings_from_bbox()
    overture_maps.overture_buildings_from_bbox()
    #connector and segment
    overture_maps.overture_transportation_from_bbox()
    overture_maps.overture_transportation_from_bbox()