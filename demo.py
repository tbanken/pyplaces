from pyplaces import overture_maps as om, foursquare_open_places as fsq

# df=om.overture_base_from_place("Toronto","bathymetry")

# print(df)

# print(fsq.find_categories("waffles"))

# print(om.find_categories("waffles"))

print(om.overture_places_from_place("Jamaica Plain, Boston, MA, USA")["names"])

print(fsq.foursquare_places_from_address("204 Hemenway Street, Boston, MA, USA"))