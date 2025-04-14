from pyplaces.foursquare_open_places import foursquare_places_from_address


# foursquare_places_from_address("204 Hemenway Street",filters=[("incorrect_column",">",0.9)])

# places.overture_addresses_from_address("204 Hemenway Street",filters=[("confidence",">","incorrect_type")])

# df = pyplaces.overture_maps.overture_places_from_address("204 Hemenway Street")

df = foursquare_places_from_address("204 Hemenway Street")

print(df.columns)
print(df.head(10))