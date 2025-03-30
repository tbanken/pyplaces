from pyplaces import PyPlaces


places = PyPlaces()


# places.overture_addresses_from_address("204 Hemenway Street",filters=[("incorrect_column",">",0.9)])

# places.overture_addresses_from_address("204 Hemenway Street",filters=[("confidence",">","incorrect_type")])

places.overture_addresses_from_address("204 Hemenway Street",filters=("confidence","incorrect_op",0.9))

# df = 


# print(len(df))
# print(df.head(10))