

filter_expr = ("x","<","y")
filter_expr_multiple = [("x","<","y"),[(),()],()]

#vars for test geoparquet
#vars for test parquet

#throw error when invalid: operator. throw error when invalid singleton
def test_filter_expression():
    pass

#assert none is none
#assert singleton works
#assert list works
#assert & and | works
def test_filter_expression_builder():
    pass


#throw error when: invalid column, invalid value in filter, invalid operator
def test_catch_and_raise_pyarrow():
    pass

#s3 read error(?)
#million assertions
#TODO could have invalid bbox(?)
def test_read_geoparquet():
    pass

#base assertion
def test_read_parquet():
    pass


#incorrect unit
#basic test assertion
def test_unit_conversion():
    pass

#just pass tbh
def test_point_buffer():
    pass

#pass invalid release
#pass valid release
#FOR EACH IMPLEMENTATION
def test_check_release():
    pass

#integration tests

def test_gdf_from_bbox():
    pass

def test_geocode_point_to_bbox():
    pass

def test_geocode_place_to_bbox():
    pass

def test_from_address():
    pass

def test_from_bbox():
    pass

def test_from_place():
    pass
