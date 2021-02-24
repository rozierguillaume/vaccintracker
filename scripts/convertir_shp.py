import geopandas as gpd

tmp = gpd.GeoDataFrame.from_file('data/input/epci-shapefile/EPCI_SHAPEFILE.shp')

tmpWGS84 = tmp.to_crs({'proj':'longlat', 'ellps':'WGS84', 'datum':'WGS84'})

tmpWGS84.to_file('data/input/epci-shapefile/EPCI_SHAPEFILE_latlong.shp')