import os
import ray
import tempfile
import numpy as np
import geopandas as gpd
from datetime import datetime
from satsearch import Search
from shapely.geometry import shape, mapping
from osgeo import gdal, osr
from loguru import logger

BANDS= ['swir16', 'nir08', 'red']

ray.init()

@ray.remote
def worker(id, year, geometry, lon, lat):
    for month in range(1, 13):
        process_month(id, month, year, geometry, lon, lat)

class Constants:
    STAC_API_URL = 'https://earth-search.aws.element84.com/v1'
    COLLECTION = 'sentinel-2-l2a'
    CLOUD_COVER_LIMIT = 5

@logger.catch
def get_scale_params(ds):
    band_values = []

    # Loop through each band in the dataset
    for band_number in range(1, ds.RasterCount + 1):  # GDAL band numbering starts from 1
        band = ds.GetRasterBand(band_number)
        
        # Fetch raster data as numpy array and flatten
        raster_data = band.ReadAsArray()
        flat_data = raster_data.flatten()
        
        # Compute 2nd and 98th percentiles
        min_val = np.percentile(flat_data, 2)
        max_val = np.percentile(flat_data, 98)

        band_values.append([min_val, max_val, 1, 255])

    return band_values


@logger.catch
def extract_chip(vrt_path, lon, lat, output_cog, buffer=4000, width=256, height=256, gamma=1.3):
    ds = gdal.Open(vrt_path)

    # Extract the dataset's projection
    inSpatialRef = osr.SpatialReference()
    inSpatialRef.ImportFromWkt(ds.GetProjectionRef())

    # Define the input coordinates CRS (EPSG:4623 - JGD2000)
    outSpatialRef = osr.SpatialReference()
    outSpatialRef.ImportFromEPSG(4326)

    # Set up the transformation
    transform = osr.CoordinateTransformation(outSpatialRef, inSpatialRef)
    x, y, _ = transform.TransformPoint(lat, lon)
    # ulx = x - buffer
    # uly = y + buffer
    # lrx = x + buffer
    # lry = y - buffer

    x_res = buffer / (width / 2)  # or buffer*2 / width
    y_res = buffer / (height / 2) # or buffer*2 / height

    half_width = width * x_res / 2
    half_height = height * y_res / 2
    ulx = x - half_width
    uly = y + half_height
    lrx = x + half_width
    lry = y - half_height

    logger.info(f"VRT: {vrt_path} | {lon} {lat} | BBOX: {ulx} {uly} {lrx} {lry}")

    options = gdal.TranslateOptions(format='JPEG',
                                    xRes=x_res,
                                    yRes=y_res,
                                    outputType=gdal.GDT_Byte,
                                    creationOptions=['WORLDFILE=YES'],
                                    scaleParams=[[600, 5400, 1, 255], [700, 4300, 1, 255], [400, 2800, 1, 255]],
                                    projWin=[ulx, uly, lrx, lry],
                                    exponents=[gamma, gamma, gamma])

    gdal.Translate(output_cog, ds, options=options)

    ds = None

@logger.catch
def create_vrt(urls, output_name, lon, lat):
    separate = True
    if len(urls) == 1:
        separate = False

    tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.vrt')
    vrt_path = tmp_file.name
    tmp_file.close()

    options = gdal.BuildVRTOptions(separate=separate)
    gdal.BuildVRT(vrt_path, urls, options=options)

    # Create the chip PNG for the given point
    output_png = f'{output_name}.jpg'
    logger.info(f"create_vrt: {output_png}")
    extract_chip(vrt_path, lon, lat, output_png)

@logger.catch
def process_month(id, month, year, geometry, lon, lat):
    start_date = datetime(year, month, 1, 0, 0, 0).isoformat() + 'Z'
    end_date = (
        datetime(year, month + 1, 1, 0, 0, 0)
        if month < 12
        else datetime(year + 1, 1, 1, 0, 0, 0)
    ).isoformat() + 'Z'

    date_range = f'{start_date}/{end_date}'
    
    #time.sleep(2)

    search = Search(
        url=Constants.STAC_API_URL,
        intersects=mapping(geometry),
        datetime=date_range,
        collections=[Constants.COLLECTION],
        query={'eo:cloud_cover': {'lt': Constants.CLOUD_COVER_LIMIT}},
        limit=1000,
    )

    items = sorted(
        search.items(), key=lambda item: item.properties['eo:cloud_cover']
    )

    #logger.info(f"{items}")

    if items:
        item = items[0]
        urls = [
            f"/vsicurl/{item.assets[b]['href']}"
            for b in BANDS
        ]

        composite_name = f'{id}_S2_L2A_{year}_{month:02}'.upper()

        create_vrt(urls, composite_name, lon, lat)

@logger.catch
def execute(year_range, geojson_file):
    gdf = gpd.read_file(geojson_file)

    # Take only the first two rows
    gdf = gdf.iloc[:2]

    logger.info(f"CRS: {gdf.crs}")

    tasks = []
    for _, row in gdf.iterrows():
        geometry = row['geometry']
        id = row['ID']
        geom = shape(geometry)
        if geom.type != 'Point':
            logger.error('Geometry is not a Point.')
            continue

        lon, lat = geom.x, geom.y
        for year in year_range:
            tasks.append(worker.remote(id, year, geom, lon, lat))

    # Wait for all tasks to complete
    ray.get(tasks)

    ray.shutdown()  # Shutdown Ray

try:
    years = range(2019, 2023)  # Example year range
    geojson_file = os.path.abspath(
        './pastagem_50k_sentinel.geojson'
    )  # Replace with your geojson file path
    execute(years, geojson_file)
except Exception as e:
    logger.exception(e)
    pass
