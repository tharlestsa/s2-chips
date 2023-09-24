# Sentinel-2 Image Extractor

This script efficiently extracts satellite imagery chips from Sentinel-2 datasets based on given geo-coordinates from a GeoJSON file. The script employs `ray` for parallel processing, ensuring fast extraction for multiple coordinates and dates. Each image chip is then saved as a JPEG file.

![Layer Grid Plugin ](./chips.gif)

## Features

- **Parallel Processing**: Utilizes the `ray` library for concurrent extraction.
- **GeoJSON Input**: Extracts chips based on points provided in a GeoJSON file.
- **Dynamic Year Range**: Allows extraction for a defined range of years.
- **Cloud Cover Filter**: Filters out images with cloud cover beyond a set threshold.
- **Sentinel-2 Data**: Extracts images from Sentinel-2 L2A datasets.

## Requirements

- Python 3.x
- ray
- geopandas
- numpy
- osgeo
- loguru
- satsearch
- shapely

## Usage

1. Modify the script's `years` variable to set your desired range of years for extraction.
2. Modify the `geojson_file` path variable to point to your GeoJSON file.
3. Run the script.

```bash
$ python3 chips.py
```

## Troubleshooting

Logging is implemented using the loguru library. Any issues or progress are logged using this. If you encounter issues, refer to the log outputs for more information.


## Contribution & Feedback

For any contributions or feedback, please contact the script maintainer or open an issue on the project's GitHub repository https://github.com/tharlestsa/s2-chips.
