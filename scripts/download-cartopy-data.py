import os

import cartopy.io.shapereader as shpreader
import certifi

# Set environment variable
os.environ["SSL_CERT_FILE"] = certifi.where()

# List of shapefiles to ensure are downloaded
shapefiles = [
    ("physical", "50m", "coastline"),
    ("physical", "50m", "lakes"),
    ("physical", "50m", "land"),
    ("physical", "50m", "ocean"),
    ("physical", "50m", "rivers_lake_centerlines"),
    ("cultural", "50m", "admin_0_countries"),
    ("cultural", "50m", "admin_0_boundary_lines_land"),
    ("cultural", "50m", "admin_1_states_provinces"),
    ("cultural", "50m", "admin_1_states_provinces_lakes"),
    ("physical", "110m", "coastline"),
    ("physical", "110m", "lakes"),
    ("physical", "110m", "land"),
    ("physical", "110m", "ocean"),
    ("physical", "110m", "rivers_lake_centerlines"),
    ("cultural", "110m", "admin_0_countries"),
    ("cultural", "110m", "admin_0_boundary_lines_land"),
    ("cultural", "110m", "admin_1_states_provinces"),
    ("cultural", "110m", "admin_1_states_provinces_lakes"),
]

for category, resolution, name in shapefiles:
    print(f"Downloading {category}/{resolution}/{name}")
    shpreader.natural_earth(resolution=resolution, category=category, name=name)
