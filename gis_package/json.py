import requests
import geopandas as gpd
from shapely.geometry import shape, Point, Polygon, MultiPolygon
import pandas as pd

def download_feature_layer(url, chunk_size, where="1=1", source_name=None):
    """
    Download all features from an ArcGIS FeatureServer layer with pagination.
    Handles both GeoJSON and EsriJSON geometry.
    """
    features = []
    offset = 0

    while True:
        params = {
            'where': where,
            'outFields': '*',
            'f': 'geojson',  # request geojson, but ArcGIS may fall back to EsriJSON
            'resultOffset': offset,
            'resultRecordCount': chunk_size
        }
        r = requests.get(url, params=params)
        r.raise_for_status()
        data = r.json()
        feats = data.get('features', [])
        if not feats:
            break
        features.extend(feats)
        offset += chunk_size
        print(f"Downloaded {len(features)} features...")

    print(f"Finished downloading {len(features)} features")

    records = []
    for f in features:
        geom = None
        if "geometry" in f and f["geometry"]:
            g = f["geometry"]

            try:
                # Case 1: already GeoJSON-like
                geom = shape(g)
            except Exception:
                # Case 2: Esri JSON (has x/y or rings)
                if "x" in g and "y" in g:
                    geom = Point(g["x"], g["y"])
                elif "rings" in g:
                    try:
                        geom = Polygon(g["rings"][0])
                    except Exception:
                        geom = MultiPolygon([Polygon(r) for r in g["rings"]])
                elif "paths" in g:  # polyline
                    from shapely.geometry import LineString
                    geom = LineString(g["paths"][0])

        props = f.get("properties", f.get("attributes", {})) or {}
        rec = {"geometry": geom, **props}
        records.append(rec)

    if any(r["geometry"] is not None for r in records):
        gdf = gpd.GeoDataFrame(records, geometry="geometry", crs="EPSG:4326")
    else:
        gdf = pd.DataFrame(records)

    return gdf

