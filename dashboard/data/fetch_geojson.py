import json
import requests

def fetch_and_convert():
    query = """
    [out:json][timeout:90];
    area["boundary"="administrative"]["admin_level"="5"]["name"="Surabaya"]->.a;
    (
      relation["boundary"="administrative"]["admin_level"="6"](area.a);
    );
    out geom;
    """
    print("Fetching data from Overpass API...")
    urls = [
        "https://lz4.overpass-api.de/api/interpreter",
        "https://z.overpass-api.de/api/interpreter",
        "https://overpass.kumi.systems/api/interpreter",
        "https://overpass.nchc.org.tw/api/interpreter",
        "https://overpass-api.de/api/interpreter"
    ]
    
    headers = {
        "User-Agent": "SurabayaKecamatanGeoJSONFetcher/1.0 (contact: teamantigravity@example.com)",
        "Accept": "application/json"
    }
    
    resp = None
    for url in urls:
        print(f"Trying Overpass endpoint: {url}...")
        try:
            # Try GET first
            resp = requests.get(url, params={"data": query}, headers=headers, timeout=30)
            if resp.status_code == 200:
                print(f"Success with {url} (GET)")
                break
            else:
                print(f"Failed {url} (GET) with status {resp.status_code}")
                # Try POST
                resp = requests.post(url, data={"data": query}, headers=headers, timeout=30)
                if resp.status_code == 200:
                    print(f"Success with {url} (POST)")
                    break
                else:
                    print(f"Failed {url} (POST) with status {resp.status_code}")
        except Exception as e:
            print(f"Error connecting to {url}: {e}")
            
    if resp is None or resp.status_code != 200:
        print("All endpoints failed.")
        if resp is not None:
            print("Last response text:", resp.text)
        return
    
    data = resp.json()
    elements = data.get("elements", [])
    print(f"Found {len(elements)} elements.")
    
    features = []
    for el in elements:
        if el.get("type") != "relation":
            continue
        
        tags = el.get("tags", {})
        name = tags.get("name", "")
        print(f"Processing: {name}")
        
        # Stitch outer ways
        outer_ways = []
        for member in el.get("members", []):
            if member.get("role") == "outer" and member.get("type") == "way":
                geom = member.get("geometry", [])
                if geom:
                    coords = [[pt["lon"], pt["lat"]] for pt in geom]
                    outer_ways.append(coords)
        
        if not outer_ways:
            continue
            
        # Stitch ways to form polygon ring(s)
        rings = stitch_ways(outer_ways)
        
        # Build GeoJSON feature
        # If there's only one ring, we can use Polygon. Otherwise, Multipolygon.
        if len(rings) == 1:
            geometry = {
                "type": "Polygon",
                "coordinates": rings
            }
        else:
            geometry = {
                "type": "MultiPolygon",
                "coordinates": [[ring] for ring in rings]
            }
            
        feature = {
            "type": "Feature",
            "properties": {
                "name": name,
                "kecamatan": name,
                "id": el.get("id"),
                "admin_level": tags.get("admin_level")
            },
            "geometry": geometry
        }
        features.append(feature)
        
    geojson = {
        "type": "FeatureCollection",
        "features": features
    }
    
    output_path = r"c:\Users\Asus\Documents\semester 4\BIG DATA\kelompok-1-eas-bigdata\dashboard\data\surabaya.geojson"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(geojson, f, indent=2)
    print(f"Saved to {output_path}")

def stitch_ways(ways):
    # ways is a list of lists of coordinates: [ [[lon1, lat1], [lon2, lat2], ...], ... ]
    # We want to combine them into closed rings.
    unused = list(ways)
    rings = []
    
    while unused:
        # Start a new ring
        current_way = unused.pop(0)
        ring = list(current_way)
        
        # Keep finding connecting ways until the ring is closed or no more connections can be made
        changed = True
        while changed:
            changed = False
            start_pt = ring[0]
            end_pt = ring[-1]
            
            # If closed, we can stop
            if abs(start_pt[0] - end_pt[0]) < 1e-7 and abs(start_pt[1] - end_pt[1]) < 1e-7:
                break
                
            for i, way in enumerate(unused):
                w_start = way[0]
                w_end = way[-1]
                
                # Check end_pt connections
                if abs(end_pt[0] - w_start[0]) < 1e-7 and abs(end_pt[1] - w_start[1]) < 1e-7:
                    ring.extend(way[1:])
                    unused.pop(i)
                    changed = True
                    break
                elif abs(end_pt[0] - w_end[0]) < 1e-7 and abs(end_pt[1] - w_end[1]) < 1e-7:
                    ring.extend(reversed(way[:-1]))
                    unused.pop(i)
                    changed = True
                    break
                # Check start_pt connections
                elif abs(start_pt[0] - w_end[0]) < 1e-7 and abs(start_pt[1] - w_end[1]) < 1e-7:
                    ring = way[:-1] + ring
                    unused.pop(i)
                    changed = True
                    break
                elif abs(start_pt[0] - w_start[0]) < 1e-7 and abs(start_pt[1] - w_start[1]) < 1e-7:
                    ring = list(reversed(way[1:])) + ring
                    unused.pop(i)
                    changed = True
                    break
        
        # Force close if not closed
        if abs(ring[0][0] - ring[-1][0]) > 1e-7 or abs(ring[0][1] - ring[-1][1]) > 1e-7:
            ring.append(ring[0])
            
        rings.append(ring)
        
    return rings

if __name__ == "__main__":
    fetch_and_convert()
