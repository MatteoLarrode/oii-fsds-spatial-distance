import pandas as pd
import geopandas as gpd
import requests
from shapely.geometry import Point
import re
import os
import matplotlib.pyplot as plt

def get_disambiguation_content(city_name):
    """
    Fetch the content of the city's disambiguation page using the Wikipedia API
    """
    base_url = "https://en.wikipedia.org/w/api.php"
    
    # First, get the wiki text content
    params = {
        "action": "parse",
        "page": city_name,
        "prop": "wikitext",
        "format": "json"
    }
    
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Raise exception for bad status codes
        data = response.json()
        
        if 'parse' in data and 'wikitext' in data['parse']:
            return data['parse']['wikitext']['*']
        else:
            raise ValueError("Unexpected API response structure")
            
    except requests.RequestException as e:
        print(f"Error fetching disambiguation page: {e}")
        return None

def parse_us_cities_from_wikitext(content):
    """
    Parse US locations of the city from the Wikipedia wikitext content.
    Returns a list of locations of the city with their state information.
    """
    cities = []
    in_us_section = False
    
    # Split content into lines
    lines = content.split('\n')
    
    for line in lines:
        # Check for US section start
        if "=== United States ===" in line:
            in_us_section = True
            continue
        # Check for next section (end of US section)
        elif in_us_section and line.startswith('==='):
            break
        
        # Process lines in US section
        if in_us_section and line.strip().startswith('*'):
            # Clean up the line and extract the link
            match = re.search(r'\[\[([^\]]+)\]\]', line)
            if match:
                entry = match.group(1)
                
                # Skip entries we don't want
                # if any(skip in entry for skip in ['metropolitan area', 'Township', 'CDP', 'disambiguation']):
                    # continue
                
                # Handle cases where link text differs from display text
                if '|' in entry:
                    entry = entry.split('|')[0]
                
                # Extract city and state
                if ',' in entry:
                    location, state_info = entry.split(',', 1)
                    state_info = state_info.strip()
                    
                    # Handle cases with additional info in parentheses
                    if '(' in state_info:
                        state_info = state_info.split('(')[0].strip()
                    
                    cities.append({
                        'title': entry,
                        'city': location,
                        'state': state_info
                    })
    
    return cities

def get_coordinates_batch(cities):
    """
    Get coordinates for a list of city locations using batch requests.
    Handles both coordinate properties and coordinate templates.
    """
    base_url = "https://en.wikipedia.org/w/api.php"
    results = []
    
    # Process in batches of 50
    batch_size = 50
    for i in range(0, len(cities), batch_size):
        batch = cities[i:i + batch_size]
        titles = [item['title'] for item in batch]
        
        # First try to get coordinates from properties
        params = {
            "action": "query",
            "titles": "|".join(titles),
            "prop": "coordinates|info|revisions",
            "inprop": "url",
            "rvprop": "content",
            "format": "json"
        }
        
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if 'query' in data and 'pages' in data['query']:
                for page_id, page in data['query']['pages'].items():
                    original = next((s for s in batch if s['title'] == page['title']), None)
                    if not original:
                        continue
                        
                    coordinates = None
                    
                    # Try to get coordinates from properties first
                    if 'coordinates' in page:
                        coords = page['coordinates'][0]
                        coordinates = {
                            'lat': coords['lat'],
                            'lon': coords['lon']
                        }
                    
                    # If no coordinates in properties, try to parse from content
                    elif 'revisions' in page and page['revisions']:
                        content = page['revisions'][0]['*']
                        coord_match = re.search(r'{{coord\|([^|}]+)\|([^|}]+)\|([^|}]+)\|([^|}]+)\|([^|}]+)\|([^|}]+)\|([^|}]+)\|([^|}]+)', content, re.IGNORECASE)
                        
                        if coord_match:
                            lat_deg, lat_min, lat_sec, lat_dir, lon_deg, lon_min, lon_sec, lon_dir = coord_match.groups()
                            try:
                                lat = float(lat_deg) + float(lat_min)/60 + float(lat_sec)/3600
                                lon = float(lon_deg) + float(lon_min)/60 + float(lon_sec)/3600
                                
                                if lat_dir.upper() == 'S':
                                    lat = -lat
                                if lon_dir.upper() == 'W':
                                    lon = -lon
                                    
                                coordinates = {
                                    'lat': lat,
                                    'lon': lon
                                }
                            except ValueError:
                                continue
                    
                    if coordinates:
                        results.append({
                            'title': page['title'],
                            'city': original['city'],
                            'state': original['state'],
                            'latitude': coordinates['lat'],
                            'longitude': coordinates['lon'],
                            'url': page.get('canonicalurl', '')
                        })
                            
        except requests.RequestException as e:
            print(f"Error in batch coordinate request: {e}")
            continue
    
    return pd.DataFrame(results)

def create_cities_geodataframe(city_name):
    """
    Main function to create a GeoDataFrame of US locations of a city
    """
    # Get disambiguation page content
    print(f"Fetching {city_name}'s disambiguation page...")
    content = get_disambiguation_content(city_name)
    
    if content is None:
        raise ValueError("Failed to fetch disambiguation page content")
    
    # Parse US cities
    print(f"Parsing {city_name} locations...")
    cities = parse_us_cities_from_wikitext(content)
    print(f"Found {len(cities)} {city_name} locations in US")
    
    # Get coordinates
    print("Fetching coordinates...")
    cities_df = get_coordinates_batch(cities)
    print(f"Successfully retrieved coordinates for {len(cities_df)} locations")
    
    # Create GeoDataFrame
    geometry = [Point(xy) for xy in zip(cities_df['longitude'], cities_df['latitude'])]
    cities_gdf = gpd.GeoDataFrame(cities_df, geometry=geometry, crs="EPSG:4326")
    
    return cities_gdf

def join_cities_to_states(cities_gdf):
    """
    Perform spatial join between cities and US states shapefile,
    handling column name conflicts properly
    """
    # Download and prepare US states shapefile
    url = "https://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_state_20m.zip"
    
    if not os.path.exists('data'):
        os.makedirs('data')
    
    zip_path = "data/us_states.zip"
    if not os.path.exists(zip_path):
        print("Downloading US states data...")
        response = requests.get(url)
        with open(zip_path, 'wb') as f:
            f.write(response.content)
    
    # Read states shapefile
    states_gdf = gpd.read_file(f"zip://{zip_path}")
    states_gdf = states_gdf.to_crs(cities_gdf.crs)
    
    # Filter out Alaska, Hawaii and Puerto Rico
    states_gdf = states_gdf[~states_gdf['STUSPS'].isin(['AK', 'HI', 'PR'])]

    # Rename columns in states_gdf to avoid conflicts
    states_gdf = states_gdf.rename(columns={
        'NAME': 'state_name',
        'STUSPS': 'state_abbrev'
    })
    
    # Before joining, rename original state column if it exists
    if 'state' in cities_gdf.columns:
        cities_gdf = cities_gdf.rename(columns={'state': 'state_from_wiki'})
    
    # Perform spatial join
    joined = gpd.sjoin(
        cities_gdf,
        states_gdf[['state_name', 'state_abbrev', 'geometry']],
        how='left',
        predicate='within'
    )
    
    # Validate and clean up the results
    joined['state_match'] = joined.apply(
        lambda x: x['state_from_wiki'].strip() == x['state_name'].strip() 
        if pd.notnull(x['state_from_wiki']) and pd.notnull(x['state_name']) 
        else False,
        axis=1
    )
    
    # Print any mismatches for verification
    mismatches = joined[~joined['state_match']]
    if not mismatches.empty:
        print("\nFound state name mismatches:")
        print(mismatches[['title', 'state_from_wiki', 'state_name', 'state_abbrev']])
    
    return joined

def plot_cities(joined_gdf, city_name, save_path=None):
    """
    Create a map of the specified city with state highlighting
    """
    # Download states shapefile if not done already
    url = "https://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_state_20m.zip"
    zip_path = "data/us_states.zip"
    states_gdf = gpd.read_file(f"zip://{zip_path}")

    # Remove Alaska, Hawaii and Puerto Rico
    states_gdf = states_gdf[~states_gdf['STUSPS'].isin(['AK', 'HI', 'PR'])]

    # Create figure and axis
    fig, ax = plt.subplots(figsize=(15, 10))
    
    # Plot all states in light gray
    states_gdf.plot(ax=ax, color='lightgray', edgecolor='white')
    
    # Get unique states that have the city
    states_with_city = joined_gdf['state_name'].unique()
    
    # Highlight states with Springfields    
    states_gdf[states_gdf['NAME'].isin(states_with_city)].plot(
        ax=ax, color='lightblue', edgecolor='white'
    )
    
    # Plot city points
    joined_gdf.plot(ax=ax, color='red', markersize=50)
    
    # Add labels for each Springfield
    for idx, row in joined_gdf.iterrows():
        ax.annotate(
            f"{row['city']}, {row['state_abbrev']}",
            xy=(row.geometry.x, row.geometry.y),
            xytext=(5, 5),
            textcoords="offset points",
            fontsize=8,
            bbox=dict(facecolor='white', edgecolor='none', alpha=0.7)
        )
    
    ax.set_title(f"US Cities Named {city_name}", fontsize=14)
    plt.axis('off')
    
    if save_path:
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        print(f"Map saved to {save_path}")
    
    return fig, ax

def analyze_cities_distribution(joined_gdf):
    """
    Analyze the distribution of the cities across states
    """
    # Count cities per state
    state_counts = joined_gdf.groupby(['state_name', 'state_abbrev']).size().reset_index(name='count')
    state_counts = state_counts.sort_values('count', ascending=False)
    
    print("\City Distribution by State:")
    print(state_counts)
    
    return state_counts

