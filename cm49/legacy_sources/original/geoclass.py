
import requests
import json
import re
import os
from datetime import datetime
from slugify import slugify as pyslugify  # Use python-slugify
from concurrent.futures import ThreadPoolExecutor, as_completed

def safe_get(data, keys, default=None):
    """
    Safely get a value from a nested dictionary or list.
    Keys can be a list of indices/keys or a dot-separated string.
    e.g., safe_get(data, '6.174.0') or safe_get(data, [6, 174, 0])
    """
    if isinstance(keys, str): keys = keys.split('.')
    
    current = data
    
    try:
        for key in keys:
            if isinstance(current, list):
                key = int(key)
                if key >= len(current):
                    return default
                current = current[key]
            elif isinstance(current, dict):
                current = current.get(key)
            else:  # If not list or dict at this level, cannot proceed
                 return default
            if current is None:  # Stop if key not found in dict
                return default
        return current
    except (KeyError, IndexError, ValueError, TypeError):
        return default

def contains_at_least_one_number(s):
    """Checks if a string contains at least one digit."""
    if not isinstance(s, str):
        return False
    return any(char.isdigit() for char in s)

# Cache for countries data
_countries_cache = None

def get_country_from_iso(iso_code=None):
    """Gets country name from ISO code using countries.json."""
    global _countries_cache
    if not iso_code:
        return None

    if _countries_cache is None:
        try:
            # Assumes countries.json is in the same directory as this script
            script_dir = os.path.dirname(__file__)
            json_path = os.path.join(script_dir, 'countries.json')
            with open(json_path, 'r', encoding='utf-8') as f:
                _countries_cache = json.load(f)
        except FileNotFoundError:
            # print("Error: countries.json not found.") # Less verbose for library use
            _countries_cache = []  # Avoid repeated attempts
            return None
        except json.JSONDecodeError:
            # print("Error: Could not decode countries.json.")
            _countries_cache = []
            return None

    iso_upper = iso_code.upper()
    for country in _countries_cache:
        if country.get('alpha-2') == iso_upper:
            return country.get('name')
    return None  # Not found

def make_geocode(address, zipcode, country_code):
    """Tries to create a geocode string like 'zipcode-lastword-number'."""
    if not address or not zipcode:
        return None

    zipcode_clean = str(zipcode).replace(' ', '').strip()
    address_clean = str(address).replace('-', ' ')
    words = address_clean.split()

    if not words:
        return None

    number = None
    number_suffix = ""  # For bis, ter etc.

    # Try first word
    if contains_at_least_one_number(words[0]):
        number = words[0]
        words.pop(0)
    # Try last word if first failed
    elif len(words) > 0 and contains_at_least_one_number(words[-1]):
        number = words[-1]
        words.pop(-1)
    # Try second to last word if last also failed
    elif len(words) > 1 and contains_at_least_one_number(words[-2]):
         number = words[-2]
         words.pop(-2)
    # Fallback: Regex for any number in the address
    else:
        match = re.search(r'\b(\d+[a-zA-Z]*)\b', address_clean)  # Find number, possibly with letters attached
        if match:
            number = match.group(1)
            # Attempt to remove the found number and surrounding space from words list
            temp_addr = address_clean.replace(number, " ", 1).strip()
            words = temp_addr.split()

    if not number or not words:
        # If still no number or no remaining words for street name, try again
        if not words and number:  # We have a number but no street? Unlikely but possible
             return None  # Cannot determine street
        elif not number and words:  # We have street words but no number?
             pass  # Continue, maybe we can use the last word anyway
        else:  # No number AND no words remaining after extraction? Bail out.
            return None

    # Extract potential suffix (bis, ter) if attached to number
    match_suffix = re.match(r'^(\d+)(bis|ter|quater|quinquies|sexies)$', number, re.IGNORECASE)
    if match_suffix:
        number = match_suffix.group(1)
        number_suffix = match_suffix.group(2).lower()
    else:
         # Check original address string for suffixes like '1 bis', '1 ter' near the number
         addr_slug = pyslugify(address_clean, separator=' ')
         num_patterns = {
             "bis": f"{number} bis",
             "ter": f"{number} ter",
             "quater": f"{number} quater",
             "quinquies": f"{number} quinquies",
             "sexies": f"{number} sexies",
         }
         for suffix, pattern in num_patterns.items():
             if pattern in addr_slug:
                 number_suffix = suffix
                 break

    # Use the last significant word of the remaining address as the street identifier
    last_word = words[-1] if words else "unknown"  # Default if no words left

    # Combine
    geocode_parts = [zipcode_clean, pyslugify(last_word)]
    num_part = number
    if number_suffix:
        num_part += number_suffix
    geocode_parts.append(pyslugify(num_part))  # Slugify the number part too

    return "-".join(filter(None, geocode_parts))

def make_geoid(zipcode, city):
    """Creates a Geo ID based on zipcode and city."""
    city_slug = pyslugify(str(city)) if city else 'nocity'
    last_word = city_slug.split('-')[-1] if '-' in city_slug else city_slug

    zipcode_str = str(zipcode) if zipcode else '00000'

    search_part = zipcode_str[:3]  # First 3 digits normally

    return pyslugify(f"{search_part} {last_word}")

def extract_associated(data):
    """Extracts 'associated' places data."""
    pois = {}
    if not isinstance(data, list):
        return {}
    for d in data:
        hexa_assoc = safe_get(d, '0')
        activities_list = safe_get(d, '1.13', [])
        activities_str = ""
        if isinstance(activities_list, list):
             activities_str = ','.join(activities_list)
        elif isinstance(activities_list, str):
             activities_str = activities_list

        if hexa_assoc:
            pois[hexa_assoc] = {
                'name': safe_get(d, '1.11'),
                'activities': activities_str,
            }
    return pois

def parse_schedule(rows):
    """Parses the schedule array."""
    if not isinstance(rows, list):
        return {}

    schedule_list = []
    day_map = {
        'lundi': 1, 'monday': 1,
        'mardi': 2, 'tuesday': 2,
        'mercredi': 3, 'wednesday': 3,
        'jeudi': 4, 'thursday': 4,
        'vendredi': 5, 'friday': 5,
        'samedi': 6, 'saturday': 6,
        'dimanche': 7, 'sunday': 7,
    }

    for row in rows:
        if not isinstance(row, list) or len(row) < 2:
            continue
        day_name = str(safe_get(row, '0', '')).lower()
        schedules = safe_get(row, '1')
        order = day_map.get(day_name)

        if order is not None:
            schedule_list.append({'day': day_name, 'schedules': schedules, 'order': order})

    schedule_list.sort(key=lambda x: x['order'])
    return {item['day']: item['schedules'] for item in schedule_list}

def extract_data(json_string, input_hexa, is_etab, hl, isHexa):
    """Extracts data from the cleaned JSON string."""
    if not json_string: # Should be caught by get_data, but as a safeguard
        return {"error": "Empty JSON string received in extract_data"}

    data_str = json_string.replace("\n", "").replace("\r", "").replace("\t", "")
    if data_str.startswith(")]}'"):
        data_str = data_str[4:]

    try:
        data = json.loads(data_str)
    except json.JSONDecodeError as e:
        return {"error": f"Failed to decode JSON: {e}", "raw_data_prefix": data_str[:200]}

    # Logic for selecting the main data portion 'r'
    if isHexa: # True if original hexa (or recursed hexa) started with '0x'
        r = safe_get(data, '6')
    else: # Original hexa was a query string
        r = safe_get(data, '0.1.0.14') # Try to find a place reference in query results
        
        if r is None: # Path '0.1.0.14' not found or was None
            r = safe_get(data, '37') # Alternative path for query results
            if r is not None: # Path '37' found
                # The value at data['37']['2']['0']['2'] is expected to be a new hexa or query string
                new_identifier = safe_get(data, '37.2.0.2') # Path from root `data`
                return get_data(new_identifier, hl, is_etab) # Recursive call with the new identifier
            else: # All attempts for query processing failed to find a new identifier
                return {"error": "Query: Could not find data block (paths '0.1.0.14' or '37' failed)", "data_structure_summary": str(data)[:200]}
        else: # Path '0.1.0.14' found from query
            # Extract the hexa ('10') from this place entry and recurse
            rowHexa = safe_get(r, '10') 
            return get_data(rowHexa, hl, is_etab) # Recursive call with this hexa

    # If 'r' is still None at this point (e.g. isHexa was True, but data['6'] was not found)
    if r is None:
         alt_r = safe_get(data, '0.0') # Fallback path if data['6'] is missing for an '0x' hexa
         if isinstance(alt_r, list) and len(alt_r) > 6:
              r = alt_r[6]
         else: # All attempts to get 'r' for an '0x' hexa failed
              return {"error": "0x Hexa: Could not find main data block (key '6' or '0.0.[6]')", "data_structure_summary": str(data)[:200]}
    
    # --- Start of actual data extraction from 'r' ---
    info_link = safe_get(r, '174.0')
    owner_data = safe_get(r, '57', [])

    owner_name_raw = safe_get(owner_data, 1)
    owner_name = None
    if owner_name_raw:
         owner_name = owner_name_raw.replace(' (propriétaire)', '').replace(' (Owner)', '').strip()

    owner_id = safe_get(owner_data, 2)
    owner_link = f'https://www.google.com/maps/contrib/{owner_id}' if owner_id else None

    if not owner_id:
        owner_name = None
        owner_link = None

    # 'input_hexa' is the hexa that led to this current data blob
    # 'safe_get(r, '10')' is the hexa found within this data blob, should ideally match input_hexa if no recursion happened to change it
    hexa = safe_get(r, '10', input_hexa) 

    social_link_raw = safe_get(r, '7.0')
    social_link = None
    if social_link_raw:
        social_link = social_link_raw.replace('/url?q=', '')
        if '&opi=' in social_link:
            social_link = social_link.split('&opi=', 1)[0].strip('/')
        if '%3F' in social_link:
             social_link = social_link.split('%3F', 1)[0]
        if social_link.startswith("http://"):
             social_link = "https://" + social_link[7:]
        elif not social_link.startswith("https://"):
             social_link = "https://" + social_link

    activities_list = safe_get(r, '13', [])
    activities = ""
    if isinstance(activities_list, list):
        activities = ','.join([pyslugify(str(c), separator='_') for c in activities_list])
    elif isinstance(activities_list, str):
        activities = pyslugify(activities_list, separator='_')

    activity = activities.split(',')[0] if activities else None

    cid = safe_get(r, '51.0.0.29.1')
    if cid and isinstance(cid, (int, float)) and cid < 0: # Assuming CID should be positive
        cid = abs(cid)

    at_place = safe_get(r, '204.0', []) # Seems unused, could be for "At this place" sections
    status_raw = safe_get(r, '88.0')
    business_status = 'OPERATIONAL' # Default
    if status_raw:
        if 'FERMÉ' in status_raw.upper() or 'CLOSED' in status_raw.upper():
            business_status = 'CLOSED_PERMANENTLY'
        else:
             # Only use if it looks like a standard Google Maps status (all caps)
             if status_raw.isupper() and not status_raw.islower() and ' ' not in status_raw:
                 business_status = status_raw
             # else keep OPERATIONAL or derive from other fields if needed

    etabs = [] # Seems intended for sub-establishments, not populated in this version

    bookings = {}
    allbookings_paths = ['75.0', '46.0'] # Check multiple possible paths for booking data
    for path in allbookings_paths:
        booking_container = safe_get(r, path)
        if booking_container:
             # Path '?.2' was specific to PHP's flexible array access, try common list structures
             booking_list = safe_get(booking_container, '0.2', []) # Example: data[75][0][2]
             if not isinstance(booking_list, list): # Try alternative list path if first failed
                  booking_list = safe_get(booking_container, '0', []) # Example: data[75][0]
             
             if isinstance(booking_list, list):
                 for booking_item in booking_list:
                     # Try various paths for name and link, as structure can vary
                     b_name = safe_get(booking_item, '0.0', safe_get(booking_item, '0.2.1', 'Unknown Provider'))
                     b_link = safe_get(booking_item, '1.2.0')
                     if b_name and b_link:
                         bookings[pyslugify(str(b_name))] = b_link # Slugify name for consistent key
                 if bookings: break # Found bookings, no need to check other main paths

    deliveries = {}
    # Path '75.0.1' or '75.1' for delivery section
    delivery_container_paths = ['75.0.1', '75.1'] 
    for path in delivery_container_paths:
        delivery_container = safe_get(r, path)
        if delivery_container:
            delivery_list = safe_get(delivery_container, '0.2', []) # Similar to bookings
            if not isinstance(delivery_list, list):
                delivery_list = safe_get(delivery_container, '0', [])

            if isinstance(delivery_list, list):
                for delivery_item in delivery_list:
                    d_name = safe_get(delivery_item, '0.2.1', 'Unknown Provider')
                    d_link = safe_get(delivery_item, '1.2.0')
                    if d_name and d_link:
                        deliveries[pyslugify(str(d_name))] = d_link
                if deliveries: break
                
    price = safe_get(r, '4.10')
    name = safe_get(r, '11')
    label = safe_get(r, '18') # Often the primary category or type
    advice_raw = safe_get(r, '4.8') # Number of reviews
    rate_raw = safe_get(r, '4.7')   # Rating score
    advice = int(advice_raw) if advice_raw is not None and str(advice_raw).isdigit() else 0
    rate = float(rate_raw) if rate_raw is not None else 0.0

    lat = float(safe_get(r, '9.2', 0.0))
    lng = float(safe_get(r, '9.3', 0.0))

    horaires_raw = safe_get(r, '34.1') # Schedule data
    schedule = parse_schedule(horaires_raw)

    web_raw = safe_get(r, '7.1') # Website from field 7.1
    web = f"https://{web_raw}" if web_raw else None
    if web == 'https://': web = None # Handle case where only prefix is present

    web_in_raw = safe_get(r, '38.0', '') # Website often found in "about" or details sections
    web_in = None
    if 'url?q=' in web_in_raw: # Google redirect URL
        web_in_parts = web_in_raw.split('url?q=', 1)
        if len(web_in_parts) > 1:
            web_in = web_in_parts[1]
            if '&opi' in web_in: web_in = web_in.split('&opi', 1)[0]
            web_in = web_in.replace('%23', '#').replace('%3F', '?').replace('%26', '&')
            if web_in and not web_in.startswith(('http://', 'https://')):
                 web_in = 'https://' + web_in
            elif web_in and web_in.startswith('http://'):
                 web_in = 'https://' + web_in[7:]
    elif web_in_raw and web_in_raw.startswith('www.'): # Plain www address
        web_in = 'https://' + web_in_raw
    elif web_in_raw and '.' in web_in_raw and not web_in_raw.startswith(('http', '/')): # Heuristic for domain
        web_in = 'https://' + web_in_raw


    full_address = safe_get(r, '39')
    place_id = safe_get(r, '78')
    photo = safe_get(r, '51.0.0.6.0') # A representative photo URL fragment

    address = safe_get(r, '183.1.1')       # Street address part
    postal_code = safe_get(r, '183.1.4')   # Postal code
    city = safe_get(r, '183.1.3', safe_get(r, '82.3')) # City, with fallback
    address_comp = safe_get(r, '183.1.45') # Address complement / district
    country_code = safe_get(r, '183.1.6', 'FR') # Country code, default FR
    country = get_country_from_iso(country_code)

    code_plus = safe_get(r, '183.2.1.0')      # Plus code (local part)
    code_plus_city = safe_get(r, '183.2.2.0') # Plus code (city/area part)

    # Try to extract postal code from full_address if specific field is missing (common for FR)
    if country_code == 'FR' and not postal_code and full_address and isinstance(full_address, str):
        match = re.search(r'\b(\d{5})\b', full_address)
        if match:
            postal_code = match.group(1)

    zipcode = postal_code # Alias for consistency

    phone_raw = safe_get(r, '178.0.1.1.0', safe_get(r, '178.0.0')) # Phone number, with fallback
    phone = str(phone_raw).replace(' ', '').replace('+','').strip() if phone_raw else None

    geocode = make_geocode(address, postal_code, country_code)
    geoid = make_geoid(zipcode, city) if city and zipcode else None
    department = None
    if country_code == 'FR' and zipcode and len(zipcode) >= 2 and zipcode.isdigit():
         department = zipcode[:2]

    google_link = f"https://www.google.com/maps/place/?q=place_id:{place_id}" if place_id else None
    outlet_info = safe_get(r, '32.0.1') # "Located in" / outlet context
    outlet_description = safe_get(r, '154.0.0', '').replace("\r", "").replace("\n", "") # Description
    outlet_logo = safe_get(r, '122.0.1') # Logo URL fragment

    reviews_rows = safe_get(r, '52.0', []) # List of reviews
    last_review_timestamp = 0
    last_review_author_name = None
    last_review_author_id = None
    last_review_date = None

    if isinstance(reviews_rows, list):
         for review in reviews_rows:
             # Timestamp for review date (milliseconds since epoch)
             date_review_ms = safe_get(review, '57') 
             if isinstance(date_review_ms, (int, float)) and date_review_ms > last_review_timestamp:
                 last_review_timestamp = date_review_ms
                 last_review_author_name = safe_get(review, '60.1') # Reviewer name
                 review_link = safe_get(review, '60.0') # Link to reviewer profile
                 if review_link and '/contrib/' in review_link:
                     try:
                         match = re.search(r'/contrib/([^/]+)', review_link)
                         if match:
                             last_review_author_id = match.group(1)
                     except Exception: # Regex or split error
                         last_review_author_id = None

    if last_review_timestamp > 0:
        try:
            # Convert ms to seconds for fromtimestamp
            last_review_date_obj = datetime.fromtimestamp(last_review_timestamp / 1000) 
            last_review_date = last_review_date_obj.strftime('%Y-%m-%d %H:%M:%S')
        except Exception: # Timestamp conversion error
            last_review_date = None

    hexa_link = f"https://www.google.com/maps/place/data=!3m1!4b1!4m5!3m4!1s{hexa}!8m2!3d{lat}!4d{lng}"

    poi = {
        'hexa': hexa,
        'name': name,
        'label': label,
        'activities': activities,
        'activity': activity,
        'full_address': full_address,
        'address': address,
        'address_comp': address_comp,
        'postal_code': postal_code,
        'zipcode': zipcode,
        'city': city,
        'country_code': country_code,
        'country': country,
        'department': department,
        'region': None, # Not directly available, would require mapping
        'lat': lat,
        'lng': lng,
        'place_id': place_id,
        'cid': cid,
        'phone': phone,
        'web': web,
        'web_in': web_in,
        'social_link': social_link,
        'google_link': google_link,
        'hexa_link': hexa_link,
        'info_link': info_link,
        'price': price,
        'rate': rate,
        'advice': advice,
        'schedule': schedule,
        'business_status': business_status,
        'photo': photo,
        'bookings': bookings,
        'deliveries': deliveries,
        'owner_name': owner_name,
        'owner_id': owner_id,
        'owner_link': owner_link,
        'outlet_info': outlet_info,
        'outlet_description': outlet_description,
        'outlet_logo': outlet_logo,
        'associated': extract_associated(safe_get(r, '99.0.0.1', [])), # Associated places
        'etabs': etabs, # Empty, for future use
        'code_plus': code_plus,
        'code_plus_city': code_plus_city,
        'geocode': geocode,
        'geoid': geoid,
        'last_review_date': last_review_date,
        'last_review_author_name': last_review_author_name,
        'last_review_author_id': last_review_author_id,
    }

    # Derive num_voie and voie from geocode and address
    if poi.get('geocode') and isinstance(poi.get('address'), str):
        parts = poi['geocode'].split('-')
        if len(parts) >= 3:
             num_voie_part = parts[-1] # Last part of geocode is number+suffix
             voie_part = poi['address']
             
             # Attempt to remove number part from address to get voie
             # This is heuristic and might not be perfect
             # Remove slugified num_voie_part from slugified address first
             # then try to map back or just use what's left of original address
             
             # A simpler approach: remove the number part if it's at the start of the address
             # or if it matches the extracted number from geocode logic
             
             # Heuristic: if num_voie_part (e.g., "1bis") is in address, try to isolate it
             # More robustly: use the number extracted by make_geocode if possible
             # For now, assume num_voie_part is the number component
             
             temp_voie = poi['address']
             # Try to remove number (and suffix) if it's a prefix
             if temp_voie.lower().startswith(pyslugify(num_voie_part, separator=' ').lower()):
                 temp_voie = temp_voie[len(pyslugify(num_voie_part, separator=' ')):]
             # Try to remove raw number if it's a prefix
             raw_num_match = re.match(r"(\d+)", num_voie_part)
             if raw_num_match:
                 raw_num = raw_num_match.group(1)
                 if temp_voie.startswith(raw_num + " "):
                     temp_voie = temp_voie[len(raw_num)+1:]

             poi['num_voie'] = num_voie_part
             poi['voie'] = temp_voie.strip().replace(',', '').strip() # Cleaned voie
        else: # Geocode not in expected format
            poi['num_voie'] = None
            poi['voie'] = poi.get('address')
    else: # No geocode or address not suitable
        poi['num_voie'] = None
        poi['voie'] = poi.get('address')


    return dict(sorted(poi.items()))

def get_data(hexa=None, hl='fr', is_etab=True):
    """Fetches and extracts data for a given hexa code or query string."""
    if not hexa: # Handles None or empty string
        return {"error": "Hexa/Query is missing or empty"}
    
    if not isinstance(hexa, str):
        # If hexa is not a string (e.g. int/float from bad extraction), return error.
        return {"error": f"Invalid Hexa/Query type: {type(hexa)}. Expected string. Value: '{str(hexa)[:100]}'"}
    
    isHexaCall = hexa.startswith('0x') # Determines if this call is for an '0x' hexa or a query

    if isHexaCall:
        # This is for a specific '0x' prefixed hexa ID
        pb_param = "!1m18!1s{}!3m12!1m3!1d2778.3643275641716!2d-2.572462222588248!3d47.7885655823078!2m3!1f0!2f0!3f0!3m2!1i1309!2i199!4f13.1!4m2!3d47.78747331262375!4d-2.569202184677119!5e1!13m50!2m2!1i408!2i240!3m2!2i10!5b1!7m42!1m3!1e1!2b0!3e3!1m3!1e2!2b1!3e2!1m3!1e2!2b0!3e3!1m3!1e8!2b0!3e3!1m3!1e10!2b0!3e3!1m3!1e10!2b1!3e2!1m3!1e9!2b1!3e2!1m3!1e10!2b0!3e3!1m3!1e10!2b1!3e2!1m3!1e10!2b0!3e4!2b1!4b1!9b0!14m4!1swgQ7YY-zBsqMlwS2-KYo!3b1!7e81!15i10555!15m40!1m6!4e2!18m3!3b0!6b0!14b0!20e2!2b1!4b1!5m5!2b1!3b1!5b1!6b1!7b1!10m1!8e3!17b1!20m2!1e3!1e6!24b1!25b1!26b1!29b1!30m1!2b1!36b1!43b1!52b1!55b1!56m2!1b1!3b1!65m5!3m4!1m3!1m2!1i224!2i298!22m1!1e81!29m0!30m1!3b1!32b1!37i572&pf=t".format(hexa)
        url = f"https://www.google.com/maps/preview/place?authuser=0&hl={hl}&gl={hl}&pb={pb_param}"
    else:
        # This is for a query string (e.g., from a failed '0x' hexa that turned into a search term,
        # or an initial query)
        q = hexa # hexa is already guaranteed to be a string here
        url = f"https://www.google.fr/search?tbm=map&fp=1&authuser=0&hl={hl}&gl={hl}&q={q}"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    try:
        response = requests.get(url, headers=headers, timeout=20)
        response.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)
        json_body = response.text
        if not json_body: # Handle empty response body
             return {"error": f"Empty response body received for Hexa/Query: '{str(hexa)[:100]}'"}
        # Pass isHexaCall to extract_data to guide its parsing strategy
        return extract_data(json_body, hexa, is_etab, hl, isHexaCall)
    except requests.exceptions.RequestException as e:
        # print(f"HTTP Request failed for Hexa/Query '{str(hexa)[:100]}': {e}") # For server-side logging
        return {"error": f"HTTP Request failed for '{str(hexa)[:100]}': {e}"}
    except Exception as e:
        # This catches other unexpected errors, including potential TypeErrors from extract_data if not fully handled
        # print(f"An unexpected error occurred in get_data for Hexa/Query '{str(hexa)[:100]}': {e}") # For server-side logging
        # import traceback
        # traceback.print_exc() # For detailed debugging
        return {"error": f"Unexpected error processing '{str(hexa)[:100]}': {e}"}

def parallel_get_data(hexas, hl='fr', is_etab=True, max_workers=4):
    """
    Fetches and extracts data for multiple hexa codes in parallel.
    """
    if not hexas:
        return []

    def fetch_single_hexa(hexa_item):
        try:
            result = get_data(hexa_item, hl, is_etab)
            return {'hexa_input': hexa_item, 'result': result} # Store original input for clarity
        except Exception as e: # Should ideally be caught by get_data's own exception handling
            return {'hexa_input': hexa_item, 'error': f"Outer error for {hexa_item}: {str(e)}"}

    results = []
    # Clamp max_workers to a reasonable number if hexas list is small
    actual_max_workers = min(max_workers, len(hexas)) if len(hexas) > 0 else 1


    with ThreadPoolExecutor(max_workers=actual_max_workers) as executor:
        future_to_hexa = {
            executor.submit(fetch_single_hexa, hexa_val): hexa_val
            for hexa_val in hexas
        }
        
        for future in as_completed(future_to_hexa):
            # hexa_val_completed = future_to_hexa[future] # Original input
            try:
                result_item = future.result()
                results.append(result_item)
                # Optional: server-side print for progress/debugging
                # name_or_error = "Error"
                # if 'result' in result_item and isinstance(result_item['result'], dict):
                #     name_or_error = result_item['result'].get('name', result_item['result'].get('error', 'Unknown status'))
                # elif 'error' in result_item:
                #     name_or_error = result_item['error']
                # print(f"Completed for {result_item.get('hexa_input', 'N/A')}: {name_or_error}")
            except Exception as e: # Exception from future.result() itself (rare if fetch_single_hexa handles its own)
                original_input_for_failed_future = future_to_hexa[future]
                results.append({'hexa_input': original_input_for_failed_future, 'error': f"Critical error processing future: {e}"})
                # print(f"Critical error for future of {original_input_for_failed_future}: {e}")

    return results