
# ============================================================
# UTILITIES
# ============================================================

def set_pygris_cache_dir(path):
    os.makedirs(path,exist_ok=True)
    # Set up local pygrid cache dir
    platformdirs.user_cache_dir = lambda appname="pygris", **kwargs: os.path.abspath(path)


def fields_filter(
    filters: list[dict] | str | None = None,
    source: list[str] | str | None = None,
    year: list[str] | int | None = None,
    field: list[str] | str | None = None,
    census_fields: dict=CENSUS_FIELDS,
) -> dict:
    if isinstance(filters,str) and filters == "all":
        return census_fields 
    
    if isinstance(source,str):
        source = [source]

    if isinstance(field,str):
        field = [field]

    if isinstance(year,int):
        year = [year]

    def match(value, allowed):
        return not allowed or value in allowed

    def extract(source_key, category_key, category):
        return {
            "source": source_key,
            "category": category_key,
            "years": category.get("years"),
            "fields": category.get("fields", {}),
            "fields_universe": category.get("fields_universe", {}),
            "agg_weights": category.get("agg_weights", {}),
        }

    # ----------------------------------------
    # STEP 1: flatten structure
    # ----------------------------------------
    flat = []
    for source_key, categories in census_fields.items():
        for category_key, category in categories.items():
            flat.append(extract(source_key, category_key, category))

    # ----------------------------------------
    # STEP 2: apply query filters (OR logic)
    # ----------------------------------------
    def apply_single_filter(f):
        return [
            item for item in flat
            if match(item["source"], f.get("source"))
            and match(item["years"], f.get("year"))
            and (
                not f.get("field")
                or any(k in f["field"] for k in item["fields"].keys())
            )
        ]

    if filters:
        filtered = []
        for f in filters:
            filtered.extend(apply_single_filter(f))
        # deduplicate
        filtered = { (i["source"], i["category"]): i for i in filtered }.values()
    else:
        filtered = flat

    # ----------------------------------------
    # STEP 3: apply global filters (AND logic)
    # ----------------------------------------
    result = []
    for item in filtered:
        if not match(item["source"], source):
            continue
        if not match(item["years"], year):
            continue
        if field:
            # keep only matching field keys
            matched_fields = {
                k: v for k, v in item["fields"].items() if k in field
            }
            if not matched_fields:
                continue
            item = {**item, "fields": matched_fields}

        result.append(item)

    # ----------------------------------------
    # STEP 4: rebuild nested structure
    # ----------------------------------------
    out = {}
    for item in result:
        s = item["source"]
        c = item["category"]

        out.setdefault(s, {})
        out[s][c] = {
            "years": item["years"],
            "fields": item["fields"],
            "fields_universe": item["fields_universe"],
            "agg_weights": item["agg_weights"],
        }

    return out

def format_filter(filter: Dict[str, Union[str, List[str]]]) -> Dict[str, List[str]]:
    """
    Normalize filter values for consistent matching.

    - 'state' -> USPS abbreviation (case-insensitive, accepts full name or abbrev)
    - other keys (county, place) -> lowercase for case-insensitive matching

    Parameters
    ----------
    filter : dict
        Filter dictionary with keys like 'state', 'county', 'place'.

    Returns
    -------
    dict
        Normalized filter dictionary with lists of strings.
    """
    target = {}

    for key, val in filter.items():
        vals = [val] if isinstance(val, str) else val
        if key.lower() == "state":
            target["state"] = [us.states.lookup(v.strip()).abbr for v in vals]
        else:
            target[key.lower()] = [v.strip().lower() for v in vals]

    return target

def pick_geoid_column(cols: Iterable[str]) -> Optional[str]:
    def priority(col: str) -> int:
        c = col.upper()
        if c == "GEOID": return 0
        if c == "GEOID20": return 1
        if c == "GEOID10": return 2
        if "GEOID" in c: return 3
        return 999
    return min(cols, key=priority, default=None)

def get_field_col_name(source: str, field_code: str) -> str:
    # Invert SOURCE_MAPPING
    inverted = {v: k for k, v in SOURCE_MAPPING.items()}

    # Normalize source using inverted lookup if possible
    s = inverted.get(source, source)

    # Standardize formatting
    s = s.replace("/", "_").replace("decennial_dhc", "dec_dhc")

    return f"source_{s}_field_{field_code}"

def get_cache_path(cache_dir: str, state: str, year: int, level: str, erase_water: bool) -> Path:
    suffix = "_erase_water" if erase_water else ""
    return Path(cache_dir) / f"{state.upper()}_{year}_{level.replace(' ', '_')}{suffix}.parquet"


def format_fields(raw_fields):
    def add_e(c: str, src: str) -> str:
        if "acs" in src and not c.endswith("E"):
            return c + "E"
        
        return c

    formatted = {}

    for src_key, topics in raw_fields.items():
        api_src = SOURCE_MAPPING.get(src_key, src_key)

        if "source" in topics.keys():
            return raw_fields # Already formatted

        for topic_key, content in topics.items():
            # Resolve years
            raw_years = content.get("years", "latest")
            if raw_years in [None, "latest"]:
                years = [CENSUS_LATEST_YEARS.get(api_src, 2020)]
            else:
                years = [raw_years] if isinstance(raw_years, (int, str)) else list(raw_years)

            logic_key = f"{src_key}_{topic_key}"
            
            # Initialize structure
            formatted[logic_key] = {
                "source": api_src,
                "years": years,
                "fields": {},
                "agg_weights": {},
                "fields_universe": {},
            } 

            for yr in years:
                field_to_col = {}
                
                # 1. Process Fields
                for field_name, codes in content.get("fields", {}).items():
                    col_name = f"{yr}_{src_key}_{topic_key}_{field_name}"
                    clean_codes = [add_e(c, api_src) for c in codes]
                    
                    formatted[logic_key]["fields"][col_name] = clean_codes
                    field_to_col[field_name] = col_name

                # 2. Process Weights (Using year-specific keys to prevent overwriting)
                for f, w in content.get("agg_weights", {}).items():
                    target_f = field_to_col.get(f, f)
                    target_w = field_to_col.get(w, w)
                    formatted[logic_key]["agg_weights"][target_f] = target_w

                # 3. Process Universe
                for f, u in content.get("fields_universe", {}).items():
                    target_f = field_to_col.get(f, f)
                    target_u = field_to_col.get(u, u)
                    formatted[logic_key]["fields_universe"][target_f] = target_u

    return formatted
