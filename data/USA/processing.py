from pathlib import Path
import os 
import platformdirs
import inspect
from typing import Union, List, Optional, Dict

import pandas as pd
import polars as pl 
import geopandas as gpd
import shapely
import pyarrow.parquet as pq

import pygris
import pygris.utils
from pygris.data import get_census, get_lodes
import us  # For state normalization

from ...core import geometry_utils 
from . import api_keys
from . import constants
from . import utils as us_utils

import warnings
warnings.filterwarnings("ignore", message="Geometry is in a geographic CRS")


# ============================================================
# CORE PROCESSING
# ============================================================

def fetch_raw_data(state_abbr: str, year: int, level: str, source: str, fields: List[str], api_key: Optional[str] = None) -> pd.DataFrame:
    state_obj = us.states.lookup(state_abbr)
    
    if "lodes" in source:
        parts = source.split("/")
        l_ver, l_type = parts[1], parts[2]
        
        # LODES Specific Aggregation Levels
        agg_level = level
        if level == "blockgroup": agg_level = "block group"
        elif level == "place": agg_level = "block"
        elif level == "state": agg_level = "county"

        df = get_lodes(version=l_ver, state=state_abbr.lower(), year=year, 
                       lodes_type=l_type, agg_level=agg_level, cache=True)
        
        geocode_col = "h_geocode" if l_type == "rac" else "w_geocode"
        
        if level == "state":
            df[geocode_col] = df[geocode_col].astype(str).str.slice(stop=2)
            
        df = df.groupby(geocode_col).agg("sum").reset_index().rename(columns={geocode_col: "GEOID"})
    else:
        hier = constants.GEO_HIERARCHIES.get(level, ["state", "county", "tract"])
        df = pd.DataFrame(get_census(
            dataset=source, year=year, variables=fields,
            params={"for": f"{hier[-1]}:*", "in": [f"{h}:*" if h != "state" else f"state:{state_obj.fips}" for h in hier[:-1]], "key": api_key},
            return_geoid=True
        ))

    # Robust GEOID column creation
    if "GEOID" not in df.columns:
        df["GEOID"] = df[us_utils.pick_geoid_column(df.columns)].astype(str)
    
    # Rename to cache format: s_{source}f{field_code}
    rename_map = {f: us_utils.get_field_col_name(source, f) for f in fields if f in df.columns}
    return df[["GEOID"] + list(rename_map.keys())].rename(columns=rename_map)

def process_state_year(state: str, year: int, level: str, census_fields: Dict, aoi_bounds: Optional[tuple], cache_dir: str, api_key: str, erase_water: bool) -> pl.DataFrame:
    state_obj = us.states.lookup(state)
    path = us_utils.get_cache_path(cache_dir, state_obj.abbr, year, level, erase_water)
    
    # 1. Check Metadata for missing fields
    existing_cols = pq.read_metadata(path).schema.names if path.exists() else []
    missing_by_src = {}
    for logic_key, config in census_fields.items():
        if year not in config["years"]: continue
        src = config["source"]
        for codes in config["fields"].values():
            for code in codes:
                col = us_utils.get_field_col_name(src, code)
                if col not in existing_cols:
                    missing_by_src.setdefault(src, set()).add(code)

    # 2. Update/Create Cache (Using GeoPandas to protect row-geometry alignment)
    if not path.exists() or missing_by_src:
        if path.exists():
            gdf = gpd.read_parquet(path)
        else:
            func = constants.GEOMETRY_FUNCS.get(level, pygris.block_groups)
            gdf = func(state=state_obj.abbr, year=year, cache=True).to_crs(4326)
            if erase_water: gdf = pygris.utils.erase_water(gdf, cache=True)
            if "GEOID" not in gdf.columns:
                gdf["GEOID"] = gdf[us_utils.pick_geoid_column(gdf.columns)].astype(str)
            
            gdf["x"], gdf["y"] = gdf.geometry.centroid.x, gdf.geometry.centroid.y
            b = gdf.bounds
            gdf["minx"], gdf["miny"], gdf["maxx"], gdf["maxy"] = b["minx"], b["miny"], b["maxx"], b["maxy"]
            gdf["year"], gdf["state"] = year, state_obj.abbr
            gdf["area"] = geometry_utils.area(gdf)

        for src, codes in missing_by_src.items():
            new_data = fetch_raw_data(state_obj.abbr, year, level, src, list(codes), api_key)
            gdf = gdf.merge(new_data, on="GEOID", how="left")
        
        gdf.to_parquet(path, index=False)

    # 3. Fast extraction (Using Polars)
    lf = pl.scan_parquet(path)
    if aoi_bounds is not None:
        minx, miny, maxx, maxy = aoi_bounds
        lf = lf.filter((pl.col("maxx") >= minx) & (pl.col("minx") <= maxx) & (pl.col("maxy") >= miny) & (pl.col("miny") <= maxy))

    # Aggregation expressions for logical fields
    agg_exprs = []
    requested_logical_names = []
    for logic_key, config in census_fields.items():
        if year not in config["years"]: continue
        for logical_name, codes in config["fields"].items():
            raw_cols = [us_utils.get_field_col_name(config["source"], c) for c in codes]
            agg_exprs.append(
                pl.sum_horizontal([
                    pl.col(c).cast(pl.Float64).fill_null(0)
                    for c in raw_cols
                ]).alias(logical_name)
            )
            requested_logical_names.append(logical_name)

    return lf.with_columns(agg_exprs).select(["GEOID", "geometry", "year", "state", "area"] + requested_logical_names).collect()

# ============================================================
# MAIN INTERFACE
# ============================================================

def load_shapes(
    level: str = "block",
    state=None,
    county=None,
    year="latest",
    erase_water: bool = False,
    crs=4326,
    cache: bool = True,
    cb: bool = False,
    aoi=None,
    cache_dir=None,
):
    if year is None or year == "latest":
        year = constants.CENSUS_LATEST_YEARS[level]

    if cache_dir is None:
        cache_dir = platformdirs.user_cache_dir("pygris_cache")

    if cache_dir is not None:
        us_utils.set_pygris_cache_dir(cache_dir)

    # Allow plural names
    for k, v in list(constants.GEOMETRY_FUNCS.items()):
        constants.GEOMETRY_FUNCS[k + "s"] = v

    func = constants.GEOMETRY_FUNCS[level]
    if isinstance(state, str):
        state = [state]
    elif state is None:
        state = [None]

    if isinstance(county, str):
        county = [county]
    elif county is None:
        county = [None]

    df = []
    for s in state:
        for c in county:
            # Build kwargs only with parameters that the function accepts
            sig = inspect.signature(func)
            func_args = {}
            if "state" in sig.parameters and s is not None:
                func_args["state"] = s
            if "county" in sig.parameters and c is not None:
                func_args["county"] = c
            if "year" in sig.parameters:
                func_args["year"] = year
            if "cache" in sig.parameters:
                func_args["cache"] = cache
            if "cb" in sig.parameters:
                func_args["cb"] = cb

            # Call the Pygris function
            shapes = func(**func_args)

            # Reproject if needed
            if crs:
                shapes = shapes.to_crs(crs)

            if erase_water:
                shapes = pygris.utils.erase_water(shapes, year=year, cache=cache)

            df.append(shapes.to_crs(4326))

    df = pd.concat(df).to_crs(4326)
    if aoi is not None:
        aoi = aoi.to_crs(4326).union_all()
        df = df[df.intersects(aoi)]

    if "GEOID" not in df.columns:
        df["GEOID"] = df[pick_geoid_column(df.columns)].astype(str)
    
    df["x"], df["y"] = df.geometry.centroid.x, df.geometry.centroid.y
    b = df.bounds
    df["minx"], df["miny"], df["maxx"], df["maxy"] = b["minx"], b["miny"], b["maxx"], b["maxy"]
    df["year"] = year
    df["area"] = geometry_utils.area(df)

    return df


def load(
    level: str = "blockgroup",
    aoi: Optional[gpd.GeoDataFrame] = None,
    cache_dir: str|None = None,
    pygris_cache_dir: str|None = None,
    api_key: str = api_keys.US_CENSUS,
    erase_water: bool = False,
    census_fields = constants.CENSUS_FIELDS,
    states: str|list[str]|None = None
) -> gpd.GeoDataFrame:
    if cache_dir is None:
        cache_dir = platformdirs.user_cache_dir("us_census_cache")

    if pygris_cache_dir is None:
        pygris_cache_dir = platformdirs.user_cache_dir("pygris_cache")
        if pygris_cache_dir is None:
            pygris_cache_dir = cache_dir
        
    if pygris_cache_dir is not None:
        us_utils.set_pygris_cache_dir(pygris_cache_dir)

    os.makedirs(cache_dir, exist_ok=True)

    census_fields = us_utils.format_fields(census_fields)
    
    # Extract years directly from the fields dictionary
    process_years = set()
    for cfg in census_fields.values():
        process_years.update(cfg["years"])
    process_years = sorted(list(process_years), reverse=True) # Newest first

    if states is None or len(states) == 0:
        if aoi is None:
            raise Exception("aoi or states are mandatory")
        
        us_states = load_shapes("states", year=process_years[0], cache_dir=pygris_cache_dir, cache=True)
        states = us_states.loc[
            us_states.geometry.intersects(
                aoi.to_crs(us_states.crs).union_all()
            ),
            "NAME",
        ].to_list()


    states = [states] if isinstance(states, str) else states
    aoi_bounds = aoi.to_crs(4326).total_bounds if aoi is not None else None

    state_results = []
    for state in states:
        year_dfs = []
        for yr in process_years:
            ydf = process_state_year(state, yr, level, census_fields, aoi_bounds, cache_dir, api_key, erase_water)
            if not ydf.is_empty():
                year_dfs.append(ydf)
        
        if not year_dfs: continue

        # Merge years by GEOID: Use newest year as base, no suffixes for conflicts
        combined_df = year_dfs[0]
        for older_df in year_dfs[1:]:
            # Join only new columns that do not exist in the newer year data
            new_cols = [c for c in older_df.columns if c not in combined_df.columns]
            if new_cols:
                # Outer join on GEOID. coalesce=True merges the join keys into one "GEOID" column automatically.
                # We also ensure geometry/year/state from the newer year are preserved where available.
                combined_df = combined_df.join(
                    older_df.select(["GEOID"] + new_cols),
                    on="GEOID",
                    how="full",
                    coalesce=True
                )
            
            # Add rows (GEOIDs) that only exist in the older year
            missing_geoids = older_df.join(
                combined_df.select("GEOID"),
                on="GEOID",
                how="anti"
            )
            if not missing_geoids.is_empty():
                combined_df = pl.concat([combined_df, missing_geoids], how="diagonal")
        
        state_results.append(combined_df)

    # Concat all states
    if not state_results:
        return gpd.GeoDataFrame()

    full_pl = pl.concat(state_results, how="diagonal")
    
    full_pl = full_pl.with_columns(
        pl.col("geometry")
        .map_elements(shapely.from_wkb, return_dtype=pl.Object)
    )

    # Convert to GeoDataFrame
    gdf = gpd.GeoDataFrame(full_pl.to_pandas(), geometry="geometry", crs=4326)
        
    if aoi is not None:
        gdf = gdf[gdf.intersects(aoi.to_crs(4326).union_all())].reset_index(drop=True)

    return gdf

def compute_densities_and_ratios(
    gdf: Union[gpd.GeoDataFrame, pd.DataFrame],
    categories: Optional[Dict[str, Dict]] = None,
    densities: bool = True,
    density_fields: Optional[List[str]] = None,
    ratios: bool = True,
    ratio_fields: Optional[List[str]] = None,
    ratio_universe_fields: Optional[List[str]] = None,
    geoid_col: Optional[str] = None
) -> gpd.GeoDataFrame:
    """
    Compute densities and ratios for fields in a GeoDataFrame or DataFrame.

    Automatically resolves universe placeholders in categories dict.

    Parameters
    ----------
    gdf : GeoDataFrame or DataFrame
    categories : dict, optional
        Categories dictionary containing fields and universes for ratios/densities.
    densities : bool
    density_fields : list of str, optional
    ratios : bool
    ratio_fields : list of str, optional
    ratio_universe_fields : list of str, optional
    geoid_col : str, optional

    Returns
    -------
    GeoDataFrame
    """
    df = gdf.copy()

    # Detect GEOID column if not provided
    if geoid_col is None:
        geoid_col = us_utils.pick_geoid_column(df.columns)
        if geoid_col is None:
            raise KeyError("No GEOID column found in gdf.")

    # Format categories if provided
    if categories is not None:
        categories = us_utils.format_fields(categories)

    # -------------------------
    # Compute ratios
    # -------------------------
    if ratios:
        if ratio_fields is not None and ratio_universe_fields is not None:
            if len(ratio_fields) != len(ratio_universe_fields):
                raise ValueError("ratio_fields and ratio_universe_fields must have the same length.")
            for field, universe in zip(ratio_fields, ratio_universe_fields):
                if field in df.columns and universe in df.columns:
                    df[f"{field}_ratio"] = df[field] / df[universe]
        elif categories is not None:
            for cat_dict in categories.values():
                fields = cat_dict.get("fields", {})
                fields_universe = cat_dict.get("fields_universe", {})

                for field_name in fields.keys():
                    # 1. Determine universe placeholder
                    if field_name in fields_universe:
                        universe_col = fields_universe[field_name]
                    else:
                        universe_col = fields_universe.get("default")

                    # 2. Skip if no universe defined
                    if universe_col is None:
                        continue

                    # 3. Skip control flags
                    if universe_col in ("NO_DENSITY_OR_RATIO", "DENSITY_ONLY"):
                        continue

                    # 4. Compute ratio only if both columns exist
                    if field_name in df.columns and universe_col in df.columns:
                        df[f"{field_name}_ratio"] = df[field_name] / df[universe_col]
    # -------------------------
    # Compute densities
    # -------------------------
    if densities:
        if isinstance(df, gpd.GeoDataFrame):
            # Compute areas in m²
            try:
                df_proj = df.to_crs(df.estimate_utm_crs())
                df['area'] = df_proj.geometry.area
            except Exception:
                # fallback: geodesic area
                df = df.to_crs(4326)
                df['area'] = df.geometry.map(lambda geom: geometry_utils.geodesic_area(geom))

            # Determine fields for density
            fields_for_density = density_fields or []
            if categories is not None and not density_fields:
                for cat_dict in categories.values():
                    fields_for_density.extend(cat_dict["fields"].keys())
            fields_for_density = [f for f in fields_for_density if f in df.columns]

            # Compute densities (per km²)
            for f in fields_for_density:
                # Try converting to numeric
                numeric = pd.to_numeric(df[f], errors="coerce")

                # If at least one non-NaN value exists, treat as numeric
                if numeric.notna().any():
                    df[f"{f}_density"] = numeric / (df["area"] / 1e6)
        else:
            print("Input is not a GeoDataFrame. Skipping density computation.")

    return df


def resample(
    census_gdf,
    geometries,
    categories=None, 
    columns=None,
    weights=None
):
    if columns is None:
        columns = []
    if weights is None:
        weights = []

    if len(weights) == 0 and len(columns) != 0:
        weights = [None for i in range(len(columns))]

    census_gdf = census_gdf.copy()
    geometries = geometries.copy()
    if categories is not None:
        categories = us_utils.format_fields(categories)

    geometries['_idx'] = geometries.index
    census_gdf = geometry_utils.source_ids_to_dst_geometry(
        geometries,
        census_gdf,
        contain='centroid',
        id_column="_idx"
    )
    census_gdf["_idx"] = census_gdf["_idx"].str[0]
    # -------------------------
    # Apply weighted aggregation
    # -------------------------
    if categories is not None:
        for cat_name, cat_dict in categories.items():
            fields = cat_dict.get("fields", {})
            agg_weights = cat_dict.get("agg_weights", {})

            columns.extend(list(fields.keys()))

            for field_name in fields.keys():
                agg_weight_key = agg_weights.get(field_name)

                if agg_weight_key:
                    weights.append(agg_weight_key)
                else:
                    weights.append(None)    

    _columns = []
    _weights = []
    for i in range(len(weights)):
        col = columns[i]
        w_col = weights[i]
        if col in census_gdf.columns:
            numeric = pd.to_numeric(census_gdf[col], errors="coerce")
            # If at least one non-NaN value exists, treat as numeric
            if numeric.notna().any():
                census_gdf[col] = numeric
                _columns.append(col)

            if w_col in census_gdf.columns:
                numeric = pd.to_numeric(census_gdf[w_col], errors="coerce")
                # If at least one non-NaN value exists, treat as numeric
                if numeric.notna().any():
                    census_gdf[w_col] = numeric
                    _weights.append(w_col)
                else:
                    _weights.append(None)
            else:
                _weights.append(None)

    columns = [col if col in census_gdf.columns else None for col in columns]
    weights = [col if col in census_gdf.columns else None for col in weights]
    if len(columns) != len(weights):
        raise Exception(f"Length mismatch. Length of columns is {len(columns)}. Length of weights is {len(weights)}")
    
    for i in range(len(weights)):
        if weights[i] is None:
            continue 

        census_gdf[columns[i]] *= census_gdf[weights[i]]

    census_df = census_gdf[["_idx",*columns]].groupby("_idx").agg('sum').reset_index()
    for i in range(len(weights)):
        if weights[i] is not None:
            census_gdf[columns[i]] /= census_gdf[weights[i]]
        
    census_df = census_df[[col for col in census_df.columns if col not in geometries.columns]]
    geometries_with_census = geometries.merge(census_df,on="_idx",how="right")
    geometries_with_census = geometries_with_census.drop(columns=["_idx"])
    return geometries_with_census