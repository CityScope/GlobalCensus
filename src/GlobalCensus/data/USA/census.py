
from typing import List, Union, Dict, Any, Optional

from pathlib import Path
import json  
import geopandas as gpd

from ...core import hierarchy

from . import api_keys
from . import utils as us_utils 
from . import processing 
from . import constants

# ============================================================
# USCENSUS CLASS
# ============================================================
class USCensus(hierarchy.MultiresPolygonData):
    """
    Census-specific multiresolution geospatial dataset with schema-free
    per-layer metadata querying.

    Key Features:
        - Multi-resolution geographic hierarchy
        - Schema-free per-layer metadata registry
        - Bidirectional lookup (name <-> resolution)
        - Controlled erase_water policy
    """

    # ---------------------------------------------------------------------
    # Initialization
    # ---------------------------------------------------------------------

    def __init__(
        self,
        aoi: gpd.GeoDataFrame,
        levels: Union[str, List[str]] = ["county", "tract", "blockgroup"],
        census_fields: Union[str, list, dict] = "all",
        pygris_cache_dir: str = "cache/pygris",
        census_cache_dir: str = "cache/us_census",
        api_key: str = api_keys.US_CENSUS,
        erase_water: Optional[bool] = None,
    ) -> None:
        """
        Initialize USCensus dataset.

        Args:
            aoi: Area of interest polygon.
            levels: Census hierarchy levels to load.
            census_fields: Field configuration.
            pygris_cache_dir: Cache for boundary shapes.
            census_cache_dir: Cache for census data.
            api_key: US Census API key.
            erase_water: If bool, applies globally.
                         If None, uses constants.ERASE_WATER[level].
        """

        self.aoi = aoi.to_crs(4326)
        self.pygris_cache_dir = pygris_cache_dir
        self.census_cache_dir = census_cache_dir
        self.api_key = api_key

        # ---------------- registry + mapping ----------------
        self._layer_registry: Dict[Union[int, str], Dict[str, Any]] = {}
        self._name_to_res: Dict[str, int] = {}
        self._res_to_name: Dict[int, str] = {}

        self._pending_erase_water = erase_water

        if isinstance(levels, str):
            levels = [levels]

        # ---------------- census schema ----------------
        self.census_fields = us_utils.format_fields(
            us_utils.fields_filter(census_fields)
        )

        years = set()
        for cfg in self.census_fields.values():
            years.update(cfg["years"])
        self.years = sorted(years, reverse=True)

        # ---------------- spatial filtering ----------------
        states = processing.load_shapes(
            "state",
            year=self.years[0],
            cache_dir=pygris_cache_dir,
            cache=True,
        )

        self.states = states.loc[
            states.geometry.intersects(self.aoi.to_crs(states.crs).union_all()),
            "NAME",
        ].to_list()

        counties = processing.load_shapes(
            "county",
            state=self.states,
            year=self.years[0],
            cache_dir=pygris_cache_dir,
            cache=True,
        )

        self.counties = counties.loc[
            counties.geometry.intersects(self.aoi.to_crs(counties.crs).union_all()),
            "NAME",
        ].to_list()

        agg_methods = self._derive_agg_methods()
        super().__init__(gdfs={}, agg_method=agg_methods)

        # ---------------- load initial layers ----------------
        load_list = self._organize_loading(levels)

        for item in load_list:
            lv = item["name"]
            print(f"USCensus: Fetching {lv}...")

            ew = self._resolve_erase_water(lv)

            if item["is_spinal"]:
                gdf = self._fetch_complete_gdf(lv)

                self.add_layer(data=gdf, name=lv, resolution=item["res"])

                self._register_spinal(lv, item["res"], ew)

            else:
                gdf = self._fetch_geometry(lv)
                src = constants.CENSUS_RESAMPLE.get(lv, "blockgroup")

                gdf_src = self._fetch_complete_gdf(src)
                src_item = self._organize_loading([src])[0]

                self.add_layer(data=gdf_src, name=src, resolution=src_item["res"])
                self.add_layer(data=gdf, name=lv, agg_from=src)

                self._register_derived(lv, src, ew)

    # ---------------------------------------------------------------------
    # Public API
    # ---------------------------------------------------------------------

    def add_level(
        self,
        levels: Union[str, List[str]],
        erase_water: Optional[bool] = None,
    ) -> None:
        """
        Add new census levels dynamically.

        Args:
            levels: level or list of levels.
            erase_water: global override or None for default behavior.
        """

        if isinstance(levels, str):
            levels = [levels]

        self._pending_erase_water = erase_water
        load_list = self._organize_loading(levels)

        for item in load_list:
            lv = item["name"]
            ew = self._resolve_erase_water(lv)

            print(f"USCensus: Fetching {lv}...")

            if item["is_spinal"]:
                gdf = self._fetch_complete_gdf(lv)

                self.add_layer(data=gdf, name=lv, resolution=item["res"])
                self._register_spinal(lv, item["res"], ew)

            else:
                gdf = self._fetch_geometry(lv)
                src = constants.CENSUS_RESAMPLE.get(lv, "blockgroup")

                gdf_src = self._fetch_complete_gdf(src)
                src_item = self._organize_loading([src])[0]

                self.add_layer(data=gdf_src, name=src, resolution=src_item["res"])
                self.add_layer(data=gdf, name=lv, agg_from=src)

                self._register_derived(lv, src, ew)

    # ---------------------------------------------------------------------
    # Registry helpers
    # ---------------------------------------------------------------------

    def _register_spinal(self, name: str, res: int, erase_water: bool) -> None:
        """Register spinal layer metadata."""
        self._layer_registry[res] = {
            "level": name,
            "erase_water": erase_water,
            "type": "spinal",
        }
        self._name_to_res[name] = res
        self._res_to_name[res] = name

    def _register_derived(self, name: str, src: str, erase_water: bool) -> None:
        """Register derived layer metadata."""
        self._layer_registry[name] = {
            "source": src,
            "erase_water": erase_water,
            "type": "derived",
        }

    # ---------------------------------------------------------------------
    # Query system
    # ---------------------------------------------------------------------

    def get_layer_info(self, key: Union[int, str]) -> Dict[str, Any]:
        """
        Get metadata for a layer by name or resolution.

        Args:
            key: layer name or resolution index.

        Returns:
            Metadata dictionary or empty dict.
        """

        if key in self._layer_registry:
            return self._layer_registry[key]

        if isinstance(key, str) and key in self._name_to_res:
            return self._layer_registry.get(self._name_to_res[key], {})

        if isinstance(key, int):
            return self._layer_registry.get(key, {})

        return {}

    # ---------------------------------------------------------------------
    # Internal logic
    # ---------------------------------------------------------------------

    def _resolve_erase_water(self, level: str) -> bool:
        """
        Resolve erase_water policy.

        Priority:
            1. explicit bool in constructor/add_level
            2. constants.ERASE_WATER[level]
        """
        if self._pending_erase_water is not None:
            return self._pending_erase_water

        return constants.ERASE_WATER.get(level, False)

    # ---------------------------------------------------------------------
    # Loading plan
    # ---------------------------------------------------------------------

    def _organize_loading(self, levels):
        """Build dependency-aware loading plan."""

        if isinstance(levels, str):
            levels = [levels]

        spinal = []
        special = []
        seen = set()

        for lv in levels:
            if lv in constants.CENSUS_HIERARCHY:
                spinal.append({
                    "name": lv,
                    "is_spinal": True,
                    "res": constants.CENSUS_HIERARCHY[lv],
                })
                seen.add(lv)
            else:
                special.append({
                    "name": lv,
                    "is_spinal": False,
                })

                src = constants.CENSUS_RESAMPLE.get(lv, "blockgroup")

                if src in constants.CENSUS_HIERARCHY and src not in seen:
                    spinal.append({
                        "name": src,
                        "is_spinal": True,
                        "res": constants.CENSUS_HIERARCHY[src],
                    })
                    seen.add(src)

        return sorted(spinal, key=lambda x: x["res"]) + special

    # ---------------------------------------------------------------------
    # Aggregation
    # ---------------------------------------------------------------------

    def _derive_agg_methods(self) -> Dict[str, str]:
        """Build aggregation rules for census fields."""
        methods = {}

        for cfg in self.census_fields.values():
            for f in cfg["fields"]:
                weight = cfg["agg_weights"].get(f)

                if weight:
                    methods[f] = f"density_{weight}"
                    methods[weight] = "sum"
                else:
                    methods[f] = "sum"

        return methods

    # ---------------------------------------------------------------------
    # Data access
    # ---------------------------------------------------------------------

    def _fetch_complete_gdf(self, level):
        return processing.load(
            level,
            self.aoi,
            self.census_cache_dir,
            self.pygris_cache_dir,
            api_key=self.api_key,
            erase_water=self._resolve_erase_water(level),
            census_fields=self.census_fields,
        )

    def _fetch_geometry(self, level):
        return processing.load_shapes(
            level,
            self.states,
            self.counties,
            self.years[0],
            erase_water=self._resolve_erase_water(level),
            aoi=self.aoi,
            cache_dir=self.pygris_cache_dir,
            cache=True,
        )[["GEOID", "geometry"]]

    # ---------------------------------------------------------------------
    # Persistence
    # ---------------------------------------------------------------------

    def save(self, path: Union[str, Path], overwrite: bool = True) -> Path:
        """Save dataset + metadata."""
        directory = super().save(path, overwrite=overwrite)

        manifest_path = directory / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

        manifest["census_metadata"] = {
            "aoi_geojson": self.aoi.__geo_interface__,
            "pygris_cache_dir": self.pygris_cache_dir,
            "census_cache_dir": self.census_cache_dir,
            "census_fields": self.census_fields,
            "states": self.states,
            "counties": self.counties,
            "years": self.years,
            "layer_registry": {str(k): v for k, v in self._layer_registry.items()},
        }

        manifest_path.write_text(json.dumps(manifest, indent=2, default=str))

        return directory

    @classmethod
    def load(cls, path: Union[str, Path]) -> "USCensus":
        """Load dataset from disk."""
        obj = super().load(path)
        directory = Path(path)

        manifest = hierarchy._read_manifest(directory)
        meta = manifest.get("census_metadata", {})

        obj.aoi = gpd.GeoDataFrame.from_features(meta["aoi_geojson"]["features"], crs=4326)
        obj.pygris_cache_dir = meta["pygris_cache_dir"]
        obj.census_cache_dir = meta["census_cache_dir"]
        obj.census_fields = meta["census_fields"]
        obj.states = meta.get("states", [])
        obj.counties = meta.get("counties", [])
        obj.years = meta.get("years", [])

        obj._layer_registry = {
            (int(k) if k.isdigit() else k): v
            for k, v in meta.get("layer_registry", {}).items()
        }

        return obj