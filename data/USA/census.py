
from typing import List, Union, Dict

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
    """Census-specific multiresolution geospatial dataset."""

    # -------------------------------------------------------------------------
    # Init
    # -------------------------------------------------------------------------

    def __init__(
        self,
        aoi: gpd.GeoDataFrame,
        levels: Union[str, List[str]] = ["county", "tract", "blockgroup"],
        census_fields: Union[str, list, dict] = "all",
        pygris_cache_dir: str = "cache/pygris",
        census_cache_dir: str = "cache/us_census",
        api_key: str = api_keys.US_CENSUS,
        erase_water: bool = False,
    ) -> None:

        self.aoi = aoi.to_crs(4326)
        self.pygris_cache_dir = pygris_cache_dir
        self.census_cache_dir = census_cache_dir
        self.api_key = api_key
        self.erase_water = erase_water

        if isinstance(levels, str):
            levels = [levels]

        self.census_fields = us_utils.format_fields(us_utils.fields_filter(census_fields))

        # extract years
        years = set()
        for cfg in self.census_fields.values():
            years.update(cfg["years"])
        self.years = sorted(years, reverse=True)

        # base geography filters
        states = processing.load_shapes("state", year=self.years[0], cache_dir=pygris_cache_dir, cache=True)
        self.states = states.loc[
            states.geometry.intersects(self.aoi.to_crs(states.crs).union_all()),
            "NAME",
        ].to_list()

        counties = processing.load_shapes("county", state=self.states, year=self.years[0], cache_dir=pygris_cache_dir, cache=True)
        self.counties = counties.loc[
            counties.geometry.intersects(self.aoi.to_crs(counties.crs).union_all()),
            "NAME",
        ].to_list()

        agg_methods = self._derive_agg_methods()

        super().__init__(gdfs={}, agg_method=agg_methods)

        load_list = self._organize_loading(levels)

        for item in load_list:
            lv = item["name"]
            print(f"USCensus: Fetching {lv}...")

            if item["is_spinal"]:
                gdf = self._fetch_complete_gdf(lv)
                self.add_layer(data=gdf, name=lv, resolution=item["res"])
            else:
                gdf = self._fetch_geometry(lv)
                src = constants.CENSUS_RESAMPLE.get(lv, "blockgroup")

                gdf_src = self._fetch_complete_gdf(src)
                src_item = self._organize_loading([src])[0]

                self.add_layer(data=gdf_src, name=src, resolution=src_item["res"])
                self.add_layer(data=gdf, name=lv, agg_from=src)

    # -------------------------------------------------------------------------
    # Aggregation rules
    # -------------------------------------------------------------------------

    def _derive_agg_methods(self) -> Dict[str, str]:
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

    # -------------------------------------------------------------------------
    # Loading plan
    # -------------------------------------------------------------------------

    def _organize_loading(self, levels):
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

    # -------------------------------------------------------------------------
    # Data fetching
    # -------------------------------------------------------------------------

    def _fetch_complete_gdf(self, level):
        if level in self.gdfs:
            return self.gdfs[level]

        if level in self._name_to_res:
            return self.gdfs[self._name_to_res[level]]

        return processing.load(
            level,
            self.aoi,
            self.census_cache_dir,
            self.pygris_cache_dir,
            api_key=self.api_key,
            erase_water=self.erase_water,
            census_fields=self.census_fields,
        )

    def _fetch_geometry(self, level):
        if level in self.gdfs:
            return self.gdfs[level][["GEOID", "geometry"]]

        if level in self._name_to_res:
            return self.gdfs[self._name_to_res[level]][["GEOID", "geometry"]]

        return processing.load_shapes(
            level,
            self.states,
            self.counties,
            self.years[0],
            erase_water=self.erase_water,
            aoi=self.aoi,
            cache_dir=self.pygris_cache_dir,
            cache=True,
        )[["GEOID", "geometry"]]

    # -------------------------------------------------------------------------
    # SAVE (extended manifest only)
    # -------------------------------------------------------------------------

    def save(self, path: Union[str, Path], overwrite: bool = True) -> Path:
        """Save dataset + census-specific metadata into manifest."""

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
        }

        manifest_path.write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")

        return directory

    # -------------------------------------------------------------------------
    # LOAD (fixed + aligned with parent)
    # -------------------------------------------------------------------------

    @classmethod
    def load(cls, path: Union[str, Path]) -> "USCensus":
        """Load dataset using parent loader + restore census metadata."""

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

        return obj