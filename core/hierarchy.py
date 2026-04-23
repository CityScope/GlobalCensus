from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Union, Tuple, Literal

import geopandas as gpd

from . import geometry_utils


# =============================================================================
# IO backend mapping
# =============================================================================

_GDF_DRIVERS = {
    "geoparquet": None,
    "parquet": None,
    "fgb": "FlatGeobuf",
    "gpkg": "GPKG",
    "geojson": "GeoJSON",
    "shp": "ESRI Shapefile",
}


# =============================================================================
# Helpers
# =============================================================================

def _ensure_dir(path: Union[str, Path], overwrite: bool) -> Path:
    p = Path(path)
    if p.exists():
        if not overwrite:
            raise FileExistsError(p)
        shutil.rmtree(p)
    p.mkdir(parents=True)
    return p


def _write_manifest(directory: Path, meta: dict) -> None:
    (directory / "manifest.json").write_text(
        json.dumps(meta, indent=2, default=str),
        encoding="utf-8",
    )


def _read_manifest(directory: Path) -> dict:
    return json.loads((directory / "manifest.json").read_text(encoding="utf-8"))


def _resolve_key(obj: "MultiresPolygonData", key: Union[int, str]) -> Union[int, str]:
    if isinstance(key, int):
        return key
    if key in obj._name_to_res:
        return obj._name_to_res[key]
    return key


# =============================================================================
# Main class
# =============================================================================

class MultiresPolygonData:
    """Hierarchical multi-resolution polygon system.

    Features:
    - Integer-based hierarchical spine (0..N)
    - Named side layers
    - Sparse DAG relationships
    - Attribute propagation across resolutions
    - Single-format persistence per dataset
    """

    # -------------------------------------------------------------------------
    # Init
    # -------------------------------------------------------------------------

    def __init__(
        self,
        gdfs: Union[List[gpd.GeoDataFrame], Dict[str, gpd.GeoDataFrame]],
        agg_method: Dict[str, str],
    ) -> None:

        self.agg_method = agg_method
        self.gdfs: Dict[Union[int, str], gpd.GeoDataFrame] = {}
        self.links: set[Tuple[Union[int, str], Union[int, str]]] = set()
        self._name_to_res: Dict[str, int] = {}

        if isinstance(gdfs, list):
            for i, gdf in enumerate(gdfs):
                self.add_layer(data=gdf, resolution=i)
        else:
            for i, (name, gdf) in enumerate(gdfs.items()):
                self.add_layer(data=gdf, name=name, resolution=i)

    # -------------------------------------------------------------------------
    # Access
    # -------------------------------------------------------------------------

    def __getitem__(self, key: Union[int, str]) -> gpd.GeoDataFrame:
        if isinstance(key, int):
            keys = sorted([k for k in self.gdfs if isinstance(k, int)])
            return self.gdfs[keys[key]]

        if key in self._name_to_res:
            return self.gdfs[self._name_to_res[key]]

        return self.gdfs[key]
    
    def layers(self) -> List[str]:
        """Return all layer names (hierarchy + side layers).

        For resolution layers, returns their assigned name if available,
        otherwise falls back to their stringified resolution index.
        """

        result: List[str] = []

        for k in self.gdfs.keys():
            # Side layers already have string names
            if isinstance(k, str):
                result.append(k)

            # Hierarchy layers: resolve to name if exists
            else:
                # reverse lookup: resolution -> name
                name = None
                for n, r in self._name_to_res.items():
                    if r == k:
                        name = n
                        break

                result.append(name if name is not None else str(k))

        return result

    # -------------------------------------------------------------------------
    # Layer management
    # -------------------------------------------------------------------------

    def add_layer(
        self,
        data: gpd.GeoDataFrame,
        name: Optional[str] = None,
        resolution: Optional[int] = None,
        agg_from: Optional[Union[int, str]] = None,
        replace_existing: bool = True,
    ) -> None:

        if name is None and resolution is None:
            raise ValueError("Must provide name or resolution.")

        # ---------------- geometry normalize ----------------
        data = data.copy()
        if data.geometry.name != "geometry":
            data = data.rename(columns={data.geometry.name: "geometry"}).set_geometry("geometry")

        # if "area" not in data.columns:
        #     data["area"] = geometry_utils.area(data)

        # ---------------- side layer ----------------
        if resolution is None:
            key = name
            self.gdfs[key] = data

            if agg_from is not None:
                ref = _resolve_key(self, agg_from)
                self.links.add((ref, name))

            self._fill_columns_internal()
            return

        # ---------------- hierarchy insertion ----------------
        if not replace_existing:
            # shift upward
            keys = sorted([k for k in self.gdfs if isinstance(k, int)], reverse=True)

            for k in keys:
                if k >= resolution:
                    self.gdfs[k + 1] = self.gdfs.pop(k)

            # shift name map
            for n in list(self._name_to_res):
                if self._name_to_res[n] >= resolution:
                    self._name_to_res[n] += 1

            # shift links
            new_links = set()
            for a, b in self.links:
                def shift(x):
                    if isinstance(x, int) and x >= resolution:
                        return x + 1
                    return x
                new_links.add((shift(a), shift(b)))
            self.links = new_links

        # insert
        self.gdfs[resolution] = data

        if name:
            self._name_to_res[name] = resolution

        # auto-link
        if resolution > 0 and (resolution - 1) in self.gdfs:
            self.links.add((resolution, resolution - 1))

        self._fill_columns_internal()

    # -------------------------------------------------------------------------
    # Spatial linkage
    # -------------------------------------------------------------------------

    def _resolution_mapping(self, parent_key: Union[int, str], child_key: Union[int, str]) -> None:
        parent = self.gdfs[parent_key]
        child = self.gdfs[child_key]

        centroids = child.copy()
        centroids.geometry = child.geometry.centroid

        joined = centroids.sjoin(parent[[parent.geometry.name]], how="left", predicate="intersects")
        joined = joined.groupby(level=0).first()

        idx_col = [c for c in joined.columns if c.startswith("index_")]
        if not idx_col:
            raise RuntimeError("Spatial join failed")

        idx_col = idx_col[0]

        self.gdfs[child_key][f"parent_{parent_key}_idx"] = joined[idx_col]

        counts = joined[idx_col].value_counts()
        self.gdfs[parent_key][f"child_{child_key}_count"] = (
            self.gdfs[parent_key].index.map(counts).fillna(0).astype(int)
        )

    # -------------------------------------------------------------------------
    # Propagation (unchanged logic preserved)
    # -------------------------------------------------------------------------

    def _propagate(self, src, tgt, direction: Literal["up", "down"]) -> bool:
        parent = tgt if direction == "up" else src
        child = src if direction == "up" else tgt

        src_df = self.gdfs[src]
        tgt_df = self.gdfs[tgt]

        cols = [c for c in self.agg_method if c in src_df.columns and c not in tgt_df.columns]
        if not cols:
            return False

        map_col = f"parent_{parent}_idx"

        if map_col not in self.gdfs[child].columns:
            self._resolution_mapping(parent, child)

        if direction == "up":
            temp = self.gdfs[child].copy()
            ops = {}

            for c in cols:
                m = self.agg_method[c]
                if m.startswith("density_"):
                    denom = m.split("_", 1)[1]
                    temp[c] = temp[c] * temp[denom]
                    ops[c] = "sum"
                else:
                    ops[c] = m

            res = temp.groupby(map_col).agg(ops)
            self.gdfs[parent] = tgt_df.merge(res, left_index=True, right_index=True, how="left")

        else:
            temp = self.gdfs[parent][cols].copy()
            self.gdfs[child] = tgt_df.merge(temp, left_on=map_col, right_index=True, how="left")

        return True

    # -------------------------------------------------------------------------
    # Consistency
    # -------------------------------------------------------------------------

    def _fill_columns_internal(self) -> None:
        for _ in range(len(self.gdfs) + 2):
            changed = False

            for a, b in self.links:
                if self._propagate(a, b, "up"):
                    changed = True
            for a, b in self.links:
                if self._propagate(b, a, "down"):
                    changed = True

            if not changed:
                break

    # -------------------------------------------------------------------------
    # IO
    # -------------------------------------------------------------------------

    def save(self, path: Union[str, Path], overwrite: bool = True, extension: str = "geoparquet") -> Path:
        path = Path(path)
        directory = _ensure_dir(path, overwrite)

        fmt = extension.lower().lstrip(".")

        files = {}

        for k, gdf in self.gdfs.items():
            out = directory / f"layer_{k}"

            if fmt in ("geoparquet", "parquet"):
                file = f"{out}.geoparquet"
                gdf.to_parquet(file)
            else:
                driver = _GDF_DRIVERS[fmt]
                file = f"{out}.{fmt}"
                gdf.to_file(file, driver=driver)

            files[str(k)] = file

        _write_manifest(directory, {
            "class": "MultiresPolygonData",
            "agg_method": self.agg_method,
            "links": list(self.links),
            "name_to_res": self._name_to_res,
            "format": fmt,
            "files": files,
        })

        return directory

    @classmethod
    def load(cls, path: Union[str, Path]) -> "MultiresPolygonData":
        directory = Path(path)
        meta = _read_manifest(directory)

        obj = cls.__new__(cls)
        obj.agg_method = meta["agg_method"]
        obj.links = set(tuple(x) for x in meta["links"])
        obj._name_to_res = meta.get("name_to_res", {})
        obj.gdfs = {}

        for k, file in meta["files"].items():
            key = int(k) if k.isdigit() else k
            obj.gdfs[key] = gpd.read_file(directory / file)

        return obj

    # -------------------------------------------------------------------------
    # Debug
    # -------------------------------------------------------------------------

    def __repr__(self) -> str:
        layers = sorted([k for k in self.gdfs if isinstance(k, int)])
        return f"MultiresPolygonData(layers={layers})"