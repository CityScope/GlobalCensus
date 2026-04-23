from typing import Dict 
import pygris 

CENSUS_HIERARCHY = {"state": 0, "county": 1, "tract": 2, "blockgroup": 3, "block": 4}

SOURCE_MAPPING = {
    "acs5": "acs/acs5", 
    "acs1": "acs/acs1", 
    "decennial_dhc": "dec/dhc", 
    "lodes8_rac": "lodes/LODES8/rac", 
    "lodes8_wac": "lodes/LODES8/wac",
    "lodes7_rac": "lodes/LODES7/rac", 
    "lodes7_wac": "lodes/LODES7/wac",
}
    

CENSUS_LATEST_YEARS: Dict[str, int] = {
    "dec/dhc": 2020,  # Available for blocks
    "acs/acs5": 2023,  # Only block groups and higher
    "acs/acs1": 2024,  # Only places and higher
    "lodes/LODES7/rac": 2021,  # LODES7 data available through 2021
    "lodes/LODES7/wac": 2021,  # LODES7 data available through 2021
    "lodes/LODES8/rac": 2021,  # LODES8 data available through 2023 (may be incomplete for recent years)
    "lodes/LODES8/wac": 2021,  # LODES8 data available through 2023 (may be incomplete for recent years)
    
    # Geometries

    # Core census geographies
    "block": 2020,  # decennial only
    "blockgroup": 2023,
    "block group": 2023,
    "block_group": 2023,
    "tract": 2023,
    "place": 2023,
    "county": 2023,
    "counties": 2023,
    "state": 2023,
    "nation": 2023,

    # Statistical areas
    "division": 2023,
    "region": 2023,
    "core_based_statistical_area": 2023,
    "combined_statistical_area": 2023,
    "metro_division": 2023,
    "new_england": 2023,
    "puma": 2023,
    "urban_area": 2023,

    # Political / voting districts
    "congressional_district": 2023,
    "state_legislative_district": 2023,
    "voting_district": 2022,  # most recent consistent nationwide release

    # School / educational
    "school_district": 2023,

    # Tribal / native
    "native_area": 2023,
    "alaska_native_regional_corporation": 2023,
    "tribal_block_group": 2020,  # tied to decennial
    "tribal_blockgroup": 2020,  # tied to decennial
    "tribal_tract": 2020,
    "tribal_subdivisions_national": 2023,

    # Additional administrative
    "county_subdivision": 2023,
}

CENSUS_VALID_LEVELS: Dict[str, list] = {
    "dec/dhc": [
        "state",
        "county",
        "tract",
        "blockgroup",
        "block",
    ],  # Available for blocks
    "acs/acs5": [
        "state",
        "county",
        "tract",
        "blockgroup",
    ],  # Only block groups and higher
    "acs/acs1": [
        "state",
        "county",
        "place",
    ],  # Only places and higher
    "lodes/LODES7/rac": [
        "state",
        "county",
        "tract",
        "blockgroup",
        "block",
    ],  # LODES7 data available through 2021
    "lodes/LODES7/wac": [
        "state",
        "county",
        "tract",
        "blockgroup",
        "block",
    ],  # LODES7 data available through 2021
    "lodes/LODES8/rac": [
        "state",
        "county",
        "tract",
        "blockgroup",
        "block",
    ],  # LODES8 data available through 2023 (may be incomplete for recent years)
    "lodes/LODES8/wac": [
        "state",
        "county",
        "tract",
        "blockgroup",
        "block",
    ],  # LODES8 data available through 2023 (may be incomplete for recent years)
}

CENSUS_RESAMPLE = {
    # Clean hierarchy
    "blockgroup": "block",
    "tract": "blockgroup",
    "county": "tract",
    "state": "county",
    "nation": "state",

    # County-built statistical areas
    "core_based_statistical_area": "county",
    "combined_statistical_area": "county",
    "metro_division": "county",
    "division": "state",
    "region": "state",
    "new_england": "state",

    # Irregular boundaries (best possible without blocks)
    "place": "blockgroup",
    "urban_area": "blockgroup",
    "puma": "blockgroup",
    "congressional_district": "blockgroup",
    "state_legislative_district": "blockgroup",
    "voting_district": "blockgroup",
    "school_district": "blockgroup",
    "native_area": "blockgroup",
    "tribal_block_group": "blockgroup",
    "tribal_blockgroup": "blockgroup",
    "tribal_tract": "tract",
    "tribal_subdivisions_national": "blockgroup",
    "county_subdivision": "blockgroup",
    "alaska_native_regional_corporation": "county",
}

# Fetch census data
GEO_HIERARCHIES = {
    "block": ["state", "county", "tract", "block"],
    "blockgroup": ["state", "county", "tract", "block group"],
    "tract": ["state", "county", "tract"],
    "county": ["state", "county"],
    "state": ["state"],
}

# Prepare geography functions
GEOMETRY_FUNCS = {
    # Census geographies
    "block": pygris.blocks,
    "blockgroup": pygris.block_groups,
    "block group": pygris.block_groups,
    "block_group": pygris.block_groups,
    "tract": pygris.tracts,
    "place": pygris.places,
    "county": pygris.counties,
    "counties": pygris.counties,
    "state": pygris.states,
    "nation": pygris.nation,
    # Statistical areas
    "division": pygris.divisions,
    "region": pygris.regions,
    "core_based_statistical_area": pygris.core_based_statistical_areas,
    "combined_statistical_area": pygris.combined_statistical_areas,
    "metro_division": pygris.metro_divisions,
    "new_england": pygris.new_england,
    "puma": pygris.pumas,
    "urban_area": pygris.urban_areas,
    # Political / voting districts
    "congressional_district": pygris.congressional_districts,
    "state_legislative_district": pygris.state_legislative_districts,
    "voting_district": pygris.voting_districts,
    # School / educational
    "school_district": pygris.school_districts,
    # Tribal / native
    "native_area": pygris.native_areas,
    "alaska_native_regional_corporation": pygris.alaska_native_regional_corporations,
    "tribal_block_group": pygris.tribal_block_groups,
    "tribal_tract": pygris.tribal_tracts,
    "tribal_subdivisions_national": pygris.tribal_subdivisions_national,
    # Additional administrative
    "county_subdivision": pygris.county_subdivisions,
}


CENSUS_FIELDS = {
    # ============================================================
    # DECENNIAL DHC
    # ============================================================
    "decennial_dhc": {
        # TOTAL POPULATION
        "population": {
            "years": "latest",
            "fields": {
                "total": ["P1_001N"],
                "housingUnits": ["H1_001N"],
            },
            "fields_universe": {
                "default": "total",
                "total": "DENSITY_ONLY",
            },
            "agg_weights": {},
        },

        # GENDER
        "gender": {
            "years": "latest",
            "fields": {
                "total": ["P1_001N"],
                "male": ["P12_002N"],
                "female": ["P12_026N"],
            },
            "fields_universe": {"default": "total"},
            "agg_weights": {},
        },

        # AGE
        "age": {
            "years": "latest",
            "fields": {
                "total": ["P12_001N"],
                "Under18": [
                    "P12_003N","P12_004N","P12_005N","P12_006N",
                    "P12_027N","P12_028N","P12_029N","P12_030N",
                ],
                "18to64": [
                    "P12_007N","P12_008N","P12_009N","P12_010N",
                    "P12_011N","P12_012N","P12_013N","P12_014N",
                    "P12_015N","P12_016N","P12_017N","P12_018N",
                    "P12_019N",
                    "P12_031N","P12_032N","P12_033N","P12_034N",
                    "P12_035N","P12_036N","P12_037N","P12_038N",
                    "P12_039N","P12_040N","P12_041N","P12_042N",
                    "P12_043N",
                ],
                "Over65": [
                    "P12_020N","P12_021N","P12_022N","P12_023N",
                    "P12_024N","P12_025N",
                    "P12_044N","P12_045N","P12_046N","P12_047N",
                    "P12_048N","P12_049N",
                ],
            },
            "fields_universe": {"default": "total"},
            "agg_weights": {},
        },

        # RACE / ETHNICITY
        "race": {
            "years": "latest",
            "fields": {
                "total": ["P3_001N"],
                "white": ["P3_002N"],
                "nonWhite": ["P3_003N","P3_004N","P3_005N","P3_006N","P3_007N","P3_008N"],
                "black": ["P3_003N"],
                "native": ["P3_004N"],
                "asian": ["P3_005N"],
                "others": ["P3_006N","P3_007N","P3_008N"],
                "hispanic": ["P5_010N"],
                "hispanicOrNonWhite": [
                    "P5_004N","P5_005N","P5_006N",
                    "P5_007N","P5_008N","P5_009N","P5_010N",
                ],
            },
            "fields_universe": {"default": "total"},
            "agg_weights": {},
        },
    },

    # ============================================================
    # ACS 5-YEAR
    # ============================================================
    "acs5": {
        "households": {
            "years": "latest",
            "fields": {
                "total": ["B25044_001"],
                "owners": ["B25003_002"],
                "renters": ["B25003_003"],
                "meanSize": ["B25010_001"],
                "vacant": ["B25002_003"],
            },
            "fields_universe": {
                "default": "total",
                "total": "DENSITY_ONLY",
                "meanSize": "NO_DENSITY_OR_RATIO",
            },
            "agg_weights": {
                "meanSize": "total",
            },
        },

        # INCOME / POVERTY
        "income": {
            "years": "latest",
            "fields": {
                "population": ["B01003_001"],
                "populationPoverty": ["C17002_001"],
                "population16Plus": ["B23025_001"],
                "households": ["B25044_001"],
                "medianHousehold": ["B19013_001"],
                "meanCapita": ["B19301_001"],
                "poverty050": ["C17002_002"],
                "poverty100": ["C17002_002", "C17002_003"],
                "poverty150": ["C17002_002", "C17002_003", "C17002_004", "C17002_005"],
                "poverty200": [
                    "C17002_002","C17002_003","C17002_004",
                    "C17002_005","C17002_006","C17002_007",
                ],
                "unemployedCount": ["B23025_005"],
                "laborForce": ["B23025_003"],
            },
            "fields_universe": {
                "default": "population16Plus",
                "medianHousehold": "NO_DENSITY_OR_RATIO",
                "meanCapita": "NO_DENSITY_OR_RATIO",
                "poverty050": "populationPoverty",
                "poverty100": "populationPoverty",
                "poverty150": "populationPoverty",
                "poverty200": "populationPoverty",
                "unemployedCount": "laborForce",
                "laborForce": "population16Plus",
            },
            "agg_weights": {
                "medianHousehold": "households",
                "meanCapita": "population",
            },
        },

        # WORKERS (RESIDENCE)
        "workers_residence": {
            "years": "latest",
            "fields": {
                "total": ["B08301_001"],
                "transit": ["B08301_010"],
                "bus": ["B08301_011"],
                "rapidTransit": ["B08301_012", "B08301_014"],
                "commuterRail": ["B08301_013"],
                "car": ["B08301_002"],
                "walk": ["B08301_019"],
                "bike": ["B08301_018"],
                "walkBike": ["B08301_018", "B08301_019"],
                "otherModes": ["B08301_015","B08301_016","B08301_017","B08301_020"],
                "meanCommuteTime": ["B08303_001"],
                "meanTransitCommuteTime": ["B08136_001"],
            },
            "fields_universe": {
                "default": "total",
                "meanCommuteTime": "NO_DENSITY_OR_RATIO",
                "meanTransitCommuteTime": "NO_DENSITY_OR_RATIO",
            },
            "agg_weights": {
                "meanCommuteTime": "total",
                "meanTransitCommuteTime": "total",
            },
        }, 

        # VEHICLES
        "vehicles": {
            "years": "latest",
            "fields": {
                "households": ["B25044_001"],
                "total": ["B25046_001"],
                "0inHousehold": ["B25044_003", "B25044_010"],
                "0or1inHousehold": [
                    "B25044_003","B25044_004",
                    "B25044_010","B25044_011",
                ],
            },
            "fields_universe": {"default": "households"},
            "agg_weights": {},
        },
    },

    # ============================================================
    # LODES RAC
    # ============================================================
    "lodes8_rac": {
        "workers_residence": {
            "years": "latest",
            "fields": {
                "total": ["C000"],
                "lowIncome": ["CE01"],
                "midIncome": ["CE02"],
                "highIncome": ["CE03"],
                "young": ["CA01"],
                "primeAge": ["CA02"],
                "older": ["CA03"],
            },
            "fields_universe": {"default": "total"},
            "agg_weights": {},
        },
    },

    # ============================================================
    # LODES WAC
    # ============================================================
    "lodes8_wac": {
        "jobs_workplace_lodes": {
            "years": "latest",
            "fields": {
                "total": ["C000"],
                "lowIncome": ["CE01"],
                "midIncome": ["CE02"],
                "highIncome": ["CE03"],
                "young": ["CA01"],
                "primeAge": ["CA02"],
                "older": ["CA03"],
            },
            "fields_universe": {"default": "total"},
            "agg_weights": {},
        },
    },
}
