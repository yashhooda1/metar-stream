"""Small hand-built documents in the shape of ERCOT's dashboard JSON
(supply-demand, fuel-mix, systemWidePrices), trimmed to a few intervals."""

SUPPLY_DEMAND = {
    "lastUpdated": "2026-10-08 16:05:00-0500",
    "data": [
        {"capacity": 73115, "demand": 53588, "forecast": 0, "dstFlag": 0, "interval": 0,
         "hourEnding": 0, "timestamp": "2026-10-08 00:00:00-0500", "epoch": 1791435600000},
        {"capacity": 73001, "demand": 53210, "forecast": 0, "dstFlag": 0, "interval": 1,
         "timestamp": "2026-10-08 00:05:00-0500", "available": 97637},
        {"capacity": 0, "demand": 61000, "forecast": 1, "timestamp": "2026-10-08 23:55:00-0500"},
        {"capacity": 70000, "demand": None, "forecast": 0, "timestamp": "2026-10-08 00:10:00-0500"},
        {"capacity": 70000, "demand": 50000, "forecast": 0, "timestamp": "not a time"},
        # later today, after lastUpdated: no demand yet, and not an error
        {"capacity": 0, "demand": None, "forecast": 0, "timestamp": "2026-10-08 22:00:00-0500"},
    ],
    "forecast": [
        {"deliveryDate": "2026-10-09", "dstFlag": "N", "hourEnding": 1,
         "availCapGen": 80018, "forecastedDemand": 52995,
         "timestamp": "2026-10-09 01:00:00-0500", "epoch": 1791525600000},
    ],
}

FUEL_MIX = {
    "lastUpdated": "2026-10-08 05:06:00-0500",
    "monthlyCapacity": {"Wind": 40405, "Solar": 40226},
    "data": {
        "2026-10-07": {
            "2026-10-07 00:00:00-0500": {
                "Coal and Lignite": {"gen": 6287.0},
                "Hydro": {"gen": 0},
                "Nuclear": {"gen": 5010.0},
                "Other": {"gen": 50.8},
                "Power Storage": {"gen": -769.35},
                "Solar": {"gen": 0.04},
                "Wind": {"gen": 10676.66},
                "Natural Gas": {"gen": 31589.56},
                "Power Storage Charging": {"gen": -769.35},
                "Power Storage Discharging": {"gen": 0},
            },
            "2026-10-07 00:05:00-0500": {
                "Wind": {"gen": 10700.0},
                "Natural Gas": {"gen": "n/a"},
            },
        }
    },
}

PRICES = {
    "lastUpdated": "2026-10-09 00:20:00-0500",
    "rtSppData": [
        {"intervalEnding": "00:15", "dstFlag": "N", "hbBusAvg": 44.73, "hbHubAvg": 46.7,
         "hbHouston": 38.54, "hbNorth": 41.25, "hbPan": 31.4, "hbSouth": 50.86, "hbWest": 56.13,
         "lzAen": 47.93, "lzCps": 52.43, "lzHouston": 40.7, "lzLcra": 49.21, "lzNorth": 41.35,
         "lzRaybn": 40.88, "lzSouth": 57.7, "lzWest": 65.55,
         "timestamp": "2026-10-09 00:15:00-0500", "interval": 1791522900000},
    ],
    "damSppData": [
        {"hourEnding": 1, "dstFlag": "N", "hbHubAvg": 50.07, "hbHouston": 47.0, "lzWest": 63.12,
         "timestamp": "2026-10-09 01:00:00-0500", "interval": 1791525600000},
    ],
}
