import math
import pandas as pd

class GEO_Map:
    """
    Holds the map for zip code and its latitude and longitude.
    """
    __instance = None

    @staticmethod
    def get_instance():
        """Static access method."""
        if GEO_Map.__instance is None:
            GEO_Map()
        return GEO_Map.__instance

    def __init__(self):
        """Virtually private constructor."""
        if GEO_Map.__instance is not None:
            raise Exception("This class is a singleton!")
        else:
            GEO_Map.__instance = self
            # Read the CSV file
            self.map = pd.read_csv("hdfs:///sharma/uszipsv.csv", header=None, names=['A', 'B', 'C', 'D', 'E'])
            self.map['A'] = self.map['A'].astype(int)
            self.map['B'] = pd.to_numeric(self.map['B'], errors='coerce')  # Latitude
            self.map['C'] = pd.to_numeric(self.map['C'], errors='coerce')  # Longitude

    def get_lat(self, pos_id):
        """Return latitude for a given postcode."""
        result = self.map[self.map.A == pos_id]
        if result.empty:
            print(f"Latitude for postcode {pos_id} not found.")
            return None
        return result.B.iloc[0]

    def get_long(self, pos_id):
        """Return longitude for a given postcode."""
        result = self.map[self.map.A == pos_id]
        if result.empty:
            print(f"Longitude for postcode {pos_id} not found.")
            return None
        return result.C.iloc[0]

    def distance(self, lat1, long1, lat2, long2):
        """Calculate the distance between two points on the Earth."""
        if None in [lat1, long1, lat2, long2]:
            print(f"Missing coordinates: lat1={lat1}, long1={long1}, lat2={lat2}, long2={long2}")
            return float('nan')  # Return NaN if any coordinate is missing

        # Check if coordinates are within valid range
        if not (-90 <= lat1 <= 90 and -180 <= long1 <= 180 and -90 <= lat2 <= 90 and -180 <= long2 <= 180):
            print(f"Invalid coordinates: lat1={lat1}, long1={long1}, lat2={lat2}, long2={long2}")
            return float('nan')

        theta = long1 - long2
        dist = (math.sin(self.deg2rad(lat1)) * math.sin(self.deg2rad(lat2)) +
                math.cos(self.deg2rad(lat1)) * math.cos(self.deg2rad(lat2)) * math.cos(self.deg2rad(theta)))
        dist = min(1.0, max(-1.0, dist))
        dist = math.acos(dist)
        dist = self.rad2deg(dist)
        dist = dist * 60 * 1.1515 * 1.609344  # Convert to kilometers
        return dist

    def rad2deg(self, rad):
        """Convert radians to degrees."""
        return rad * 180.0 / math.pi

    def deg2rad(self, deg):
        """Convert degrees to radians."""
        return deg * math.pi / 180.0
