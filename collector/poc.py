"""
    This is a module to provide support classes for reading:
    
    Example:
        To use the ``GetData`` declare in a string the ``path`` to the matfile ::
        
            >>> from collector.matlab import GetData
            >>> x = GetData('data/raw/carma.data5.csv')

"""

# ============================================================================
# STANDARD  IMPORTS
# ============================================================================

import pandas as pd
from dataclasses import dataclass
from decouple import config
from tqdm import tqdm
from sodapy import Socrata

# ============================================================================
# CLASS AND DEFINITIONS
# ============================================================================

API_KEY = config("KEY")
DATASET_ID = "wpek-zziu"
# Required only for creating/ modyfing data
# API_USERNAME = config("USER")
# API_PASS = config("PASS")

# ============================================================================
# INTERNAL IMPORTS
# ============================================================================

from .constants import (
    COLUMNS_SPACING_POC,
    COLUMNS_SPEED_POC,
    COLUMNS_TIME_POC,
    average_velocity,
    changes,
    detection,
)


@dataclass
class POCData:
    """
    This is a class to manipulate data from the POC Data.

    Example:
        To use the ``GetData`` declare in a string the ``path`` to the csv file ::

            >>> from collector.carma import GetData
            >>> x = GetData('data/raw/carma/data5.csv')
            >>> x.

    """

    def __init__(self, csv_path: str = ""):
        self._csvpath = csv_path
        self._client = Socrata("data.transportation.gov", API_KEY)

    def __repr__(self):
        return f"{self.__class__.__name__}({self._csvpath})"

    def get_request(self, query):
        """
        Performs a query to the online DB. Check the SQL syntax here

        https://dev.socrata.com/docs/queries/

        Example:

        To use just call::

            >>> experiment = GetData()
            >>> experiment.get_allruns()
        """
        with self._client as c:
            results = c.get(DATASET_ID, query=query)
        return results

    def get_allruns(self):
        """
        Get all runs from the experiment from the online DB

        Example:

        To use just call::

            >>> experiment = GetData()
            >>> experiment.get_allruns()
        """
        return self.get_request(self.ALL_RUNS)

    def get_features(self):
        """
        This function obtains features from the real online DB sodapy

        More info at this DoT Dataset_.

        .. _DoT Dataset: https://data.transportation.gov/Automobiles/Test-Data-of-Proof-of-Concept-Vehicle-Platooning-B/wpek-zziu


        """
        runs = self.get_allruns()
        runsdf = pd.DataFrame(runs)
        query_runs = []
        features = []

        # Query features per run.
        for _, v in tqdm(
            runsdf.iterrows(), total=len(runs), desc="Downloading Metadata"
        ):
            query = self.QUERY_RUN(v.run)
            q = self.get_request(query)
            feature = set(q[0].keys())
            query_runs.append(q)
            features.append(
                {"run": v.run, "features": feature, "n_features": len(feature)}
            )
        self._dfFeat = pd.DataFrame(features)

        # Find missing features w.r.t total
        finalFeatures = set()
        # Get total features
        for _, v in self._dfFeat.iterrows():
            finalFeatures.update(v.features)
        # Find missing ones
        self._dfFeat["missingFeatures"] = self._dfFeat.apply(
            lambda x: finalFeatures.difference(x["features"]), axis=1
        )

    @property
    def ALL_RUNS(self):
        """
        SQL query to get all runs
        """
        return """
            SELECT DISTINCT run
            """

    def QUERY_RUN(self, runid):
        """
        SQL Query to get a single run
        """
        return f"""
            SELECT * WHERE run={runid} LIMIT 1
            """

    # ============================================================================
    # LOCAL METHODS
    # ============================================================================

    def _load_data_from_csv(self, csv_path: str = ""):
        """
        Load csv data from the full path.

        Examples:
            Loading a file in `csvpath`::

                >>> x = POCData()
                >>> x._load_data_from_csv(csvpath)
        """
        self._csvpath = csv_path if not self._csvpath else self._csvpath
        self._experiment = self._csvpath.split("/")[-1].split(".")[-2]
        cols_to_load = COLUMNS_SPACING_POC + COLUMNS_SPEED_POC + COLUMNS_TIME_POC
        self._csvdata = pd.read_csv(self._csvpath, usecols=cols_to_load, sep=",", decimal=".")
        
        #self._csvdata[["Day", "Heure"]] = self._csvdata[
         #   "bin_utc_time_formatted"
        #].str.split(" ", expand=True)
        self._csvdata["Time"] = self._csvdata["elapsed_time (s)"]
         #   pd.to_datetime(self._csvdata["Heure"])
         #   .diff()
         #   .fillna(pd.Timedelta(milliseconds=50))
        #).apply(lambda x: x.total_seconds())
