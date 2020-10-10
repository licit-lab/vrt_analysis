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
from sodapy import Socrata
from decouple import config
from tqdm import tqdm

API_KEY = config("KEY")
DATASET_ID = "wpek-zziu"

# Required only for creating/ modyfing data
# API_USERNAME = config("USER")
# API_PASS = config("PASS")

# ============================================================================
# CLASS AND DEFINITIONS
# ============================================================================


@dataclass
class GetData:
    """
        This is a class to manipulate data from the CARMA csv file. 

        Example:
            To use the ``GetData`` declare in a string the ``path`` to the csv file ::

                >>> from collector.carma import GetData
                >>> x = GetData('data/raw/carma/data5.csv')
                >>> x.     
               
    """

    def __init__(self, csv_path):
        self._csvpath = csv_path
        self._csvdata = pd.read_csv(csv_path)
        self._client = Socrata("data.transportation.gov", API_KEY)

    def get_request(self, query):
        with self._client as c:
            results = c.get(DATASET_ID, query=query)
        return results

    def get_allruns(self):
        return self.get_request(self.ALL_RUNS)

    def get_features(self):
        runs = self.get_allruns()
        runsdf = pd.DataFrame(runs)
        query_runs = []
        features = []
        # Query features per run.
        for k, v in tqdm(
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
        for k, v in self._dfFeat.iterrows():
            finalFeatures.update(v.features)
        # Find missing ones
        self._dfFeat["missingFeatures"] = self._dfFeat.apply(
            lambda x: finalFeatures.difference(x["features"]), axis=1
        )

    @property
    def ALL_RUNS(self):
        return """
            SELECT DISTINCT run
            """

    def QUERY_RUN(self, runid):
        return f"""
            SELECT * WHERE run={runid} LIMIT 1
            """
