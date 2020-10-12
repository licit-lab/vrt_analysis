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
from matplotlib import pyplot as plt

# ============================================================================
# INTERNAL IMPORTS
# ============================================================================

from .constants import COLUMNS_SPACING, COLUMNS_SPEED, COLUMNS_TIME, average_velocity, changes, detection
from .generic import standardize_dataframe, compute_statistics, detect_transition_times

# ============================================================================
# CLASS AND DEFINITIONS
# ============================================================================

API_KEY = config("KEY")
DATASET_ID = "wpek-zziu"
# Required only for creating/ modyfing data
# API_USERNAME = config("USER")
# API_PASS = config("PASS")


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
        for _, v in tqdm(runsdf.iterrows(), total=len(runs), desc="Downloading Metadata"):
            query = self.QUERY_RUN(v.run)
            q = self.get_request(query)
            feature = set(q[0].keys())
            query_runs.append(q)
            features.append({"run": v.run, "features": feature, "n_features": len(feature)})
        self._dfFeat = pd.DataFrame(features)

        # Find missing features w.r.t total
        finalFeatures = set()
        # Get total features
        for _, v in self._dfFeat.iterrows():
            finalFeatures.update(v.features)
        # Find missing ones
        self._dfFeat["missingFeatures"] = self._dfFeat.apply(lambda x: finalFeatures.difference(x["features"]), axis=1)

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

    def compute_response_times(self, csvpath: str = ""):
        """ 
            Performs computation of the response times for a specific dataset, the 
            full pipeline includes 

            * loading data
            * clenaing data 
            * computing statistics 
            * computing transition times 
            * computing response times 
                * (leader / follower) 
                * (head / follower)

        """
        self._load_data_from_csv(csvpath)
        self._clean_data()
        self._compute_speed_statistics()
        self._compute_transition_times()
        self._compute_leader_follower_times()
        self._compute_head_follower_times()

    def _load_data_from_csv(self, csv_path: str = ""):
        """
            Load csv data from the full path. 

            Examples: 
                Loading a file in `csvpath`::

                    >>> x = GetData()
                    >>> x._load_data_from_csv(matlab_path)
        """
        self._csvpath = csv_path if not self._csvpath else self._csvpath
        self._experiment = self._csvpath.split("/")[-1].split(".")[-2]
        cols_to_load = COLUMNS_SPACING + COLUMNS_SPEED + COLUMNS_TIME
        self._csvdata = pd.read_csv(self._csvpath, usecols=cols_to_load, sep=",", decimal=".")

    def _distance_to_leader(self):
        """ 
            From available local processed datasets define the function computes the headway spacing: 
        """

        def clean_col(df):
            for i in (1, 2, 3, 4):
                if i == 1:
                    df[f"distToLeader_f{i}"] = df[f"follower{i}_radar1"]
                else:
                    df[f"distToLeader_f{i}"] = df[f"follower{i-1}_radar1"] + df[f"follower{i}_radar1"]

        self._csvdata.apply(clean_col, axis=0)

    def _clean_data(self):
        """
            This is a script to clean values starting from head of the platoons towards the tail. 
        """

        # Cleaning speeds

        # Clean leader data
        lead_clean = self._csvdata[~self._csvdata["leader_GPS_CARMA_speed"].isna()]
        follower_clean = lead_clean.copy()
        lst_datas_flt = [follower_clean]

        # Clean follower data (recursively)
        for i in range(1, 5):
            follower_clean = follower_clean[~follower_clean[f"follower{i}_GPS_CARMA_speed"].isna()]
            lst_datas_flt.append(follower_clean)

        # Concatenate and drop duplicates
        dfFilter = pd.concat(lst_datas_flt).drop_duplicates()

        # Cliping values between 0 ~ 50 (for min speed)
        dfCliped = dfFilter.copy()
        dfCliped["leader_GPS_CARMA_speed"] = dfCliped["leader_GPS_CARMA_speed"].clip(0, 50)

        # Sorting values
        dfSorted = dfCliped.reset_index().sort_values(by=["Heure", "Time"])

        # Reassigning to csvdata
        self._csvdata = dfSorted

    def _compute_speed_statistics(self):
        """
            Compute statistics for the speed variable. 

            Check more info within the generic.py module
        """
        compute_statistics(self._csvdata)

    def _compute_transition_times(self):
        """
            Compute transition times from the 
        """
        print(f"Treating case: {self._experiment}")
        self._transitiontimes = detect_transition_times(self._csvdata)
        self._transitiontimes = pd.melt(self._transitiontimes, var_name="vehid").dropna()

    def _compute_leader_follower_times(self):
        return

    def _compute_head_follower_times(self):
        return

    # ============================================================================
    # Generic content probably for a general class to create heritage
    # ============================================================================

    def plot_speeds(self, **kwargs):
        """
            Custom plot of speeds
        """
        cols2plot = [average_velocity(veh) for veh in range(5)]
        return self.plot_curves(self._csvdata, cols2plot, **kwargs)

    def plot_speeds_changes(self, **kwargs):
        """
            Plot speeds with changes 
        """
        f, a = plt.subplots(1, 5, figsize=(25, 5))

        for vehid, ax in zip(range(5), a.flatten()):
            col2plot = [average_velocity(vehid)]
            self.plot_curves(self._csvdata, col2plot, ax=ax, c="lightsteelblue", **kwargs)
            self.plot_curves(
                self._csvdata[self._csvdata[changes(vehid)]], col2plot, ax=ax, kind="scatter", c="r", **kwargs
            )
        return f, a

    def plot_speed_timedetections(self, **kwargs):
        """
            Plot speeds with time detections
        """
        f, a = plt.subplots(1, 5, figsize=(25, 5))

        for vehid, ax in zip(range(5), a.flatten()):
            col2plot = [average_velocity(vehid)]
            self.plot_curves(self._csvdata, col2plot, ax=ax, c="lightsteelblue", **kwargs)
            fltdata = self._csvdata[self._csvdata[detection(vehid)]]
            self.plot_curves(fltdata, col2plot, ax=ax, kind="scatter", c="r", **kwargs)

    @staticmethod
    def plot_curves(df2Plot, columns, title="", **kwargs):
        """ 
            Plot data of a specific variable determined by columns.  A set of columns is also admissible 

            Example: 
                To plot a set of speeds::

                    >>> x = GetData()
                    >>> x._load_data_from_csv(matlab_path)
                    >>> x._clean_data()
                    >>> x._compute_speed_statistics()
                    >>> x.plot_curves(x._csvdata,"0_Avg_Speed")

        """
        COLS2PLOT = COLUMNS_TIME + columns
        if not kwargs.get("ax", None):
            f, ax = plt.subplots(figsize=(10, 10))
            kwargs["ax"] = ax
            df2Plot[COLS2PLOT].plot(x="Time", title=f"{title}", grid=True, **kwargs)
            return f, ax
        else:
            df2Plot[COLS2PLOT].plot(x="Time", y=columns, grid=True, **kwargs)
        plt.tight_layout()
