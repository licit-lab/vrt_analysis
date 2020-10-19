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
from matplotlib import pyplot as plt

# ============================================================================
# INTERNAL IMPORTS
# ============================================================================

from .constants import (
    COLUMNS_SPACING_POC,
    COLUMNS_SPACING_CARMA,
    COLUMNS_SPEED_CARMA,
    COLUMNS_SPACING_CARMA,
    COLUMNS_SPEED_POC,
    COLUMNS_TIME_CARMA,
    COLUMNS_TIME_POC,
    average_velocity,
    changes,
    detection,
)
from .generic import standardize_dataframe, compute_statistics, detect_transition_times, consecutive_times

# ============================================================================
# CLASS AND DEFINITIONS
# ============================================================================


@dataclass
class CarmaData:
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

    def __repr__(self):
        return f"{self.__class__.__name__}({self._csvpath})"

    # ============================================================================
    # LOCAL METHODS
    # ============================================================================

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
        cols_to_load = COLUMNS_SPACING_CARMA + COLUMNS_SPEED_CARMA + COLUMNS_TIME_CARMA
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
