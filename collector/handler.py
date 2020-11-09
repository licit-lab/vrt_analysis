"""
    This is a module to provide support classes for reading:
    
    Example:
        To use the ``GetData`` declare in a string the ``path`` to the matfile ::
        
            >>> from collector.handler import DataHandler
            >>> x = GetData('data/raw/carma.data5.csv')

"""
# ============================================================================
# STANDARD  IMPORTS
# ============================================================================

import pandas as pd
from matplotlib import pyplot as plt

# ============================================================================
# INTERNAL IMPORTS
# ============================================================================

from .carma import CarmaData
from .poc import POCData
from .constants import COLUMNS_TIME
from .generic import (
    clean_data,
    standardize_dataframe,
    compute_statistics,
    detect_transition_times,
    consecutive_times,
    average_velocity,
    changes,
    detection,
)

# ============================================================================
# CLASS AND DEFINITIONS
# ============================================================================


class DataHandler:
    def __init__(self, csvpath=""):

        if "carma" in csvpath:
            self.datahandler = CarmaData(csvpath)
        else:
            self.datahandler = POCData(csvpath)
        self.datahandler._load_data_from_csv()
        self.data = self.datahandler._csvdata
        self._csvpath = self.datahandler._csvpath

    def __repr__(self):
        return repr(self.data)

    def compute_response_times(self, csvpath: str = ""):
        """
        Performs computation of the response times for a specific dataset, the
        full pipeline includes

        * loading data
        * cleaning data
        * computing statistics
        * computing transition times
            * (leader / follower)
            * (head / follower)

        """
        print("Standarizing data")
        self._standardize_data()
        print("Cleaning data")
        self._clean_data()
        print("Computing Statistics")
        self._compute_speed_statistics()
        print("Computing transition times")
        self._compute_transition_times()
        # print("Computing response time i/ i-1")
        # self._compute_leader_follower_times()
        # print("Computing response time 1/i")
        # self._compute_head_follower_times()

    def _standardize_data(self):
        """
        Standartize columns for data handling

        Check more info within the generic.py module
        """
        self.data = standardize_dataframe(self.data)

    def _clean_data(self):
        """
        Clean nan values in data

         Check more info within the generic.py module
        """
        self.data = clean_data(self.data)

    def _compute_speed_statistics(self):
        """
        Compute statistics for the speed variable.

        Check more info within the generic.py module
        """
        compute_statistics(self.data)

    def _compute_transition_times(self):
        """
        Compute transition times from the
        """
        print(f"Treating case: {self.datahandler._experiment}")
        self._transitiontimes = detect_transition_times(self.data)
        self._transitiontimes = pd.melt(
            self._transitiontimes, var_name="vehid"
        ).dropna()

    def _compute_reaction_timeinstants(self):
        """
        Retrieve reaction times from the transition times

        The function constructs a list of lists, the inner lists contains transition times for all vehicles in the platoon
        """
        lst_test = []

        for _, v in self._transitiontimes.groupby("vehid"):
            lst_test.append(list(v.value.values))

        reaction_instants = []
        leader_times = lst_test[0]
        for head_time in leader_times:
            reaction_instants.append(consecutive_times(lst_test, head_time))

        return [ri for ri in reaction_instants if len(ri) == 5]

    def _compute_leader_follower_times(self):
        """
        Compute the response time leader - follower
        """
        reaction_instants = self._compute_reaction_timeinstants()
        response_times = []
        for ri in reaction_instants:
            response_times += [
                {i: y - x} for i, x, y in zip(range(1, 5), ri[:-1], ri[1:])
            ]
        return pd.DataFrame(response_times)

    def _compute_head_follower_times(self):
        """
        Compute the response time head - follower
        """
        reaction_instants = self._compute_reaction_timeinstants()
        lead_times = []
        for ri in reaction_instants:
            lead_times += [{i: x - ri[0]} for i, x in zip(range(1, 5), ri[1:])]
        return pd.DataFrame(lead_times)

    # ============================================================================
    # Generic content probably for a general class to create heritage
    # ============================================================================

    def plot_speeds(self, **kwargs):
        """
        Custom plot of speeds
        """
        cols2plot = [average_velocity(veh) for veh in range(5)]
        return self.plot_curves(self.data, cols2plot, **kwargs)

    def plot_speeds_changes(self, **kwargs):
        """
        Plot speeds with changes
        """
        f, a = plt.subplots(1, 5, figsize=(25, 5))

        for vehid, ax in zip(range(5), a.flatten()):
            col2plot = [average_velocity(vehid)]
            self.plot_curves(
                self.data, col2plot, ax=ax, c="lightsteelblue", **kwargs
            )
            self.plot_curves(
                self.data[self.data[changes(vehid)]],
                col2plot,
                ax=ax,
                kind="scatter",
                c="r",
                **kwargs,
            )
        return f, a

    def plot_speed_timedetections(self, **kwargs):
        """
        Plot speeds with time detections
        """
        f, a = plt.subplots(1, 5, figsize=(25, 5))

        for vehid, ax in zip(range(5), a.flatten()):
            col2plot = [average_velocity(vehid)]
            self.plot_curves(
                self.data, col2plot, ax=ax, c="lightsteelblue", **kwargs
            )
            fltdata = self.data[self.data[detection(vehid)]]
            self.plot_curves(
                fltdata, col2plot, ax=ax, kind="scatter", c="r", **kwargs
            )

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
                >>> x.plot_curves(x.data,"0_Avg_Speed")

        """
        COLS2PLOT = COLUMNS_TIME + columns
        if not kwargs.get("ax", None):
            f, ax = plt.subplots(figsize=(10, 10))
            kwargs["ax"] = ax
            df2Plot[COLS2PLOT].plot(
                x="Time", title=f"{title}", grid=True, **kwargs
            )
            return f, ax
        else:
            df2Plot[COLS2PLOT].plot(x="Time", y=columns, grid=True, **kwargs)
        plt.tight_layout()
