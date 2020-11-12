"""
    This module describe generic functions that could be eventually applied to 
    different datasets 
"""

# ============================================================================
# STANDARD  IMPORTS
# ============================================================================

from dataclasses import dataclass
import pandas as pd
import numpy as np
from itertools import repeat

# ============================================================================
# INTERNAL IMPORTS
# ============================================================================

from .constants import (
    COLUMNS_SPEED_POC,
    STANDARD_SPEED_COLUMNS,
    DCT_STD_SPEED_CSV,
    # Standard functions for columns
    standard_speed,
    average_velocity,
    stdev_velocity,
    derivative_sd_velocity,
    abs_derivative_sd_velocity,
    derivative_velocity,
    changes,
    detection,
)


COLUMNS_TIME = ["Time"]

# ============================================================================
# CLASS AND DEFINITIONS
# ============================================================================


def standardize_dataframe(dataExp):
    """ 
        This just fixes the column names so that they are familiar for all 
    """
    return dataExp.rename(columns=DCT_STD_SPEED_CSV)


def clean_data(dataExp):
    """
        This is a function to clean values starting from head of the platoons towards the tail. 
    """
    data_filtered = []
    for vehid in range(0, 5):
        data_filtered.append(dataExp[~dataExp[standard_speed(vehid)].isna()])

    # Concatenate and drop duplicates
    dataFilter = pd.concat(data_filtered).drop_duplicates()

    # Cliping values between 0 ~ 50 (for min speed)
    for vehid in range(0,5):
        dataFilter[standard_speed(vehid)].clip(0, 50, inplace=True)

    # Sorting values (normalement ce n'est pas utile)
    dataFilter.sort_values(by=["Time"], inplace=True)
    dataFilter.reset_index(drop = True)

    return dataFilter


def compute_statistics(dataExp, windowSize: int = 10):
    """ 
        Compute statistics from the speed variable. This script will compute statiscs for the speed variable for all the vehicles within the platoon. 

        The function computes: 

        * Moving average for the speeds 
        * Standard deviation in a defined horizon for moving average  (from Speed moving average)
        * Derivative/ Absolute derivative of standard deviation 
        * Derivative of speed (from Speed moving average)

        Args: 
            windowSize(int): Size of the moving average window. Fixed to Forward index for prediction capabilities
    """

    standardize_dataframe(dataExp)

    # Forward indexer (to account for k+h instead of classic k-h)
    indexer = pd.api.indexers.FixedForwardWindowIndexer(window_size=windowSize)

    for vehid, col in enumerate(STANDARD_SPEED_COLUMNS):
        # Find moving average speed
        dataExp[average_velocity(vehid)] = dataExp[col].rolling(window=indexer).mean()
        #dataExp[average_velocity(vehid)] = dataExp[average_velocity(vehid)].rolling(window=indexer).mean()

        # Find moving average standard deviation (from Avg. Speed)
        dataExp[stdev_velocity(vehid)] = dataExp[average_velocity(vehid)].rolling(window=indexer).std()

        # Find derivative of Std.
        dataExp[derivative_sd_velocity(vehid)] = dataExp[stdev_velocity(vehid)].diff()
        dataExp[abs_derivative_sd_velocity(vehid)] = dataExp[derivative_sd_velocity(vehid)].abs()

        # Find diff. Speed
        dataExp[derivative_velocity(vehid)] = dataExp[average_velocity(vehid)].diff().abs()

    return dataExp


def detect_changing_times(dataExp, indexerFuture):
    """
        Based on statistics this computes the transition times of the vehicles within the platoon:

        The function add the column `change_i` to denote the samples detected as changing samples.
    """
    FACTOR_SPEED_CHG = 95

    # For each veh in platoon
    for vehid in range(5):

        # Compute future window percentile over Abs(Diff(std)) ->
        dataPerc = (
            dataExp[abs_derivative_sd_velocity(vehid)]
            .rolling(window=indexerFuture)
            .apply(np.percentile, args=(FACTOR_SPEED_CHG,))
            .dropna()
        )

        # Select appropiate ones: Mark as true samples which 80 perc >
        dataExp[changes(vehid)] = dataPerc > dataExp[abs_derivative_sd_velocity(vehid)].std()
        dataExp[changes(vehid)].fillna(False, inplace=True)


def detect_transition_times(dataExp, windowForward=20):
    """
        Based on the detection of changing times it computes the samples that trigger the time samples
    """
    # Forward indexer (to account for k+h instead of classic k-h)
    indexerFuture = pd.api.indexers.FixedForwardWindowIndexer(window_size=windowForward)

    detect_changing_times(dataExp, indexerFuture)

    cols_changes = [changes(veh) for veh in range(5)]
    columns = COLUMNS_TIME + cols_changes

    dataDetections = dataExp[columns].copy()

    # dataDetections["Detection time"] = dataDetections["Time"]
    # dataDetections.set_index("Time", inplace=True)

    # For each veh in platoon
    platoonDetections = []
    for vehid in range(5):

        total = dataDetections[changes(vehid)].rolling(window=indexerFuture).sum()
        count = dataDetections[changes(vehid)].rolling(window=indexerFuture).count()
        ratio = total.divide(count).fillna(0)
        bool_ratio = ratio.astype(bool)
        int_detection = bool_ratio.rolling(2).agg(lambda x: not x.iloc[0] and x.iloc[1]).fillna(0)

        # Find speed variations greater than a threshold
        mask_positive_speed_rate = dataExp[derivative_velocity(vehid)] < dataExp[derivative_velocity(vehid)].std()
        final_mask = (int_detection * mask_positive_speed_rate).astype(bool)

        # Append dictionary {vehid: time} -> [:-1] to eliminate the last one which is regularly not due to real transition but due to switches betwen
        timesvehid = [{vehid: det_time} for det_time in dataDetections[final_mask]["Time"]]
        platoonDetections += timesvehid

        dataExp[detection(vehid)] = final_mask

    return pd.DataFrame(platoonDetections)


def consecutive_times(test_list, *args):
    """
        This function considers finding the times that are larger than the ones from the leader in a 
        set of detection times
    """

    # If no args set the first element of the list as a base comparator
    if not args:
        # To compare w.r.t leader no reference provided take 0 as a first reference to compare
        # args = (0,)
        # To compare w.r.t head no reference provided assign the head of the platoon
        args = (test_list[0][0],)

    if len(test_list) < 2:
        try:
            last_value = [t for t in test_list[0] if t > args[0] and t <= args[0] + 20][0]
            return [last_value]
        except IndexError:
            return []

    # If n-1
    try:
        # Looks for informaiton in the follower
        # current_t = [[t for t in test_list[0] if t >= args[0] and t <= args[0] + 20][0]]

        # Looks for information in the head of teh platoon
        valid_t = [t for t in test_list[0] if t >= args[-1] and t <= args[-1] + 20]
        values2compare = valid_t + [args[-1]]
        next_t = consecutive_times(test_list[1:], values2compare[0], values2compare[-1])
    except IndexError:
        current_t = []
        next_t = []

    return values2compare[:1] + next_t
