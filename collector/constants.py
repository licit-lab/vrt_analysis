"""
    Constants and generic values 
"""

# Column namaes datasets in data/raw/carma

COLUMNS_SPACING_CARMA = [f"follower{i}_radar1" for i in (1, 2, 3, 4)]
COLUMNS_SPEED_CARMA = ["leader_GPS_CARMA_speed"] + [f"follower{i}_GPS_CARMA_speed" for i in (1, 2, 3, 4)]
COLUMNS_SPACING_POC = [f"distToPVeh_CACC_follower{i}" for i in (1, 2, 3, 4)]
COLUMNS_SPEED_POC = ["velocity_fwd_PINPOINT_leader"] + [f"velocity_fwd_PINPOINT_follower{i}" for i in (1, 2, 3, 4)]
COLUMNS_TIME_CARMA = ["Time", "Heure"]
COLUMNS_TIME_POC = [
    "bin_utc_time_formatted",
]
COLUMNS_TIME = ["Time"]

# Standard column names
STANDARD_SPEED_COLUMNS = [f"Speed - {i}" for i in range(5)]
DCT_STD_SPEED_CSV = dict(zip(COLUMNS_SPEED_CARMA + COLUMNS_SPEED_POC, STANDARD_SPEED_COLUMNS + STANDARD_SPEED_COLUMNS))

# Processed derived columns
standard_speed = lambda veh_id: STANDARD_SPEED_COLUMNS[veh_id]
average_velocity = lambda veh_id: f"{veh_id}_Avg_Speed"
derivative_velocity = lambda veh_id: f"{veh_id}_Diff_Speed"
stdev_velocity = lambda veh_id: f"{veh_id}_Std_Speed"
derivative_sd_velocity = lambda veh_id: f"{veh_id}_Diff_Std_Speed"
abs_derivative_sd_velocity = lambda veh_id: f"{veh_id}_Abs_Diff_Std_Leader_Speed"
changes = lambda veh_id: f"{veh_id}_Change"
detection = lambda veh_id: f"{veh_id}_Detection"
