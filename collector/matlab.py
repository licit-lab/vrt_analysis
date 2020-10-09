"""
    This is a module to provide support classes for reading:
    
    Example:
        To use the ``GetData`` declare in a string the ``path`` to the matfile ::
        
            >>> from collector.matlab import GetData
            >>> x = GetData('data/raw/mat/session_4_D_2018-09-03.mat')
            >>> x.vehicle_names
            >>> x.transform_data_vehicle('Prius1') 

"""

# ============================================================================
# STANDARD  IMPORTS
# ============================================================================

import scipy.io
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from dataclasses import dataclass, InitVar


class PlotClass:
    def plot(self, vary, **kwargs):
        f, a = plt.subplots(figsize=(7, 7))
        return (f, a)


@dataclass
class GetData(PlotClass):
    """
        This is a class to manipulate data from the matlab file. 

        Example:
            To use the ``GetData`` declare in a string the ``path`` to the matfile ::

                >>> from collector.matlab import GetData
                >>> x = GetData('data/raw/mat/session_4_D_2018-09-03.mat')
                >>> x.vehicle_names
                >>> x.transform_data_vehicle('Prius1')       
                
                
    """

    Ublox_GPS_driver_fix: pd.DataFrame
    Ublox_GPS_driver_fix_velocity: pd.DataFrame
    base_link_accel: pd.DataFrame
    vehicle_gear: pd.DataFrame
    vehicle_odom: pd.DataFrame
    vehicle_pedals: pd.DataFrame
    vehicle_steering_wheel: pd.DataFrame
    world_model_front_target: pd.DataFrame
    vehicle_hmi: pd.DataFrame
    vehicle_ControllerState: pd.DataFrame
    APU: pd.DataFrame
    convert_times: InitVar[bool] = False

    def __init__(self, matlab_path):
        self._mathpath = matlab_path
        self._matfile = scipy.io.loadmat(matlab_path)["Logset"]

    def transform_data_vehicle(self, vehicle: str = "Prius1"):
        """
            Transform data into a dictionary of values accessible 
            for future operations
            
            The dictionary contains this keys: 
            
            
            ('Ublox_GPS_driver_fix',
             'Ublox_GPS_driver_fix_velocity',
             'base_link_accel',
             'vehicle_gear',
             'vehicle_odom',
             'vehicle_pedals',
             'vehicle_steering_wheel',
             'world_model_front_target',
             'vehicle_hmi',
             'vehicle_ControllerState',
             'APU')
            
            Look at the log_set_description_file for more details
        """

        # Retrieve vehicle data
        vehicle_data = self._matfile[vehicle][0][0]
        self.datakeys = vehicle_data.dtype.names
        self._dict_data = {}
        for key in self.datakeys:
            self._dict_data[key] = self._get_dataframe(vehicle_data[0][0][key])
        self._format_data()

    def _get_dataframe(self, array):
        """
            Extracts data from array to transform into DataFrame
        """
        keys = array[0][0].dtype.names
        arraydata = {}
        for key in keys:
            arraydata[key] = array[key][0][0][0]

        return pd.DataFrame(arraydata)

    def _format_data(self):
        """ 
            Data to format time stamps
        """
        tc = lambda x: timedelta(seconds=x)
        if not self.convert_times:

            # Pick one of the variables
            key = "Ublox_GPS_driver_fix"  # datakeys[0]

            deltaT = (
                getattr(self, key)
                .time.diff()
                .fillna(0)
                .apply(tc)
                # + getattr(self, "APU").timestamp.values[0]
            )
            basedate = datetime.fromtimestamp(self.APU.timestamp.values[0])
            newdate = basedate + deltaT

            getattr(self, key).time = newdate

            # Interesting all time objects refer to the same time column so updating one is good enough

            self.APU.timestamp = basedate
            self.convert_times = True
            return

    @property
    def vehicle_names(self):
        """
            Return vehicle name data
        """
        return self._matfile.dtype.names

    @property
    def Ublox_GPS_driver_fix(self):
        return self._dict_data["Ublox_GPS_driver_fix"]

    @property
    def Ublox_GPS_driver_fix_velocity(self):
        return self._dict_data["Ublox_GPS_driver_fix"]

    @property
    def base_link_accel(self):
        return self._dict_data["base_link_accel"]

    @property
    def vehicle_gear(self):
        return self._dict_data["vehicle_gear"]

    @property
    def vehicle_odom(self):
        return self._dict_data["vehicle_odom"]

    @property
    def vehicle_pedals(self):
        return self._dict_data["vehicle_pedals"]

    @property
    def vehicle_steering_wheel(self):
        return self._dict_data["vehicle_steering_wheel"]

    @property
    def world_model_front_target(self):
        return self._dict_data["world_model_front_target"]

    @property
    def vehicle_hmi(self):
        return self._dict_data["vehicle_hmi"]

    @property
    def vehicle_ControllerState(self):
        return self._dict_data["vehicle_ControllerState"]

    @property
    def APU(self):
        return self._dict_data["APU"]
