# %%
%load_ext autoreload
%autoreload 2
#%%
from collector.handler import DataHandler

# %%
csvpath = "data/raw/carma/data6.csv"
experiment1 = DataHandler(csvpath)
csvpath = "data/raw/poc/data28_mieux.csv"
experiment2 = DataHandler(csvpath)

#%%
experiment1._standardize_data()
experiment2._standardize_data()

#%% 
experiment1._clean_data()
# experiment2._clean_data()

#%%
experiment.compute_response_times()

#%%
experiment._transitiontimes

# %%
experiment.plot_speed_timedetections()
# %%
from collector.generic import consecutive_times

lst_test = []
for k, v in experiment._transitiontimes.groupby("vehid"):
    lst_test.append(list(v.value.values))

reaction_instants = []
for head_time in lst_test[0]:
    reaction_instants.append(consecutive_times(lst_test, lst_test[0][1]))

# %%
experiment._compute_leader_follower_times()

# %%
experiment._compute_head_follower_times()
# %%
