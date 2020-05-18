
"""
Author: Mohammad Dehghani Ashkezari <mdehghan@uw.edu>

Date: 2020-05-07

Function: Exposes CMAP RESTful API methods.
"""


module CMAP

greet() = print("Welcome to the Simons CMAP Julia client!")

include("./metaMethods.jl")
include("./dataMethods.jl")


export 
set_api_key,
get_api_key,
API,
query,
get_catalog,
search_catalog,
datasets,
head,
columns,
get_dataset_ID,
get_dataset_metadata,
get_var_catalog,
get_var_long_name,
get_unit,
get_var_resolution,
get_var_coverage,
get_var_stat,
has_field,
is_grid,
is_climatology,
get_references,
get_metadata,
cruises,
cruise_by_name,
cruise_bounds,
cruise_trajectory,
cruise_variables,
get_dataset,
space_time,
time_series,
depth_profile,
section,
match,
along_track



end # module
