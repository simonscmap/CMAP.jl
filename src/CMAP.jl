
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
query,
cruises


end # module
