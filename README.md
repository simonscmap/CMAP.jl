![Cover](pic/CMAP.png)

<p>
  <a href="https://github.com/simonscmap/CMAP.jl/actions">
    <img src="https://img.shields.io/github/workflow/status/simonscmap/CMAP.jl/Mac%20OS%20X?label=OSX&logo=Apple&logoColor=white&style=flat-square">
  </a>
  <a href="https://github.com/simonscmap/CMAP.jl/actions">
    <img src="https://img.shields.io/github/workflow/status/simonscmap/CMAP.jl/Linux?label=Linux&logo=Linux&logoColor=white&style=flat-square">
  </a>
  <a href="https://github.com/simonscmap/CMAP.jl/actions">
    <img src="https://img.shields.io/github/workflow/status/simonscmap/CMAP.jl/Windows?label=Windows&logo=Windows&logoColor=white&style=flat-square">
  </a>
  <a href="https://codecov.io/gh/simonscmap/CMAP.jl">
    <img src="https://img.shields.io/codecov/c/github/simonscmap/CMAP.jl/master?label=Codecov&logo=codecov&logoColor=white&style=flat-square">
  </a>
</p>

# cmap.jl

CMAP.jl is the Julia client for Simons CMAP project. It provides access to the Simons CMAP database where various ocean data sets are hosted. These data sets include multi decades of remote sensing observations (satellite), numerical model estimates, and field expeditions.

This package is adopted from [pycmap](https://github.com/simonscmap/pycmap), which is the python client of Simons CMAP ecosystem.

## Documentation

To be filled out.

## Usage example

1. Set your Simons API key.

    To make API requests, you need to get an API key from [Simons CMAP website](https://simonscmap.com). Once you have your API key, run the following command to store the API key on your local machine:

    ```julia
    set_api_key("your api key")
    ```


1. Get the list of data sets:

    ```julia
    datasets()
    ```

1. Retrieve a subset of sea surface temperature measured by satellite.

    ```julia
    space_time(
               table = "tblArgoMerge_REP",
               variable = "argo_merge_salinity_adj",
               dt1 = "2015-05-01",
               dt2 = "2015-05-30",
               lat1 = 28.1,
               lat2 = 35.4,
               lon1 = -71.3,
               lon2 = -50,
               depth1 = 0,
               depth2 = 100
              )
    ```

