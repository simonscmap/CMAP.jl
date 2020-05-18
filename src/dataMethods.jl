



include("./rest.jl")



"""
get_dataset(tableName::String)

Returns the entire dataset.
It is not recommended to retrieve datasets with more than 100k rows using this method.
For large datasets, please use the 'space_time' method and retrieve the data in smaller chunks.
Note that this method does not return the dataset metadata. 
Use the 'get_dataset_metadata' method to get the dataset metadata.

Examples
≡≡≡≡≡≡≡≡≡≡
get_dataset("tblHOT_LAVA")

"""
function get_dataset(tableName::String)
datasetID = get_dataset_ID(tableName);
maxRow = 2000000;
api = API();
status, response = query(api, "SELECT JSON_stats FROM tblDataset_Stats WHERE Dataset_ID=$datasetID");
jDict = JSON.parse(response.JSON_stats[1]);
rows = parse(Int, jDict["lat"]["count"]);
if rows > maxRow
    msg = "\nThe requested dataset has $rows records.\n";
    msg *= "It is not recommended to retrieve datasets with more than $maxRow rows using this method.\n";
    msg *= "For large datasets, please use the 'space_time' method and retrieve the data in smaller chunks.\n"; 
    error(msg);
end 
status, response = query(api, "SELECT * FROM $tableName")
return response
end 



"""
    _subset(spName::String, 
            table::String, 
            variable::String, 
            dt1::String, 
            dt2::String, 
            lat1::Float64,
            lat2::Float64, 
            lon1::Float64, 
            lon2::Float64, 
            depth1::Float64, 
            depth2::Float64
           )

Returns a subset of data according to space-time constraints.
This method is meant to be used internally.

Examples
≡≡≡≡≡≡≡≡≡≡
_subset("uspSpaceTime", "tblAltimetry_REP", "sla", "2016-01-01", "2016-01-01", 30., 31., -160., -159., 0., 0.);

"""
function _subset(spName::String, 
                table::String, 
                variable::String, 
                dt1::String, 
                dt2::String, 
                lat1::Float64,
                lat2::Float64, 
                lon1::Float64, 
                lon2::Float64, 
                depth1::Float64, 
                depth2::Float64
                )
    statement = "EXEC $spName '$table', '$variable', '$dt1', '$dt2', $lat1, $lat2, $lon1, $lon2, $depth1, $depth2"; 
    api = API();
    status, response = query(api, statement);
    return response
end 



"""
    _interval_to_uspName(interval::String) 

Returns a timeseries-based stored procedure name according to the specified interval.
This method is meant to be used internally.

Examples
≡≡≡≡≡≡≡≡≡≡
_interval_to_uspName("week");

"""
function _interval_to_uspName(interval::String) 
    usp = "";
    if interval == ""
        usp = "uspTimeSeries"
    elseif interval in ["w", "week", "weekly"]   
        usp = "uspWeekly"
    elseif interval in ["m", "month", "monthly"]   
        usp = "uspMonthly"
    elseif interval in ["q", "s", "season", "seasonal", "seasonality", "quarterly"]   
        usp = "uspQuarterly"
    elseif interval in ["a", "y", "year", "yearly", "annual"]   
        usp = "uspAnnual"
    else
        error("Invalid interval: $interval")   
    end       
    return usp
end 



"""
    space_time(;
               table::String, 
               variable::String, 
               dt1::String, 
               dt2::String, 
               lat1::Number,
               lat2::Number, 
               lon1::Number, 
               lon2::Number, 
               depth1::Number, 
               depth2::Number
              )

Returns a subset of data according to the specified space-time constraints (dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2). 
The results are ordered by time, lat, lon, and depth (if exists), respectively.

Examples
≡≡≡≡≡≡≡≡≡≡
space_time(
           table="tblArgoMerge_REP", 
           variable="argo_merge_salinity_adj", 
           dt1="2015-05-01", 
           dt2="2015-05-30", 
           lat1=28.1, 
           lat2=35.4, 
           lon1=-71.3, 
           lon2=-50, 
           depth1=0, 
           depth2=100
           )

"""
function space_time(;
                    table::String, 
                    variable::String, 
                    dt1::String, 
                    dt2::String, 
                    lat1::Number,
                    lat2::Number, 
                    lon1::Number, 
                    lon2::Number, 
                    depth1::Number, 
                    depth2::Number
                    )
    return _subset("uspSpaceTime",
                   table, 
                   variable, 
                   dt1, 
                   dt2, 
                   float(lat1),
                   float(lat2), 
                   float(lon1), 
                   float(lon2), 
                   float(depth1), 
                   float(depth2)
                   )
end 



"""
    time_series(;
               table::String, 
               variable::String, 
               dt1::String, 
               dt2::String, 
               lat1::Number,
               lat2::Number, 
               lon1::Number, 
               lon2::Number, 
               depth1::Number, 
               depth2::Number
              )

Returns a subset of data according to the specified space-time constraints (dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2). 
The returned data subset is aggregated by time: at each time interval, the mean and standard deviation of the variable values within the space-time constraints are computed. 
The sequence of these values construct the timeseries. 
The timeseries data can be binned weekly, monthly, quarterly, or annually, if the interval parameter is set (this feature is not applicable to climatological datasets). 
The resulted timeseries is returned in the form of a dataframe ordered by time.

Examples
≡≡≡≡≡≡≡≡≡≡
time_series(
           table="tblAltimetry_REP", 
           variable="adt", 
           dt1="2016-01-01", 
           dt2="2018-01-01", 
           lat1=33, 
           lat2=35, 
           lon1=-160, 
           lon2=-159, 
           depth1=0, 
           depth2=0,
           interval="monthly"
           )

"""
function time_series(;
                    table::String, 
                    variable::String, 
                    dt1::String, 
                    dt2::String, 
                    lat1::Number,
                    lat2::Number, 
                    lon1::Number, 
                    lon2::Number, 
                    depth1::Number, 
                    depth2::Number,
                    interval::String=""
                    )
    usp = _interval_to_uspName(interval);
    if (usp != "uspTimeSeries") && (is_climatology(table))
        error(
              """
              Custom binning (monthly, weekly, ...) is not suppoerted for climatological data sets. 
              Table $table represents a climatological data set.
              """
            );
    end    
    return _subset(usp,
                   table, 
                   variable, 
                   dt1, 
                   dt2, 
                   float(lat1),
                   float(lat2), 
                   float(lon1), 
                   float(lon2), 
                   float(depth1), 
                   float(depth2)                   
                   )
end 



"""
    depth_profile(;
                 table::String, 
                 variable::String, 
                 dt1::String, 
                 dt2::String, 
                 lat1::Number,
                 lat2::Number, 
                 lon1::Number, 
                 lon2::Number, 
                 depth1::Number, 
                 depth2::Number
                )

Returns a subset of data according to the specified space-time constraints (dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2). 
The returned data subset is aggregated by depth: at each depth level the mean and standard deviation of the variable values within the space-time constraints are computed. 
The sequence of these values construct the depth profile. 
The resulting depth profile is returned in the form of a Pandas dataframe ordered by depth.
          
Examples
≡≡≡≡≡≡≡≡≡≡
depth_profile(
             table="tblPisces_NRT", 
             variable="CHL", 
             dt1="2016-04-30", 
             dt2="2016-04-30", 
             lat1=20, 
             lat2=24, 
             lon1=-170, 
             lon2=-150, 
             depth1=0, 
             depth2=500
             )

"""
function depth_profile(;
                      table::String, 
                      variable::String, 
                      dt1::String, 
                      dt2::String, 
                      lat1::Number,
                      lat2::Number, 
                      lon1::Number, 
                      lon2::Number, 
                      depth1::Number, 
                      depth2::Number
                      )
    return _subset("uspDepthProfile",
                   table, 
                   variable, 
                   dt1, 
                   dt2, 
                   float(lat1),
                   float(lat2), 
                   float(lon1), 
                   float(lon2), 
                   float(depth1), 
                   float(depth2)
                   )
end 



"""
    section(;
            table::String, 
            variable::String, 
            dt1::String, 
            dt2::String, 
            lat1::Number,
            lat2::Number, 
            lon1::Number, 
            lon2::Number, 
            depth1::Number, 
            depth2::Number
           )

Returns a subset of data according to the specified space-time constraints.
The results are ordered by time, lat, lon, and depth.

Examples
≡≡≡≡≡≡≡≡≡≡
section(
        table="tblPisces_NRT", 
        variable="NO3", 
        dt1="2016-04-30", 
        dt2="2016-04-30", 
        lat1=10, 
        lat2=50, 
        lon1=-159, 
        lon2=-158, 
        depth1=0, 
        depth2=500
        )

"""
function section(;
                table::String, 
                variable::String, 
                dt1::String, 
                dt2::String, 
                lat1::Number,
                lat2::Number, 
                lon1::Number, 
                lon2::Number, 
                depth1::Number, 
                depth2::Number
                )
    return _subset("uspSectionMap",
                   table, 
                   variable, 
                   dt1, 
                   dt2, 
                   float(lat1),
                   float(lat2), 
                   float(lon1), 
                   float(lon2), 
                   float(depth1), 
                   float(depth2)
                   )
end 



"""
    match(;
          sourceTable::String,
          sourceVariable::String,
          targetTables::Array,
          targetVariables::Array,
          dt1::String, 
          dt2::String, 
          lat1::Number, 
          lat2::Number, 
          lon1::Number, 
          lon2::Number, 
          depth1::Number, 
          depth2::Number,
          timeTolerance::Array,
          latTolerance::Array,
          lonTolerance::Array,
          depthTolerance::Array    
          )

Colocalizes the source variable (from source table) with the target variable (from target table).
The tolerance parameters set the matching boundaries between the source and target data sets. 
Returns a dataframe containing the source variable joined with the target variable.


# Arguments
- `sourceTable::String`: table name of the source data set.
- `sourceVariable::String`: the source variable. The target variables are matched (colocalized) with this variable.
- `targetTables::Array`: table names of the target data sets to be matched with the source data.
- `targetVariables::Array`: variable names to be matched with the source variable.
- `dt1::String`: start date or datetime.
- `dt2::String`: end date or datetime.
- `lat1::Number`: start latitude [degree N]. This parameter sets the lower bound of the meridional cut. Note latitude ranges from -90 to 90.
- `lat2::Number`: end latitude [degree N]. This parameter sets the upper bound of the meridional cut. Note latitude ranges from -90 to 90.
- `lon1::Number`: start longitude [degree E]. This parameter sets the lower bound of the zonal cut. Note latitude ranges from -180 to 180.
- `lon2::Number`: end longitude [degree E]. This parameter sets the upper bound of the zonal cut. Note latitude ranges from -180 to 180.
- `depth1::Number`: start depth [m]. This parameter sets the lower bound of the vertical cut. Note depth is a positive number (it is 0 at surface and grows towards ocean floor).
- `depth2::Number`: end depth [m]. This parameter sets the upper bound of the vertical cut. Note depth is a positive number (it is 0 at surface and grows towards ocean floor).
- `timeTolerance::Number`: integer array of temporal tolerance values between pairs of source and target datasets. The size and order of values in this array should match those of targetTables. This parameter is in day units except when the target variable represents monthly climatology data in which case it is in month units. Note fractional values are not supported in the current version.
- `latTolerance::Number`: float array of spatial tolerance values in meridional direction [deg] between pairs of source and target data sets. 
- `lonTolerance::Number`: float array of spatial tolerance values in zonal direction [deg] between pairs of source and target data sets. 
- `depthTolerance::Number`: float array of spatial tolerance values in vertical direction [m] between pairs of source and target data sets. 

Examples
≡≡≡≡≡≡≡≡≡≡
The source variable in this example is particulate pseudo cobalamin (Me_PseudoCobalamin_Particulate_pM) measured by 
Ingalls lab during the KM1315 cruise. This variable is colocalized with one target variabele, picoprokaryote concentration, 
from Darwin model. 

match(
    sourceTable="tblKM1314_Cobalmins",               
    sourceVariable="Me_PseudoCobalamin_Particulate_pM", 
    targetTables=["tblDarwin_Phytoplankton"],  
    targetVariables=["picoprokaryote"],                             
    dt1="2013-08-11", 
    dt2="2013-09-05", 
    lat1=22.25,       
    lat2=450.25,      
    lon1=-159.25,     
    lon2=-127.75,         
    depth1=-5,         
    depth2=305,       
    timeTolerance=[1],        
    latTolerance=[0.25],     
    lonTolerance=[0.25],         
    depthTolerance=[5]  
    ); 
"""
function match(;
               sourceTable::String,
               sourceVariable::String,
               targetTables::Array,
               targetVariables::Array,
               dt1::String, 
               dt2::String, 
               lat1::Number, 
               lat2::Number, 
               lon1::Number, 
               lon2::Number, 
               depth1::Number, 
               depth2::Number,
               timeTolerance::Array,
               latTolerance::Array,
               lonTolerance::Array,
               depthTolerance::Array    
               )
    return compile(
                   spname="uspMatch",
                   sourceTable=sourceTable,
                   sourceVariable=sourceVariable,
                   targetTables=targetTables,
                   targetVariables=targetVariables,
                   dt1=dt1, 
                   dt2=dt2, 
                   lat1=lat1, 
                   lat2=lat2, 
                   lon1=lon1, 
                   lon2=lon2, 
                   depth1=depth1, 
                   depth2=depth2,
                   timeTolerance=timeTolerance,
                   latTolerance=latTolerance,
                   lonTolerance=lonTolerance,
                   depthTolerance=depthTolerance    
                  )
end 



"""
    along_track(;
                cruise::String,
                targetTables::Array,
                targetVariables::Array,
                depth1::Number, 
                depth2::Number,
                timeTolerance::Array,
                latTolerance::Array,
                lonTolerance::Array,
                depthTolerance::Array    
                )

Takes a cruise name and colocalizes the cruise track with the specified variable(s).


# Arguments
- `cruise::String`: cruise name.
- `targetTables::Array`: table names of the target data sets to be matched with the source data.
- `targetVariables::Array`: variable names to be matched with the source variable.
- `depth1::Number`: start depth [m]. This parameter sets the lower bound of the vertical cut. Note depth is a positive number (it is 0 at surface and grows towards ocean floor).
- `depth2::Number`: end depth [m]. This parameter sets the upper bound of the vertical cut. Note depth is a positive number (it is 0 at surface and grows towards ocean floor).
- `timeTolerance::Number`: integer array of temporal tolerance values between pairs of source and target datasets. The size and order of values in this array should match those of targetTables. This parameter is in day units except when the target variable represents monthly climatology data in which case it is in month units. Note fractional values are not supported in the current version.
- `latTolerance::Number`: float array of spatial tolerance values in meridional direction [deg] between pairs of source and target data sets. 
- `lonTolerance::Number`: float array of spatial tolerance values in zonal direction [deg] between pairs of source and target data sets. 
- `depthTolerance::Number`: float array of spatial tolerance values in vertical direction [m] between pairs of source and target data sets. 

Examples
≡≡≡≡≡≡≡≡≡≡
This example demonstrates how to colocalize the "gradients_1" cruise (official name: KOK1606) with 2 target variables:
"prochloro_abundance" from underway seaflow dataset "PO4" from Darwin climatology model.

along_track(
            cruise="gradients_1",               
            targetTables=["tblSeaFlow", "tblDarwin_Nutrient_Climatology"],  
            targetVariables=["prochloro_abundance", "PO4_darwin_clim"],                             
            depth1=0,         
            depth2=5,       
            timeTolerance=[0, 0],        
            latTolerance=[0.01, 0.25],     
            lonTolerance=[0.01, 0.25],         
            depthTolerance=[0, 5]  
            ); 
"""
function along_track(;
                    cruise::String,
                    targetTables::Array,
                    targetVariables::Array,
                    depth1::Number, 
                    depth2::Number,
                    timeTolerance::Array,
                    latTolerance::Array,
                    lonTolerance::Array,
                    depthTolerance::Array    
                    )
    df = cruise_bounds(cruise);                
    return match(
                 sourceTable="tblCruise_Trajectory",
                 sourceVariable=string(df.ID[1]),
                 targetTables=targetTables,
                 targetVariables=targetVariables,
                 dt1=df.dt1[1], 
                 dt2=df.dt2[1], 
                 lat1=df.lat1[1], 
                 lat2=df.lat2[1], 
                 lon1=df.lon1[1], 
                 lon2=df.lon2[1], 
                 depth1=depth1, 
                 depth2=depth2,
                 timeTolerance=timeTolerance,
                 latTolerance=latTolerance,
                 lonTolerance=lonTolerance,
                 depthTolerance=depthTolerance    
                 )
end 

