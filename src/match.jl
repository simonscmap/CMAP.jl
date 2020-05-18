"""
Author: Mohammad Dehghani Ashkezari <mdehghan@uw.edu>

Date: 2020-05-08

Function: implemantation of matching (colocalizing) data sets.
"""

using Dates
include("./rest.jl")




"""
    _atomic_match(match::Match)

Colocalizes the source variable (from source table) with a single target variable (from target table).
The tolerance parameters set the matching boundaries between the source and target data sets. 
Returns a dataframe containing the source variable joined with the target variable.
This function is meant to be used internally.

Examples
≡≡≡≡≡≡≡≡≡≡

_atomic_match(
             "uspMatch",
             "tblKM1314_Cobalmins",               
             "Me_PseudoCobalamin_Particulate_pM", 
             "tblDarwin_Phytoplankton",  
             "picoprokaryote",                             
             "2013-08-11", 
             "2013-09-05", 
             22.25,       
             450.25,      
             -159.25,     
             -127.75,         
             -5,         
             305,       
             1,        
             0.25,     
             0.25,         
             5  
             );       
"""
function _atomic_match(
                       spName::String,
                       sourceTable::String,
                       sourceVariable::String,
                       targetTable::String,
                       targetVariable::String,
                       dt1::String, 
                       dt2::String, 
                       lat1::Number, 
                       lat2::Number, 
                       lon1::Number, 
                       lon2::Number, 
                       depth1::Number, 
                       depth2::Number,
                       timeTolerance::Number,
                       latTolerance::Number,
                       lonTolerance::Number,
                       depthTolerance::Number
                      )                      
       
    statement = """
                EXEC $spName 
                    '$sourceTable', 
                    '$sourceVariable',
                    '$targetTable', 
                    '$targetVariable', 
                    '$dt1', 
                    '$dt2', 
                    '$lat1', 
                    '$lat2', 
                    '$lon1', 
                    '$lon2', 
                    '$depth1', 
                    '$depth2', 
                    '$timeTolerance', 
                    '$latTolerance', 
                    '$lonTolerance', 
                    '$depthTolerance' 
                """              
    api = API();            
    status, response = query(api, statement);
    return response  
end




"""
Loops through the target data sets and match them with the source data set according to the the accosiated tolerance parameters.
Returns a compiled dataframe of the source and matched target data sets.
The matching results rely on the spatio-temporal tolerance parameters.
Notice the source has to be a single variable and cannot be a climatological variable. 
You may pass empty string ('') as source variable if you only want to get the time and location info from the source table.
The target variables (one or more) are matched with the source variable, if any match exists.
Please note that the number of matching entries between each target variable and the source variable might vary depending on the temporal and spatial resolution of the target variable. 


# Arguments
- `spname::String`: stored procedure name that executes the matching logic.
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

"""
function compile(;
                 spname::String,
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

    function shift_dt(dt::String, delta::Int)
        dtFormat = "yyyy-mm-dd";
        if occursin("T", dt)
            dt = replace(dt, "T" => " ");
            if occursin(".000Z", dt)
                dt = replace(dt, ".000Z" => "");
            end                            
            dtFormat = "yyyy-mm-dd HH:MM:SS";
        end
        dt = DateTime(dt, dtFormat);
        dt = dt + Dates.Day(delta);
        # TODO: Handle monthly climatology data sets
        shifted = Dates.format(dt, "yyyy-mm-dd HH:MM:SS");
    end  

    df = DataFrame();
    for i =  1:length(targetTables)
        data = _atomic_match(
                            spname, 
                            sourceTable, 
                            sourceVariable, 
                            targetTables[i], 
                            targetVariables[i], 
                            shift_dt(dt1, -timeTolerance[i]),
                            shift_dt(dt2, timeTolerance[i]),
                            lat1 - latTolerance[i], 
                            lat2 + latTolerance[i], 
                            lon1 - latTolerance[i], 
                            lon2 + latTolerance[i], 
                            depth1 - depthTolerance[i], 
                            depth2 + depthTolerance[i],
                            timeTolerance[i], 
                            latTolerance[i], 
                            lonTolerance[i], 
                            depthTolerance[i]
                            )
        if nrow(data) < 1
            println("$i: No matching entry associated with $(targetVariables[i]).")
            continue
        end    
        println("$i: $(targetVariables[i]) matched.")   
        
        if nrow(df) < 1
            df = data;
        elseif ( 
                df[!, names(df)[1]] == data[!, names(df)[1]] &&
                df.lat == data.lat &&
                df.lon == data.lon
                )


            df[!, Symbol(targetVariables[i])] = data[!, Symbol(targetVariables[i])]
            df[!, Symbol(targetVariables[i]*"_std")] = data[!, Symbol(targetVariables[i]*"_std")]        
        else
            println("The matched dataframe corresponding to $(targetVariables[i]) does not have the same size as the first targert variable. Please change the tolerance parameters.") 
        end    
    end     
    return df
end    