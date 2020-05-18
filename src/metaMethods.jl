


include("./rest.jl")
include("./match.jl")



"""
get_catalog()

Returns a dataframe containing full Simons CMAP catalog of variables.

Examples
≡≡≡≡≡≡≡≡≡≡
get_catalog()

"""
function get_catalog()
api = API();
status, response = query(api, "EXEC uspCatalog");
return response
end    



"""
search_catalog(keywords::String)

Returns a dataframe containing a subset of Simons CMAP catalog of variables. 
All variables at Simons CMAP catalog are annotated with a collection of semantically related keywords. 
This method takes the passed keywords and returns all of the variables annotated with similar keywords.
The passed keywords should be separated by blank space. The search result is not sensitive to the order of keywords and is not case sensitive.
The passed keywords can provide any 'hint' associated with the target variables. Below are a few examples: 

* the exact variable name (e.g. NO3), or its linguistic term (Nitrate)

* methodology (model, satellite ...), instrument (CTD, seaflow), or disciplines (physics, biology ...) 

* the cruise official name (e.g. KOK1606), or unofficial cruise name (Falkor)

* the name of data producer (e.g Penny Chisholm) or institution name (MIT)

If you searched for a variable with semantically-related-keywords and did not get the correct results, please let us know. 
We can update the keywords at any point.

Examples
≡≡≡≡≡≡≡≡≡≡
search_catalog("nitrite falkor")

"""
function search_catalog(keywords::String)
api = API();
status, response = query(api, "EXEC uspSearchCatalog '$keywords'");
return response
end   



"""
datasets()

Returns a dataframe containing the list of data sets hosted by Simons CMAP database.

Examples
≡≡≡≡≡≡≡≡≡≡
datasets()

"""
function datasets()
api = API();
status, response = query(api, "EXEC uspDatasets");
return response
end 


"""
head(tableName::String, rows::Integer=5)

Returns top records of a data set.

Examples
≡≡≡≡≡≡≡≡≡≡
head("tblFalkor_2018")

"""
function head(tableName::String, rows::Integer=5)
api = API();
status, response = query(api, "EXEC uspHead '$tableName', $rows" );
return response
end 


"""
columns(tableName::String)

Returns the list of data set columns.

Examples
≡≡≡≡≡≡≡≡≡≡
columns("tblAMT13_Chisholm")

"""
function columns(tableName::String)
api = API();
status, response = query(api, "EXEC uspColumns '$tableName'" );
return response
end 



"""
get_dataset_ID(tableName::String)

Returns dataset ID.

Examples
≡≡≡≡≡≡≡≡≡≡
get_dataset_ID("tblCHL_REP")

"""
function get_dataset_ID(tableName::String)
api = API();
status, response = query(api, "SELECT DISTINCT(Dataset_ID) FROM dbo.udfCatalog() WHERE LOWER(Table_Name)=LOWER('$tableName') " );
if nrow(response) < 1
    error("Invalid table name: $tableName")
end    
if nrow(response) > 1
    error("More than one table found. Please provide a more specific name: ")
    println(response);
end    

return response.Dataset_ID[1]
end 



"""
    get_dataset_metadata(tableName::String)

Returns a dataframe containing the dataset metadata.

Examples
≡≡≡≡≡≡≡≡≡≡
get_dataset_metadata("tblArgoMerge_REP")

"""
function get_dataset_metadata(tableName::String)
    api = API();
    status, response = query(api, "EXEC uspDatasetMetadata '$tableName'" );
    return response
end 


"""
    get_var(tableName::String, varName::String)

Returns a single-row dataframe from tblVariables containing info associated with varName.
This method is mean to be used internally and will not be exposed at documentations.

Examples
≡≡≡≡≡≡≡≡≡≡
get_var("tblCHL_REP", "chl")

"""
function get_var(tableName::String, varName::String)
    api = API();
    status, response = query(api, "SELECT * FROM tblVariables WHERE Table_Name='$tableName' AND Short_Name='$varName'" );
    return response
end 


"""
    get_var_catalog(tableName::String, varName::String)

Returns a single-row dataframe from catalog (udfCatalog) containing all of the variable's info at catalog.

Examples
≡≡≡≡≡≡≡≡≡≡
get_var_catalog("tblCHL_REP", "chl")

"""
function get_var_catalog(tableName::String, varName::String)
    api = API();
    status, response = query(api, "SELECT * FROM [dbo].udfCatalog() WHERE Table_Name='$tableName' AND Variable='$varName'" );
    return response
end 


"""
    get_var_long_name(tableName::String, varName::String)

Returns the long name of a given variable.

Examples
≡≡≡≡≡≡≡≡≡≡
get_var_long_name("tblAltimetry_REP", "adt")

"""
function get_var_long_name(tableName::String, varName::String)
    api = API();
    status, response = query(api, "SELECT Long_Name, Short_Name FROM tblVariables WHERE Table_Name='$tableName' AND  Short_Name='$varName'");
    return response.Long_Name[1]
end 


"""
    get_unit(tableName::String, varName::String)

Returns the long name of a given variable.

Examples
≡≡≡≡≡≡≡≡≡≡
get_unit("tblHOT_ParticleFlux", "silica_hot")

"""
function get_unit(tableName::String, varName::String)
    api = API();
    status, response = query(api, "SELECT Unit, Short_Name FROM tblVariables WHERE Table_Name='$tableName' AND  Short_Name='$varName'");
    return response.Unit[1]
end 


"""
    get_var_resolution(tableName::String, varName::String)

Returns the long name of a given variable.

Examples
≡≡≡≡≡≡≡≡≡≡
get_var_resolution("tblModis_AOD_REP", "AOD")

"""
function get_var_resolution(tableName::String, varName::String)
    api = API();
    status, response = query(api, "EXEC uspVariableResolution '$tableName', '$varName'");
    return response
end 



"""
    get_var_coverage(tableName::String, varName::String)

Returns a single-row dataframe from catalog (udfCatalog) containing the variable's spatial and temporal coverage.

Examples
≡≡≡≡≡≡≡≡≡≡
get_var_coverage("tblCHL_REP", "chl")

"""
function get_var_coverage(tableName::String, varName::String)
    api = API();
    status, response = query(api, "EXEC uspVariableCoverage '$tableName', '$varName'");
    return response
end 



"""
    get_var_stat(tableName::String, varName::String)

Returns a single-row dataframe from catalog (udfCatalog) containing the variable's summary statistics.

Examples
≡≡≡≡≡≡≡≡≡≡
get_var_stat("tblHOT_LAVA", "Prochlorococcus")

"""
function get_var_stat(tableName::String, varName::String)
    api = API();
    status, response = query(api, "EXEC uspVariableStat '$tableName', '$varName'");
    return response
end 



"""
    has_field(tableName::String, varName::String)

Returns a single-row dataframe from catalog (udfCatalog) containing the variable's summary statistics.

Examples
≡≡≡≡≡≡≡≡≡≡
has_field("tblAltimetry_REP", "sla")

"""
function has_field(tableName::String, varName::String)
    api = API();
    status, response = query(api, "SELECT COL_LENGTH('$tableName', '$varName') AS RESULT");
    if isequal(response.RESULT[1], missing)
        return false
    end    
    return true
end 



"""
    is_grid(tableName::String, varName::String)

Returns a boolean indicating whether the variable is a gridded product or has irregular spatial resolution.

Examples
≡≡≡≡≡≡≡≡≡≡
is_grid("tblArgoMerge_REP", "argo_merge_salinity_adj")

"""
function is_grid(tableName::String, varName::String)
    api = API();
    grid = true;
    statement = "SELECT Spatial_Res_ID, RTRIM(LTRIM(Spatial_Resolution)) AS Spatial_Resolution FROM tblVariables "
    statement *= "JOIN tblSpatial_Resolutions ON [tblVariables].Spatial_Res_ID=[tblSpatial_Resolutions].ID "
    statement *= "WHERE Table_Name='$tableName' AND Short_Name='$varName' "

    status, response = query(api, statement);
    if nrow(response) < 1
        return missing
    end    
    if strip(lowercase(response.Spatial_Resolution[1])) == lowercase("irregular")
        return false    
    end    
    return true    
end 


"""
    is_climatology(tableName::String)

Returns true if the table represents a climatological data set.    
Currently, the logic is based on the table name.
TODO: Ultimately, it should query the DB to determine if it's a climatological data set.

Examples
≡≡≡≡≡≡≡≡≡≡
is_climatology("tblDarwin_Plankton_Climatology")

"""
function is_climatology(tableName::String)
    occursin("_Climatology", tableName) ? true : false
end    



"""
    get_references(datasetID::Int)

Returns a dataframe containing refrences associated with a data set.

Examples
≡≡≡≡≡≡≡≡≡≡
get_references(21)

"""
function get_references(datasetID::Int)
    api = API();
    status, response = query(api, "SELECT Reference FROM dbo.udfDatasetReferences($datasetID)");
    return response
end   



"""
    get_metadata(tableName::String, varName::String)

Returns a dataframe containing the associated metadata.

Examples
≡≡≡≡≡≡≡≡≡≡
get_metadata("tblsst_AVHRR_OI_NRT", "sst")

"""
function get_metadata(tableName::String, varName::String)
    api = API();
    status, response = query(api, "EXEC uspVariableMetaData '$tableName', '$varName'");
    return response
end 



"""
    cruises()

Returns a dataframe containing a list of all of the hosted cruise names.

Examples
≡≡≡≡≡≡≡≡≡≡
cruises()

"""
function cruises()
    api = API();
    status, response = query(api, "EXEC uspCruises");
    return response
end 



"""
    cruise_by_name(cruiseName::String)

Returns a dataframe containing cruise info using cruise name.
The details include cruise official name, nickname, ship name, start/end time/location, etc …
A full list of cruise names can be retrieved using cruise method.
If applicable, you may also use cruise “nickname” (‘Diel’, ‘Gradients_1’ …). 

Examples
≡≡≡≡≡≡≡≡≡≡
cruise_by_name("KOK1606");
cruise_by_name("Gradients_1");

"""
function cruise_by_name(cruiseName::String)
    api = API();
    status, response = query(api, "EXEC uspCruiseByName '$cruiseName'");
    if nrow(response) < 1
        error("Invalid cruise name: $cruiseName.");
    end    
    if nrow(response) > 1
        println(response);
        error("More than one cruise found (see above). Please provide a more specific name.")
    end    
    return response
end 



"""
    cruise_bounds(cruiseName::String)

Returns a dataframe containing the spatio-temporal bounding box accosiated with the specified cruise.
A full list of cruise names can be retrieved using cruise method.
If applicable, you may also use cruise “nickname” (‘Diel’, ‘Gradients_1’ …).

Examples
≡≡≡≡≡≡≡≡≡≡
cruise_bounds("KOK1606");
cruise_bounds("Gradients_1");

"""
function cruise_bounds(cruiseName::String)
    id = cruise_by_name(cruiseName).ID[1];
    api = API();
    status, response = query(api, "EXEC uspCruiseBounds $id");
    return response
end 



"""
    cruise_trajectory(cruiseName::String)

Returns a dataframe containing the cruise trajectory.
A full list of cruise names can be retrieved using cruise method.
If applicable, you may also use cruise “nickname” (‘Diel’, ‘Gradients_1’ …).

Examples
≡≡≡≡≡≡≡≡≡≡
cruise_trajectory("KOK1606");
cruise_trajectory("Gradients_1");

"""
function cruise_trajectory(cruiseName::String)
    id = cruise_by_name(cruiseName).ID[1];
    api = API();
    status, response = query(api, "EXEC uspCruiseTrajectory $id");
    return response
end 



"""
    cruise_variables(cruiseName::String)

Returns a dataframe containing all registered variables (at Simons CMAP) during a cruise.
A full list of cruise names can be retrieved using cruise method.
If applicable, you may also use cruise “nickname” (‘Diel’, ‘Gradients_1’ …).

Examples
≡≡≡≡≡≡≡≡≡≡
cruise_variables("KOK1606");
cruise_variables("Gradients_1");

"""
function cruise_variables(cruiseName::String)
    id = cruise_by_name(cruiseName).ID[1];
    api = API();
    status, response = query(api, "SELECT * FROM dbo.udfCruiseVariables($id)");
    return response
end 





