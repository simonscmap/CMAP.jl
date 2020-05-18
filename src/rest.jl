"""
Author: Mohammad Dehghani Ashkezari <mdehghan@uw.edu>

Date: 2020-05-07

Function: HTTP requests to the CMAP RESTful API.
"""

using HTTP 
using CSV
using DataFrames
using JSON



mutable struct API
    token :: String
    domain :: String
    tokenPrefix :: String
    
    function API(
                token::String="", 
                tokenPrefix::String="Api-Key ", 
                domain::String="https://simonscmap.com"
                )

        if isempty(token)
            token = get_api_key();
        end            

        @assert(length(token)>0, "API key not found!")                

        api = new();
        api.token = token;
        api.tokenPrefix = tokenPrefix;
        api.domain = domain;
        api
    end
end



function api_key_fname()
    return "api_key.csv"
end


"""
    set_api_key(key::String) 

Saves your API key on local disk.

Examples
≡≡≡≡≡≡≡≡≡≡
set_api_key("Your API Key") 

"""
function set_api_key(key::String)    
    CSV.write(api_key_fname(), DataFrame(apiKey = key))
end    


"""
    get_api_key()

Returns your api key, if exists.

Examples
≡≡≡≡≡≡≡≡≡≡
get_api_key()

"""
function get_api_key()
    keyFile = api_key_fname()
    if ~isfile(keyFile)
        error("API Key file not found. The following command will register the API key:\n\nset_api_key(key::String)\n\n")
    end    
    return DataFrame(CSV.File(keyFile))[1, 1]
end


function atomic_request(api::API, route, payload)
    try
        baseURL = api.domain;        
        tokenPrefix = api.tokenPrefix;        
        token = tokenPrefix * api.token;
        queryString = "";
        if ~isempty(payload)
            queryString = HTTP.escapeuri(payload);
        end            
        url = baseURL * route * queryString;        
        headers = (("Authorization", token), 
                   ("content-type","application/json")
                  )
        response = HTTP.request("GET", url, headers);
        status = response.status;
        respStr = String(response.body);
        df = CSV.read(IOBuffer(chomp(respStr)))
        return status, df
    catch e
        println("Error in atomic_request : $e")
        return 
    end
end



"""
    query(api::API, queryStatement::String)

Takes a custom SQL query statement and returns the results in form of a dataframe.

Examples
≡≡≡≡≡≡≡≡≡≡
api = API()
status, response = query(api, "SELECT * FROM tblSensors")
println(response)

"""
function query(api::API, queryStatement::String)
    payload = Dict("query" => queryStatement);
    route = "/api/data/query?";
    return atomic_request(api, route, payload)
end    
