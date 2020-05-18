using CMAP
using Test
using DataFrames

@testset "CMAP.jl" begin    
    status, response = query(API(), "select * from tblMakes")
    @test nrow(response) == 3
end
