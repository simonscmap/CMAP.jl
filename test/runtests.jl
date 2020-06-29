using CMAP
using Test
using DataFrames

@testset "CMAP.jl" begin
    status, response = query(API(), "select * from tblMakes")
    @test nrow(response) == 3
end

@testset "Gradients 2" begin
    @testset "Nutrients" begin
        @test get_dataset("tblMGL1704_Gradients2_Nutrients") isa DataFrame
    end
end
