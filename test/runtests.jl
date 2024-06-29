using PyArrow
using Test, Aqua


@testset "Aqua" begin
    Aqua.test_all(PyArrow)
end
@testset "PyArrow.jl" begin
    jl_table = (; a = [rand(2); NaN], b = ["a", "b", missing])
    py_table = PyArrow.table(jl_table)
    @test py_table isa Py

    t = PyArrowTable(PyArrow.table(PyArrowTable(py_table)))
end
