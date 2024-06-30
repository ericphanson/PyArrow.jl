using PyArrow
using Test, Aqua

@testset "Aqua" begin
    Aqua.test_all(PyArrow)
end

@testset "PyArrow.jl" begin
    jl_table = (; a = [rand(2); NaN], b = ["a", "b", "c"])
    py_table = PyArrow.table(jl_table)
    @test py_table isa Py

    t = PyArrowTable(PyArrow.table(PyArrowTable(PyArrow.table(PyArrowTable(py_table)))))
    jl_table.a[3] = 5.0
    # Zero-copy even through a bunch of layers of roundtripping
    @test Tables.getcolumn(t, :a)[3] == 5.0

    # Same for string columns
    jl_table.b[3] = "no"
    @test Tables.getcolumn(t, :b)[3] == "c"

    # However, not zero-copy with missing/nothing:
    jl_table = (; a = [rand(2); missing], b = ["a", "b", missing])
    py_table = PyArrow.table(jl_table)
    @test py_table isa Py
    t = PyArrowTable(PyArrow.table(PyArrowTable(PyArrow.table(PyArrowTable(py_table)))))
    jl_table.a[3] = 5.0
    jl_table.b[3] = "no"
    @test_broken isequal(Tables.getcolumn(t, :a)[3], 5.0)
    @test_broken isequal(Tables.getcolumn(t, :b)[3], "no")
end
