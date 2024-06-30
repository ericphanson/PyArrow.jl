using PyArrow
using Test, Aqua
using Tables
using DataAPI
using DataFrames

@testset "PyArrow.jl" begin
    jl_table = (; a=[rand(2); NaN], b=["a", "b", "c"])
    py_table = PyArrow.table(jl_table)
    @test py_table isa Py

    t = PyArrowTable(py_table)
    # Test indexing
    @test isequal(pyconvert(Vector, t["a"]), jl_table.a)
    @test isequal(pyconvert(Vector, t[0]), jl_table.a)

    # Test property-access
    @test pyconvert(Int, t.num_columns) == 2
    @test pyconvert(Int, t.num_rows) == 3

    # Test DataAPI
    @test DataAPI.ncol(t) == 2
    @test DataAPI.nrow(t) == 3

    # Test Tables.jl integration: conversion to DataFrames
    df = DataFrame(t)
    @test ncol(df) == 2
    @test Base.names(df) == ["a", "b"]
    @test df.b[1] isa Py

    # Can convert to julia types:
    df_jl = mapcols(v -> pyconvert.(Any, v), df)
    @test df_jl.b[1] isa String

    # zero-copy properties
    t = PyArrowTable(PyArrow.table(PyArrowTable(PyArrow.table(PyArrowTable(py_table)))))
    jl_table.a[3] = 5.0
    # Zero-copy even through a bunch of layers of roundtripping
    @test Tables.getcolumn(t, :a)[3] == 5.0

    # Same for string columns
    jl_table.b[3] = "no"
    @test pyconvert(String, Tables.getcolumn(t, :b)[3]) == "c"

    # However, not zero-copy with missing/nothing:
    jl_table = (; a=[rand(2); missing], b=["a", "b", missing])
    py_table = PyArrow.table(jl_table)
    @test py_table isa Py
    t = PyArrowTable(PyArrow.table(PyArrowTable(PyArrow.table(PyArrowTable(py_table)))))
    jl_table.a[3] = 5.0
    jl_table.b[3] = "no"
    @test_broken isequal(Tables.getcolumn(t, :a)[3], 5.0)
    @test_broken isequal(Tables.getcolumn(t, :b)[3], "no")
end

@testset "README examples" begin
    # Here we test these don't error at least
    readme = read(joinpath(pkgdir(PyArrow), "README.md"), String)
    for m in eachmatch(r"```julia\n([\s\S]+?)\n```", readme)
        str = m[1]
        println("Evaluating:", "\n", str, "\n")
        eval(Meta.parseall(str))
        @test true
    end
end

@testset "Aqua" begin
    Aqua.test_all(PyArrow; ambiguities=false)
end
