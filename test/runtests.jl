using PyArrow
using Test, Aqua
using Tables
using DataAPI
using DataFrames
using CondaPkg
using SentinelArrays
using Dates

const pa = pyarrow
const feather = pyimport("pyarrow.feather")
TEST_TABLES = joinpath(pkgdir(PyArrow, "test", "test_tables"))

# Print packages for CI logs
CondaPkg.withenv() do
    return run(`micromamba list`)
end

@testset "PyArrow.jl" begin
    jl_table = (; a=[rand(2); NaN], b=["a", "b", "c"])
    py_table = PyArrow.table(jl_table)
    @test py_table isa Py

    t = PyArrowTable(py_table)

    # PythonCall interface
    @test PythonCall.ispy(t)
    @test pyconvert(Bool, Py(t) == t.py)
    @test pyconvert(Bool, Py(t) == py_table)

    # Properties
    @test isequal(pyconvert(Vector, t["a"]), jl_table.a)
    @test isequal(pyconvert(Vector, t[0]), jl_table.a)

    # Test properties
    @test propertynames(t) isa Vector{Symbol}
    @test :py in propertynames(t)
    @test :num_columns in propertynames(t)
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

    # Multiple record batches
    data = (; f0=@py([1, 2, 3, 4]),
            f1=@py(["foo", "bar", "baz", nothing]),
            f2=@py([true, nothing, false, true]))
    batch = pa.RecordBatch.from_arrays(pylist(data), @py(["f0", "f1", "f2"]))
    table = pa.Table.from_batches(pylist([batch for _ in 1:5]))
    @test pyconvert(Bool, table[0].num_chunks == 5)
    jl_table = PyArrowTable(table)
    @test Tables.getcolumn(jl_table, :f0) isa ChainedVector
    @test eltype(Tables.getcolumn(jl_table, :f0)) == Int
    @test collect(Tables.getcolumn(jl_table, :f0)) == repeat([1, 2, 3, 4], 5)
end

@testset "datetimes" begin
    # failed in the wild:
    table = feather.read_table(joinpath(TEST_TABLES, "datetimes.arrow"))
    pat = PyArrowTable(table)
    cols = Tables.columntable(pat)
    dates = map(x -> pyconvert(DateTime, x), cols[2])
    @test all(x -> x isa DateTime, dates)
    @test dates[1] == DateTime("2024-01-03T22:05:33.470")
    t = pyconvert(Time, cols[3][1])
    @test t == Time(21, 34, 15)
    @test all(x -> Bool(x == pybuiltins.None), cols[4])

    # DataFrames use slightly different paths than `Tables.columntable`
    # so check we can construct one as well:
    df = DataFrame(pat)
    @test df isa DataFrame
    df_jl = mapcols(v -> pyconvert.(Any, v), df)
    @test df_jl[!, 2] isa Vector{DateTime}

    # Now let us insert a missing in the middle of a DateTime column and see if we can roundtrip it
    col = df_jl[!, 2]
    df_jl[!, 2] = [i == 2 ? missing : col[i] for i in eachindex(col)]
    @test df_jl[!, 2] isa Vector{Union{Missing,DateTime}}
    py_table = PyArrow.table(df_jl)
    rt = DataFrame(PyArrowTable(py_table))
    @test rt isa DataFrame
    @test ismissing(pyconvert(Any, rt[2, 2]))
    @test pyconvert(Any, rt[3, 2]) isa DateTime
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
