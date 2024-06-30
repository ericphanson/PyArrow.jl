module PyArrow

#####
##### Dependencies
#####

using Reexport, Tables, SentinelArrays, DataAPI

@reexport using PythonCall

include("compat.jl")

#####
##### Exports
#####

export pyarrow, PyArrowTable
@public_or_nothing table

#####
##### Implementation
#####

const pyarrow = PythonCall.pynew()
const numpy = PythonCall.pynew()

function __init__()
    PythonCall.pycopy!(pyarrow, pyimport("pyarrow"))
    PythonCall.pycopy!(numpy, pyimport("numpy"))
    return nothing
end

"""
    PyArrow.table(src; metadata=nothing, nthreads=nothing)

A wrapper around `pyarrow.table` which converts a Tables.jl-compatible table
to a pyarrow table.

Attempts to do so in a zero-copy manner by converting
columns to numpy arrays (without copying) before passing to `pa.table`
"""
function table(src; metadata=nothing, nthreads=nothing)
    # Note: we don't use `PythonCall.pytable`,
    # since we want to aim for zero-copy, and support missing better
    cols = Tables.columns(src)
    names = map(pystr, Tables.columnnames(cols))
    py_cols = pylist(column_to_arrow(Tables.getcolumn(cols, i)) for i in 1:length(names))
    return pyarrow.table(py_cols; names, metadata, nthreads)
end

function column_to_arrow(v)
    if v isa PyArray
        return Py(v)
    elseif Missing <: eltype(v)
        return Py(replace(v, missing => nothing))
    elseif isbitstype(eltype(v))
        return Py(v).to_numpy(; copy=false)
    else
        return Py(v)
    end
end

"""
    PyArrowTable(py)

Wraps the python object `py` which corresponds to a pyarrow table to provide a Tables.jl-compatible interface.
"""
struct PyArrowTable <: PyTable
    py::Py
end

PythonCall.ispy(x::PyArrowTable) = true
PythonCall.Py(x::PyArrowTable) = x.py

function column_from_arrow(v)
    n = pyconvert(Int, v.num_chunks)

    get_chunk = i -> begin
        w = v.chunk(i)
        w = w.to_numpy(; zero_copy_only=false)
        return PyArray(w; copy=false)
    end

    if n == 1
        return get_chunk(0)
    else
        return ChainedVector([get_chunk(i) for i in 0:(n - 1)])
    end
end

DataAPI.ncol(x::PyArrowTable) = x.py.num_columns
DataAPI.nrow(x::PyArrowTable) = x.py.num_rows

Tables.columns(df::PyArrowTable) = df
Tables.columnnames(df::PyArrowTable) = map(n -> pyconvert(Symbol, n), df.py.column_names)

function Tables.getcolumn(df::PyArrowTable, i::Int)
    return column_from_arrow(df.py[i - 1])
end

function Tables.getcolumn(df::PyArrowTable, nm::Symbol)
    return column_from_arrow(df.py[pystr(String(nm))])
end

end
