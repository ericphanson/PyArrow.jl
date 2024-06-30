module PyArrow

#####
##### Dependencies
#####

using Reexport: @reexport
using Tables: Tables
using DataAPI: DataAPI
using SentinelArrays: ChainedVector

@reexport using PythonCall

include("compat.jl")
include("PyArrowTable.jl")

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

function column_to_arrow(v)
    if ispy(v)
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
    PyArrow.table(src; metadata=nothing, nthreads=nothing) -> Py

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

end
