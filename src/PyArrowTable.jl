
"""
    PyArrowTable(py)

Wraps the python object `py` which corresponds to a pyarrow table to provide a Tables.jl-compatible interface.

Supports:
- Property access to access the properties of the underlying pyarrow table (plus `table.py` to access the raw pyarrow table object)
- Indexing to access columns (`table[0]`, `table["col_name"]`)
- the Tables.jl columnular interface, so e.g. `DataFrame(table)` should work.
"""
struct PyArrowTable <: PyTable
    py::Py
end

function Base.getproperty(df::PyArrowTable, field::Symbol)
    if field === :py
        return getfield(df, :py)
    else
        return getproperty(getfield(df, :py), field)
    end
end

function Base.getindex(df::PyArrowTable, index::Any)
    return df.py[index]
end

function Base.propertynames(df::PyArrowTable)
    return [propertynames(df.py); :py]
end

PythonCall.ispy(x::PyArrowTable) = true
PythonCall.Py(x::PyArrowTable) = x.py

function column_from_arrow(v)
    n = pyconvert(Int, v.num_chunks)

    get_chunk = i -> begin
        w = v.chunk(i).to_numpy(; zero_copy_only=false)
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
