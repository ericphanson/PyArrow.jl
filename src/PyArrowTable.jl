
"""
    PyArrowTable(py)

Wraps the python object `py` which corresponds to a pyarrow table to provide a Tables.jl-compatible interface.

Supports:
- Property access to access the properties of the underlying pyarrow table (plus `table.py` to access the raw pyarrow table object). These yields PythonCall `Py` objects, which may need to be converted with `pyconvert`.
- Indexing to access columns (`table[0]`, `table["col_name"]`). This also yields raw `Py` objects (which are not even `<: AbstractVector`).
- the Tables.jl columnar interface, so e.g. `DataFrame(table)` should work. The columns are `PyArray`'s (i.e. `AbstractVector`'s pointing to python objects), or `ChainedVector`'s with `PyArray` subcomponents (if there is more than 1 batch).
- DataAPI's `ncol` and `nrow` accessors.
- datetime objects convertible to `np.datetime64`. `PyArrowTable` will convert to `np.datetime64`, which will convert the timezone to UTC using the timezone specified in the original object, or using the local timezone if one is not present (see: https://stackoverflow.com/a/25163415/20471121).

Does not yet support:

- DataAPI's metadata-related functions
- `Tables.partitions` to iterate record batches

This functionality may be added in future non-breaking releases.

Consider:
```julia
using DataFrames, PyArrow
feather = pyimport("pyarrow.feather")
table = feather.read_table("table.arrow") # example
df = DataFrame(PyArrowTable(table)) # now we have a DataFrame with Py types

df_jl = mapcols(v -> pyconvert.(Any, v), df) # now we have Julia types
```
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
        # We special case `np.datetime64` since naive conversion fails
        # Note we end up converting to julia, then back to python!
        # This way the output of this function will be readable by Julia,
        # and we output python objects unconditionally.
        if Bool(np.issubdtype(w.dtype, np.datetime64))
            return PyArray(Py(map(convert_numpy_datetime64, w)))
        else
            return PyArray(w; copy=false)
        end
    end

    if n == 1
        return get_chunk(0)
    else
        return ChainedVector([get_chunk(i) for i in 0:(n - 1)])
    end
end

DataAPI.ncol(x::PyArrowTable) = pyconvert(Int, x.py.num_columns)
DataAPI.nrow(x::PyArrowTable) = pyconvert(Int, x.py.num_rows)

Tables.columns(df::PyArrowTable) = df
Tables.columnnames(df::PyArrowTable) = map(n -> pyconvert(Symbol, n), df.py.column_names)

function Tables.getcolumn(df::PyArrowTable, i::Int)
    return column_from_arrow(df.py[i - 1])
end

function Tables.getcolumn(df::PyArrowTable, nm::Symbol)
    return column_from_arrow(df.py[pystr(String(nm))])
end

function convert_numpy_datetime64(x)
    # Julia's DateTime only has microsecond precision,
    # so convert to microseconds
    py_ms = np.datetime64(x, "ms")

    # Not-a-time maps to missing
    if Bool(np.isnat(py_ms))
        return missing
    end
    # Convert to an integer
    ms = pyconvert(Int, py_ms.astype(np.dtype("int")))
    # unix2datetime expects seconds:
    return unix2datetime(ms / 1000)
end
