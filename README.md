# PyArrow

[![Build Status](https://github.com/ericphanson/PyArrow.jl/actions/workflows/CI.yml/badge.svg?branch=main)](https://github.com/ericphanson/PyArrow.jl/actions/workflows/CI.yml?query=branch%3Amain)
[![Coverage](https://codecov.io/gh/ericphanson/PyArrow.jl/branch/main/graph/badge.svg)](https://codecov.io/gh/ericphanson/PyArrow.jl)

## Installation

This package uses [`PythonCall`](https://cjdoris.github.io/PythonCall.jl) to make
[pyarrow](https://arrow.apache.org/docs/python/index.html) available from within Julia. Unsurprisingly,
pyarrow and its dependencies need to be installed in order for this to work
and PyArrow.jl will attempt to install when the package is built: this should happen
more or less automatically via [`CondaPkg`](https://github.com/cjdoris/CondaPkg.jl).
You can configure various options via `CondaPkg`.

See [Arrow.jl](https://github.com/apache/arrow-julia) for a pure-Julia alternative. PyArrow.jl can be useful for testing cross-language interoperability for Arrow.jl-powered serialization.

## Usage

In the same philosophy as PythonCall, this allows for the transparent use of
pyarrow from within Julia.
The major things the package does are wrap the installation of pyarrow in the
package installation, export `pyarrow`, and re-export PythonCall.
After that, it's just a Python package accessible via `using PyArrow` in
Julia. The usual conversion rules and behaviors from PythonCall apply.
The [tests](test/runtests.jl) test a few conversion gotchas.

PyArrow also supplies two helper functions:

- `PyArrow.table`, to convert Tables.jl-compatible tables to pyarrow's in-memory format
- `PyArrowTable`, to wrap pyarrow tables in a Tables.jl-compatible interface

These are zero-copy when possible, but it is not guaranteed. See the [tests](./test/runtests.jl) for some cases of what works and what doesn't work.

## Examples

Here we translate some of the [Getting Started](https://arrow.apache.org/docs/python/getstarted.html) pyarrow docs.

First, constructing a table with a list-of-lists:

```julia
using PyArrow
import PyArrow: pyarrow as pa

days = pa.array([1, 12, 17, 23, 28], type=pa.int8())
months = pa.array([1, 3, 5, 7, 1], type=pa.int8())
years = pa.array([1990, 2000, 1995, 2000, 1995], type=pa.int16())
birthdays_table = pa.table(pylist([days, months, years]),
                           names=pylist(["days", "months", "years"]))
```

This yields:

```julia-repl
julia> birthdays_table
Python:
pyarrow.Table
days: int8
months: int8
years: int16
----
days: [[1,12,17,23,28]]
months: [[1,3,5,7,1]]
years: [[1990,2000,1995,2000,1995]]
```


We can also write this as a Tables.jl-compatible table, then use `PyArrow.table` to convert it:
```julia
jl_table = (; days = Int8[1, 12, 17, 23, 28],
              months = Int8[1, 3, 5, 7, 1],
              years = Int16[1990, 2000, 1995, 2000, 1995])

birthdays_table = PyArrow.table(jl_table)
```

```julia-repl
Python:
pyarrow.Table
days: int8
months: int8
years: int16
----
days: [[1,12,17,23,28]]
months: [[1,3,5,7,1]]
years: [[1990,2000,1995,2000,1995]]
```

Given such a pyarrow table, we can also access it from Julia using a `PyArrowTable`:

```julia
jl_table = PyArrowTable(birthdays_table)

using DataFrames
df = DataFrame(jl_table)
```

which yields

```julia-repl
julia> df = DataFrame(jl_table)
5×3 DataFrame
 Row │ days  months  years
     │ Int8  Int8    Int16
─────┼─────────────────────
   1 │    1       1   1990
   2 │   12       3   2000
   3 │   17       5   1995
   4 │   23       7   2000
   5 │   28       1   1995
```

Note one may want to use `mapcols(v -> pyconvert.(Any, v), df)` to convert the columns and their elements to native-Julia objects. Here, this isn't necessary since all elements are numbers, but for strings it can be helpful:

```julia
jl_table = (; days = Int8[1, 12, 17, 23, 28],
              months = Int8[1, 3, 5, 7, 1],
              years = Int16[1990, 2000, 1995, 2000, 1995],
              str = ["a", "b", "c", "d", missing])
py_table = PyArrow.table(jl_table)

df = DataFrame(PyArrowTable(py_table))
```
which yields

```julia-repl
julia> df
5×4 DataFrame
 Row │ days  months  years  str
     │ Int8  Int8    Int16  Py
─────┼───────────────────────────
   1 │    1       1   1990  a
   2 │   12       3   2000  b
   3 │   17       5   1995  c
   4 │   23       7   2000  d
   5 │   28       1   1995  None
```

Note the element-type of the column `str` is `Py`. In particular:

```julia-repl
julia> df[1, :str]
Python: 'a'

julia> df[1, :str] == "a"
false
```

However, we can do
```julia
df_jl = mapcols(v -> pyconvert.(Any, v), df)
```

which yields

```julia-repl
julia> df_jl = mapcols(v -> pyconvert.(Any, v), df)
5×4 DataFrame
 Row │ days   months  years  str
     │ Int64  Int64   Int64  Union…
─────┼──────────────────────────────
   1 │     1       1   1990  a
   2 │    12       3   2000  b
   3 │    17       5   1995  c
   4 │    23       7   2000  d
   5 │    28       1   1995

```

We can also write it in parquet format:

```julia
const pq = pyimport("pyarrow.parquet")
pq.write_table(birthdays_table, "birthdays.parquet")
reloaded_birthdays = pq.read_table("birthdays.parquet")
```

Datasets:

```julia
const ds = pyimport("pyarrow.dataset")
ds.write_dataset(birthdays_table, "savedir", format="parquet",
                 partitioning=ds.partitioning(
                    pa.schema([birthdays_table.schema.field("years")])
                ), existing_data_behavior=pystr("overwrite_or_ignore"))
birthdays_dataset = ds.dataset("savedir", format="parquet", partitioning=pylist(["years"]))
birthdays_dataset.files
```

yields:

```julia-repl
julia> birthdays_dataset.files
Python: ['savedir/1990/part-0.parquet', 'savedir/1995/part-0.parquet', 'savedir/2000/part-0.parquet']
```

Batches:

```julia
data = (; f0 = @py([1, 2, 3, 4]),
        f1 = @py(["foo", "bar", "baz", nothing]),
        f2 = @py([true, nothing, false, true]))

batch = pa.RecordBatch.from_arrays(pylist(data), @py(["f0", "f1", "f2"]))
table = pa.Table.from_batches(pylist([batch for _ in 1:5]))
jl_table = PyArrowTable(table)
```

These used `ChainedVectors` to transparently represent the chunked columns:

```julia-repl
julia> Tables.getcolumn(jl_table, 1)
20-element SentinelArrays.ChainedVector{Int64, PyArray{Int64, 1, false, true, Int64}}:
 1
 ⋮
 4

julia> DataFrame(jl_table)
20×3 DataFrame
 Row │ f0     f1    f2
     │ Int64  Py    Py
─────┼────────────────────
   1 │     1  foo   True
   2 │     2  bar   None
   3 │     3  baz   False
  ⋮  │   ⋮     ⋮      ⋮
  19 │     3  baz   False
  20 │     4  None  True
           15 rows omitted
```
