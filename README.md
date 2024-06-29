# PyArrow

[![Build Status](https://github.com/ericphanson/PyArrow.jl/actions/workflows/CI.yml/badge.svg?branch=main)](https://github.com/ericphanson/PyArrow.jl/actions/workflows/CI.yml?query=branch%3Amain)
[![Coverage](https://codecov.io/gh/ericphanson/PyArrow.jl/branch/main/graph/badge.svg)](https://codecov.io/gh/ericphanson/PyArrow.jl)

## Installation

This package uses [`PythonCall`](https://cjdoris.github.io/PythonCall.jl) to make
[pyarrow](https://arrow.apache.org/docs/python/index.html) and [pandas](https://pandas.pydata.org/) available from within Julia. Unsurprisingly,
pyarrow and its dependencies need to be installed in order for this to work
and PyArrow.jl will attempt to install when the package is built: this should happen
more or less automatically via [`CondaPkg`](https://github.com/cjdoris/CondaPkg.jl).
You can configure various options via `CondaPkg`.

See [Arrow.jl](https://github.com/apache/arrow-julia) for a pure-Julia alternative.

## Usage

In the same philosophy as PythonCall, this allows for the transparent use of
pyarrow from within Julia.
The major things the package does are wrap the installation of pyarrow in the
package installation, export `pyarrow`, and re-export PythonCall.
After that, it's just a Python package accessible via `using PyArrow` in
Julia. The usual conversion rules and behaviors from PythonCall apply.
The [tests](test/runtests.jl) test a few conversion gotchas.

PyArrow supplies one helper function, `PyArrow.table`, to convert Tables.jl-compatible tables to pyarrow's in-memory format.

## Examples

Here we translate the [Getting Started](https://arrow.apache.org/docs/python/getstarted.html) pyarrow docs.

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

```julia
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

```julia
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


We can write it in parquet format:

```julia
const pq = pyimport("pyarrow.parquet")
pq.write_table(birthdays_table, "birthdays.parquet")
reloaded_birthdays = pq.read_table("birthdays.parquet")
```
