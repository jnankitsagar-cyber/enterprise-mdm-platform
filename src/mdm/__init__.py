"""Enterprise Master Data Management platform - core engine.

The package is engine-agnostic: every transformation is a pure function over a
record (dict). The same functions run
  * locally, over pandas / Python lists (this repo's demo), and
  * on Databricks, wrapped as (pandas_)UDFs inside PySpark jobs (see /spark).
"""
__version__ = "1.0.0"
