import os

import pytest

os.environ.setdefault("SPARK_MASTER", "local[2]")
os.environ.setdefault("SPARK_DRIVER_MEMORY", "2g")
os.environ.setdefault("SPARK_SHUFFLE_PARTITIONS", "2")
os.environ.setdefault("SPARK_LOG_LEVEL", "ERROR")


@pytest.fixture(scope="session")
def spark():
    from lakehouse.spark import build_spark

    session = build_spark("tests")
    yield session
    session.stop()
