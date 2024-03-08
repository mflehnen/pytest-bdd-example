import pytest

from src.spark_session_builder import SparkSessionBuilder


@pytest.fixture(scope="session")
def spark_session():
    session = SparkSessionBuilder.get_session()

    return session

