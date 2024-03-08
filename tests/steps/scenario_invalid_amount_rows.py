import pytest
from pytest_bdd import scenario, given, when, then, parsers

from src.data_transformation import my_data_transformation_function
from tests.steps import parse_gherkin_table


@pytest.fixture
def test_context():
    """
    Defining a context variable for sharing data between steps
    """
    return {}


@scenario('../features/silver_data.feature', 'Amount values check')
def test_silver_data_cleansing():
    # invoking the feature
    pass


@given('''I'm the product owner of financial services''')
def job_runner(spark_session, test_context):
    pass


@when(parsers.parse('I receive a streaming of data from my bronze table like the following:\n{attr_value_table}'))
def bronze_streaming(spark_session, attr_value_table, test_context):
    df = parse_gherkin_table(spark_session, attr_value_table)
    test_context['source_data'] = df
    df.show(truncate=False)


@when('I trigger the data transformation for silver table')
def bronze_streaming(test_context):
    # call the transformation function
    source_df = test_context['source_data']
    valid_df, recycle_df = my_data_transformation_function(source_df)
    test_context['valid_data'] = valid_df
    test_context['recycle_data'] = recycle_df


@then(parsers.parse('I should have in silver table only the {expected_count:d} rows with positive amount values'))
def row_count_validation(expected_count, test_context):
    # TODO:
    # assert if the silver table count matches the expectation from the feature
    valid_df = test_context['valid_data']
    assert expected_count == valid_df.count()


@then(parsers.parse('the {duplicated_count:d} rows violating the constraints should be moved into recycle'))
def recycle_validation(duplicated_count, test_context):
    # TODO:
    # assert if a recycle table count matches the expectation from the feature
    recycle_df = test_context['recycle_data']
    assert duplicated_count == recycle_df.count()

