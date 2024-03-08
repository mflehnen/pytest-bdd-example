import pytest
from pytest_bdd import scenario, given, when, then, parsers

from src.data_transformation import tax_rate_calculation
from tests.steps import parse_gherkin_table


@pytest.fixture
def test_context():
    """
    Defining a context variable for sharing data between steps
    """
    return {}


@scenario('../features/settlement.feature',
          scenario_name='Calculate the tax rate charged when settling transactions')
def test_settlement_transactions():
    # invoking the feature
    pass


@given(parsers.parse('That a I have an external table with metrics about the seller relationship:\n{attr_value_table}'))
def load_metrics_table(spark_session, attr_value_table, test_context):
    if 'metrics_data' in test_context.keys():
        df = test_context['metrics_data']
    else:
        df = parse_gherkin_table(spark_session, attr_value_table)
        test_context['metrics_data'] = df
    df.show(truncate=False)


@given('''I'm the product owner of financial services''')
def job_runner():
    pass


@when(parsers.parse('I have a bunch of transactions from all the sellers at the end of the month\n{attr_value_table}'))
def load_transactions(spark_session, attr_value_table, test_context):
    if 'source_data' in test_context.keys():
        df = test_context['source_data']
    else:
        df = parse_gherkin_table(spark_session, attr_value_table)
        test_context['source_data'] = df
    df.show(truncate=False)


@when(parsers.parse('The seller is not currently blocked:\n{attr_value_table}'))
def load_sellers(spark_session, attr_value_table, test_context):
    if 'sellers_data' in test_context.keys():
        df = test_context['sellers_data']
    else:
        df = parse_gherkin_table(spark_session, attr_value_table)
        test_context['sellers_data'] = df
    df.show(truncate=False)


@when(parsers.parse('The seller is not listed int the fraud suspicion list:\n{attr_value_table}'))
def load_fraudulent_sellers(spark_session, attr_value_table, test_context):
    if 'fraud_sellers_data' in test_context.keys():
        df = test_context['fraud_sellers_data']
    else:
        df = parse_gherkin_table(spark_session, attr_value_table)
        test_context['fraud_sellers_data'] = df
    df.show(truncate=False)


@when(parsers.parse('The seller has completed a minimum of {qty:d} transactions per day in the last 6 months'))
def validate_metrics(qty):
    pass


@when('''The company has a special promotion "Promotion XYZ" for the month in the seller's region''')
def validate_promotion():
    pass


@when('''I run the tax calculation job''')
def transform_data(test_context):
    transactions_df = test_context['source_data']
    sellers_df = test_context['sellers_data']
    fraud_df = test_context['fraud_sellers_data']

    settlements_df = tax_rate_calculation(transactions_df, sellers_df, fraud_df)
    test_context['settlements'] = settlements_df.collect()

    settlements_df.show(truncate=False)


@then(parsers.parse(
    '''The {tax_rate:f} and {tax_amount:f} calculated to settle the transactions for each {seller_id:w} with the {tax_condition:w} should match as the following:'''))
def validate_result(test_context, tax_rate, tax_amount, seller_id, tax_condition):
    row_list = test_context['settlements']
    row = next(filter(lambda row: row['Seller_Id'] == seller_id, row_list))
    assert row['Tax_Rate'] == tax_rate, "Tax_Rate"
    assert row['Tax_Amount'] == tax_amount, "Tax_Amount"
    assert row['Tax_Condition'] == tax_condition, "Tax_Condition"

