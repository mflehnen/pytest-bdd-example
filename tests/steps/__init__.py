from pyspark.sql import DataFrame


def _split_row(row):
    """
    transforms the gherkin table row into a list
    """
    return str(row).strip("|").split("|")


def parse_gherkin_table(spark_session, table_with_headers) -> DataFrame:
    """
    Transforms the gherkin table into a dataframe
    """
    list_table_rows = table_with_headers.split("\n")
    headers = [name.strip() for name in _split_row(list_table_rows[0])]
    data = [[value.strip() for value in _split_row(row)] for row in list_table_rows[1:]]

    df = spark_session.createDataFrame(data, headers)
    return df
