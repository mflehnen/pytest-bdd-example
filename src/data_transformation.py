from pyspark.sql import DataFrame
from pyspark.sql.functions import sum, expr


def my_data_transformation_function(df: DataFrame) -> (DataFrame, DataFrame):
    """
    a very simple transformation
    """
    deduplicated_data = df.drop_duplicates()

    valid_df = deduplicated_data.filter("amount > 0")
    recycle_df = deduplicated_data.filter("amount <= 0")

    return valid_df, recycle_df


def tax_rate_calculation(transactions_df: DataFrame, sellers_df: DataFrame, fraud_df: DataFrame) -> DataFrame:
    """
    a more complex transformation example
    """
    spark = transactions_df.sparkSession

    tax_df = spark.createDataFrame([
        ("DEFAULT", 1.5),
        ("SPECIAL", 0.7),
        ("FRAUD", 0.0),
        ("BLOCKED", 0.0)
    ], schema=["Tax_condition", "Tax_rate"])

    aggregated_data_df = (
        transactions_df.groupBy("Seller_Id")
        .agg(sum("Amount").alias("Amount"))
    )

    transformed_df = (
        aggregated_data_df.alias("a")
        .join(sellers_df.alias("s"), on="Seller_Id", how="left")
        .join(fraud_df.alias("f"), on="Seller_Id", how="left")
        .withColumn("Tax_Condition", expr("""
            CASE
                WHEN f.Seller_Id IS NOT NULL THEN 'FRAUD'
                WHEN s.Status = 'Blocked' THEN 'BLOCKED'
                WHEN s.Seller_Id = 'S2' THEN 'SPECIAL'
                ELSE 'DEFAULT'
            END """))
        .join(tax_df.alias("t"), on="Tax_Condition")
        .selectExpr("a.Seller_Id", "a.Amount as Base_Amount", "t.Tax_Condition", "t.Tax_Rate", "round(a.Amount * t.Tax_Rate / 100, 1) as Tax_Amount" )
    )

    return transformed_df

