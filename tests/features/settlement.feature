Feature: Settlement

    Tax rate for settlement transactions.

    Background:
        Given That a I have an external table with metrics about the seller relationship:
            | Seller_Id | lowest_tx_count_in_6_month | highest_tx_count_in_6_month |
            | S1        | 1800                       | 2752                        |
            | S2        | 2490                       | 3826                        |
            | S3        | 9600                       | 9700                        |
            | S4        | 9800                       | 9820                        |
            | S5        | 2750                       | 4000                        |


    # TIP: Using Scenario Outline: just as an example here. But for a situation like that a common scenario could run
    # faster comparing the entire result of the expectation table at once.
    Scenario Outline: Calculate the tax rate charged when settling transactions
        Given I'm the product owner of financial services

        When I have a bunch of transactions from all the sellers at the end of the month
            | Transaction_Id  | Seller_Id | Amount   |
            | 1               | S1        | 15652.4  |
            | 2               | S2        | 28624.3  |
            | 3               | S3        | 148245.2 |
            | 4               | S4        | 2458.9   |
            | 5               | S5        | 986821.0 |


        And The seller is not currently blocked:
            | Seller_Id | Region | Status  |
            | S1        | R01    | Active  |
            | S2        | R02    | Active  |
            | S3        | R03    | Active  |
            | S4        | R04    | Blocked |
            | S5        | R05    | Active  |

        And The seller is not listed int the fraud suspicion list:
            | Seller_Id |
            | S3        |

        And The seller has completed a minimum of 2500 transactions per day in the last 6 months

        And The company has a special promotion "Promotion XYZ" for the month in the seller's region

        And I run the tax calculation job

        Then The <Tax_Rate> and <Tax_Amount> calculated to settle the transactions for each <Seller_Id> with the <Tax_Condition> should match as the following:

        Examples:
            | Tax_Rate | Tax_Amount | Seller_Id | Tax_Condition |
            | 1.5      | 234.8      | S1        | DEFAULT       |
            | 0.7      | 200.4      | S2        | SPECIAL       |
            | 0.0      | 0.0        | S3        | FRAUD         |
            | 0.0      | 0.0        | S4        | BLOCKED       |
            | 1.5      | 14802.3    | S5        | DEFAULT       |