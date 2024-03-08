Feature: Loading rows into silver table.

    Scenario: De-duplicating rows from source
        Given I'm the product owner of financial services

        When I receive a streaming of data from my bronze table like the following:
            | Transaction_Id  | Customer_Id | Date          | Amount  |
            | 1               | A562        | 2023-15-03    | 15652.5 |
            | 1               | A562        | 2023-15-03    | 15652.5 |
        And I trigger the data transformation for silver table

        Then I should have in silver table only 1 distinct rows


    Scenario: Amount values check
        Given I'm the product owner of financial services

        When I receive a streaming of data from my bronze table like the following:
            | Transaction_Id  | Customer_Id | Date          | Amount  |
            | 1               | A562        | 2023-15-03    | 15652.5 |
            | 1               | A562        | 2023-15-03    | 15652.5 |
            | 2               | A426        | 2023-16-02    | -6524.2 |
            | 3               | H335        | 2023-17-02    | -1547.1 |
            | 4               | G876        | 2023-18-02    | -224.3  |
            | 5               | H635        | 2023-18-02    | 1784.3  |
            | 6               | L986        | 2023-18-02    | 2565.4  |
        And I trigger the data transformation for silver table

        Then I should have in silver table only the 3 rows with positive amount values
        And the 3 rows violating the constraints should be moved into recycle