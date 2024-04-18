# TradeExecutionPipeline
Apache Airflow DAG that moves trade executions downloaded from a Interactive Brokers account to a PostgeSQL database.

## How it works
A report containing a days trade exections lands in the `landing` directory in the form of a csv file. The workflow reads
in the trade data, performs some checks and transformations before writing the executions to the database.