import argparse
import datetime
import findspark
import logging
import numpy as np
import pandas as pd
import random
import time

from pandarallel import pandarallel
from pyspark.sql import SparkSession


def generate_customer_profiles_table(n_customers, random_state=0):
    np.random.seed(random_state)

    customer_id_properties = []

    # Generate customer properties from random distributions
    for customer_id in range(n_customers):
        x_customer_id = np.random.uniform(0, 100)
        y_customer_id = np.random.uniform(0, 100)

        mean_amount = np.random.uniform(5, 100)  # Arbitrary (but sensible) value
        std_amount = mean_amount / 2  # Arbitrary (but sensible) value

        mean_nb_tx_per_day = np.random.uniform(0, 4)  # Arbitrary (but sensible) value

        customer_id_properties.append([customer_id,
                                       x_customer_id, y_customer_id,
                                       mean_amount, std_amount,
                                       mean_nb_tx_per_day])

    customer_profiles_table = pd.DataFrame(customer_id_properties, columns=['customer_id',
                                                                            'x_customer_id', 'y_customer_id',
                                                                            'mean_amount', 'std_amount',
                                                                            'mean_nb_tx_per_day'])

    return customer_profiles_table


def get_list_terminals_within_radius(customer_profile, x_y_terminals, r):
    # Use numpy arrays in the following to speed up computations

    # Location (x,y) of customer as numpy array
    x_y_customer = customer_profile[['x_customer_id', 'y_customer_id']].values.astype(float)

    # Squared difference in coordinates between customer and terminal locations
    squared_diff_x_y = np.square(x_y_customer - x_y_terminals)

    # Sum along rows and compute squared root to get distance
    dist_x_y = np.sqrt(np.sum(squared_diff_x_y, axis=1))

    # Get the indices of terminals which are at a distance less than r
    available_terminals = list(np.where(dist_x_y < r)[0])

    # Return the list of terminal IDs
    return available_terminals


def generate_transactions_table(customer_profile, start_date, nb_days):
    customer_transactions = []

    random.seed(int(customer_profile.customer_id))
    np.random.seed(int(customer_profile.customer_id))

    # For all days
    for day in range(nb_days):

        # Random number of transactions for that day
        nb_tx = np.random.poisson(customer_profile.mean_nb_tx_per_day)

        # If nb_tx positive, let us generate transactions
        if nb_tx > 0:

            for tx in range(nb_tx):

                # Time of transaction: Around noon, std 20000 seconds. This choice aims at simulating the fact that
                # most transactions occur during the day.
                time_tx = int(np.random.normal(86400 / 2, 20000))

                # If transaction time between 0 and 86400, let us keep it, otherwise, let us discard it
                if (time_tx > 0) and (time_tx < 86400):

                    # Amount is drawn from a normal distribution
                    amount = np.random.normal(customer_profile.mean_amount, customer_profile.std_amount)

                    # If amount negative, draw from a uniform distribution
                    if amount < 0:
                        amount = np.random.uniform(0, customer_profile.mean_amount * 2)

                    amount = np.round(amount, decimals=2)

                    if len(customer_profile.available_terminals) > 0:
                        terminal_id = random.choice(customer_profile.available_terminals)

                        customer_transactions.append([time_tx + day * 86400, day,
                                                      customer_profile.customer_id,
                                                      terminal_id, amount])

    customer_transactions = pd.DataFrame(customer_transactions,
                                         columns=['tx_time_seconds', 'tx_time_days', 'customer_id', 'terminal_id',
                                                  'tx_amount'])

    if len(customer_transactions) > 0:
        customer_transactions['tx_datetime'] = pd.to_datetime(customer_transactions["tx_time_seconds"], unit='s',
                                                              origin=start_date)
        customer_transactions = customer_transactions[
            ['tx_datetime', 'customer_id', 'terminal_id', 'tx_amount', 'tx_time_seconds', 'tx_time_days']]

    return customer_transactions


def generate_dataset(n_customers=50000, n_terminals=100, nb_days=1, start_date="2023-01-01", r=5):
    start_time = time.time()
    customer_profiles_table = generate_customer_profiles_table(n_customers, random_state=0)
    logging.info("Time to generate customer profiles table: {0:.3f}s".format(time.time() - start_time))

    start_time = time.time()
    terminal_profiles_table = generate_terminal_profiles_table(n_terminals, random_state=1)
    logging.info("Time to generate terminal profiles table: {0:.3f}s".format(time.time() - start_time))

    start_time = time.time()
    x_y_terminals = terminal_profiles_table[['x_terminal_id', 'y_terminal_id']].values.astype(float)
    # customer_profiles_table['available_terminals'] = customer_profiles_table.apply(
    #     lambda x: get_list_terminals_within_radius(x, x_y_terminals=x_y_terminals, r=r), axis=1)
    # With Pandarallel
    customer_profiles_table['available_terminals'] = customer_profiles_table\
        .parallel_apply(lambda x: get_list_terminals_within_radius(x, x_y_terminals=x_y_terminals, r=r), axis=1)
    customer_profiles_table['nb_terminals'] = customer_profiles_table.available_terminals.apply(len)
    logging.info("Time to associate terminals to customers: {0:.3f}s".format(time.time() - start_time))

    start_time = time.time()
    # transactions_df = customer_profiles_table.groupby('customer_id').apply(
    #     lambda x: generate_transactions_table(x.iloc[0], start_date, nb_days=nb_days)).reset_index(drop=True)
    # With Pandarallel
    transactions_df = customer_profiles_table.groupby('customer_id')\
        .parallel_apply(lambda x: generate_transactions_table(x.iloc[0], start_date, nb_days=nb_days))\
        .reset_index(drop=True)
    logging.info("Time to generate transactions: {0:.3f}s".format(time.time() - start_time))

    # Sort transactions chronologically
    transactions_df = transactions_df.sort_values('tx_datetime')
    # Reset indices, starting from 0
    transactions_df.reset_index(inplace=True, drop=True)
    transactions_df.reset_index(inplace=True)
    # TRANSACTION_ID are the dataframe indices, starting from 0
    transactions_df.rename(columns={'index': 'transaction_id'}, inplace=True)

    return customer_profiles_table, terminal_profiles_table, transactions_df


def generate_terminal_profiles_table(n_terminals, random_state=0):
    np.random.seed(random_state)

    terminal_id_properties = []

    # Generate terminal properties from random distributions
    for terminal_id in range(n_terminals):
        x_terminal_id = np.random.uniform(0, 100)
        y_terminal_id = np.random.uniform(0, 100)

        terminal_id_properties.append([terminal_id,
                                       x_terminal_id, y_terminal_id])

    terminal_profiles_table = pd.DataFrame(terminal_id_properties, columns=['terminal_id',
                                                                            'x_terminal_id', 'y_terminal_id'])

    return terminal_profiles_table


def add_frauds(customer_profiles_table, terminal_profiles_table, transactions_df):
    # By default, all transactions are genuine
    transactions_df['tx_fraud'] = 0
    transactions_df['tx_fraud_scenario'] = 0

    # Scenario 1
    transactions_df.loc[transactions_df.tx_amount > 220, 'tx_fraud'] = 1
    transactions_df.loc[transactions_df.tx_amount > 220, 'tx_fraud_scenario'] = 1
    nb_frauds_scenario_1 = transactions_df.tx_fraud.sum()
    logging.info("Number of frauds from scenario 1: " + str(nb_frauds_scenario_1))

    # Scenario 2
    for day in range(transactions_df.tx_time_days.max()):
        compromised_terminals = terminal_profiles_table.terminal_id.sample(n=2, random_state=day)

        compromised_transactions = transactions_df[(transactions_df.tx_time_days >= day) &
                                                   (transactions_df.tx_time_days < day + 28) &
                                                   (transactions_df.terminal_id.isin(compromised_terminals))]

        transactions_df.loc[compromised_transactions.index, 'tx_fraud'] = 1
        transactions_df.loc[compromised_transactions.index, 'tx_fraud_scenario'] = 2

    nb_frauds_scenario_2 = transactions_df.tx_fraud.sum() - nb_frauds_scenario_1
    logging.info("Number of frauds from scenario 2: " + str(nb_frauds_scenario_2))

    # Scenario 3
    for day in range(transactions_df.tx_time_days.max()):
        compromised_customers = customer_profiles_table.customer_id.sample(n=3, random_state=day).values

        compromised_transactions = transactions_df[(transactions_df.tx_time_days >= day) &
                                                   (transactions_df.tx_time_days < day + 14) &
                                                   (transactions_df.customer_id.isin(compromised_customers))]

        nb_compromised_transactions = len(compromised_transactions)

        random.seed(day)
        index_fauds = random.sample(list(compromised_transactions.index.values), k=int(nb_compromised_transactions / 3))

        transactions_df.loc[index_fauds, 'tx_amount'] = transactions_df.loc[index_fauds, 'tx_amount'] * 5
        transactions_df.loc[index_fauds, 'tx_fraud'] = 1
        transactions_df.loc[index_fauds, 'tx_fraud_scenario'] = 3

    nb_frauds_scenario_3 = transactions_df.tx_fraud.sum() - nb_frauds_scenario_2 - nb_frauds_scenario_1
    logging.info("Number of frauds from scenario 3: " + str(nb_frauds_scenario_3))

    return transactions_df


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    findspark.init()
    findspark.find()

    pandarallel.initialize(progress_bar=True)

    parser = argparse.ArgumentParser()
    parser.add_argument("--i_customers", type=int, default=50000)
    parser.add_argument("--i_terminals", type=int, default=1000)
    parser.add_argument("--i_days", type=int, default=1)
    parser.add_argument("--i_start_date", type=str, default=datetime.date.today().strftime("%Y-%m-%d"))
    parser.add_argument("--hdfs_host", type=str, required=True)
    parser.add_argument("--hdfs_dir_output", type=str, required=True)
    parser.add_argument("--s3_bucket", type=str)
    parser.add_argument("--s3_bucket_prefix", type=str)
    args = parser.parse_args()

    logging.info(f"{args.i_customers=}")
    logging.info(f"{args.i_terminals=}")
    logging.info(f"{args.i_days=}")
    logging.info(f"{args.i_start_date=}")
    logging.info(f"{args.hdfs_host=}")
    logging.info(f"{args.hdfs_dir_output=}")
    logging.info(f"{args.s3_bucket=}")
    logging.info(f"{args.s3_bucket_prefix=}")

    spark = (
        SparkSession.builder
        .appName("generate_transaction_data")
        .master("yarn")
        .config("spark.sql.broadcastTimeout", str(60 * 60 * 3))
        .getOrCreate()
    )
    spark.conf.set('spark.sql.repl.eagerEval.enabled', True)

    (customer_profiles_table, terminal_profiles_table, transactions_df) = \
        generate_dataset(n_customers=args.i_customers,
                         n_terminals=args.i_terminals,
                         nb_days=args.i_days,
                         start_date=args.i_start_date,
                         r=5)

    x_y_terminals = terminal_profiles_table[['x_terminal_id', 'y_terminal_id']].values.astype(float)
    # And get the list of terminals within radius of $50$ for the last customer
    get_list_terminals_within_radius(customer_profiles_table.iloc[4], x_y_terminals=x_y_terminals, r=50)

    customer_profiles_table['available_terminals'] = customer_profiles_table.apply(
        lambda x: get_list_terminals_within_radius(x, x_y_terminals=x_y_terminals, r=50), axis=1)

    transactions_df = add_frauds(customer_profiles_table, terminal_profiles_table, transactions_df)

    start_date = datetime.datetime.strptime(args.i_start_date, "%Y-%m-%d")

    for day in range(transactions_df.tx_time_days.max() + 1):
        transactions_day_df = transactions_df[transactions_df.tx_time_days == day].sort_values('tx_time_seconds')

        date = start_date + datetime.timedelta(days=day)
        filename_output = date.strftime("%Y-%m-%d") + '.parquet'

        spark_df = spark.createDataFrame(transactions_day_df)
        filepath_hdfs = f"hdfs://{args.hdfs_host}{args.hdfs_dir_output}/{filename_output}"
        logging.info(f"Writing dataframe to {filepath_hdfs=}")
        spark_df.write.parquet(filepath_hdfs, mode="overwrite")

        # if args.save_to_s3:
        filepath_s3 = f"s3a://{args.s3_bucket}{args.s3_bucket_prefix}/{filename_output}"
        logging.info(f"Uploading data to {filepath_s3=}")
        spark_df.write.parquet(filepath_s3, mode="overwrite")

    spark.stop()


if __name__ == "__main__":
    main()
