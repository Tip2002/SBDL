import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType

from lib.ConfigLoader import get_config


def get_party_schema():
    return StructType([
        StructField("load_date", DateType(), True),
        StructField("account_id", StringType(), True),
        StructField("party_id", StringType(), True),
        StructField("relation_type", StringType(), True),
        StructField("relation_start_date", TimestampType(), True)
    ])


def get_account_schema():
    return StructType([
        StructField("load_date", DateType(), True),
        StructField("active_ind", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("source_sys", StringType(), True),
        StructField("account_start_date", TimestampType(), True),
        StructField("legal_title_1", StringType(), True),
        StructField("legal_title_2", StringType(), True),
        StructField("tax_id_type", StringType(), True),
        StructField("tax_id", StringType(), True),
        StructField("branch_code", StringType(), True),
        StructField("country", StringType(), True)
    ])


def get_address_schema():
    return StructType([
        StructField("load_date", DateType(), True),
        StructField("party_id", StringType(), True),
        StructField("address_line_1", StringType(), True),
        StructField("address_line_2", StringType(), True),
        StructField("city", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("country_of_address", StringType(), True),
        StructField("address_start_date", DateType(), True)
    ])


def resolve_schema(schema):
    # Treat string "null" as None
    if schema == "null":
        return None

    if schema is None:
        return None
    if isinstance(schema, StructType):
        return schema
    if isinstance(schema, str):
        s = schema.lower()
        if s == "account":
            return get_account_schema()
        elif s == "party":
            return get_party_schema()
        elif s == "address":
            return get_address_schema()
        else:
            raise ValueError(f"Unknown schema name string passed: {schema}")
    raise ValueError(f"Invalid schema type passed: {type(schema)}")


def read_accounts(spark: SparkSession, env: str, header: bool = True, schema=None):
    schema = resolve_schema(schema) or get_account_schema()
    config = get_config(env)
    path = config.get("input.accounts.path", "test_data/accounts")
    print(f"Reading accounts data from: {path}")
    print(f"Using schema: {schema.simpleString()}")
    return spark.read.option("header", str(header).lower()) \
                     .schema(schema) \
                     .csv(path)


def read_parties(spark: SparkSession, env: str, header: bool = True, schema=None):
    schema = resolve_schema(schema) or get_party_schema()
    config = get_config(env)
    path = config.get("input.parties.path", "test_data/parties")
    print(f"Reading parties data from: {path}")
    print(f"Using schema: {schema.simpleString()}")
    return spark.read.option("header", str(header).lower()) \
                     .schema(schema) \
                     .csv(path)


def read_address(spark: SparkSession, env: str, header: bool = True, schema=None):
    schema = resolve_schema(schema) or get_address_schema()
    config = get_config(env)
    path = config.get("input.party_address.path", "test_data/party_address")
    print(f"Reading address data from: {path}")
    print(f"Using schema: {schema.simpleString()}")
    return spark.read.option("header", str(header).lower()) \
                     .schema(schema) \
                     .csv(path)
