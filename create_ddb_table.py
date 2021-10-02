import boto3
import random


ddb_resource = boto3.resource("dynamodb", endpoint_url="http://localhost:8000")
ddb_client = ddb_resource.meta.client
table_name = "leaderboard"

players = ["Player-1"] * 2 + ["Player-2"] * 2 + ["Player-3"] * 2 + ["Player-4"] * 2 + ["Player-5"] * 2
games = ["Game-1"] * 3 + ["Game-2"] * 3 + ["Game-3"] * 3

response = ddb_client.create_table(
    TableName=table_name,
    KeySchema=[
        {"AttributeName": "partition_key", "KeyType": "HASH"},
    ],
    AttributeDefinitions=[
        {"AttributeName": "partition_key", "AttributeType": "S"},
    ],
    BillingMode="PAY_PER_REQUEST",
)
print(response)

print(f"Waiting for table leaderboard...")
waiter = ddb_client.get_waiter("table_exists")
waiter.wait(TableName=table_name)
response = ddb_client.describe_table(TableName=table_name)
print(response["Table"]["TableStatus"])
