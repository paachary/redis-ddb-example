import random
import boto3
from botocore.exceptions import ClientError

ddb_resource = boto3.resource("dynamodb", endpoint_url="http://localhost:8000")

players = ["Player-0"] * 2 + ["Player-1"] * 2 + ["Player-2"] * 2 + ["Player-3"] * 2 + ["Player-4"] * 2
games = ["Game-1"] * 3 + ["Game-2"] * 3 + ["Game-3"] * 3


def put_items_ddb(table_name=None):
    table = ddb_resource.Table(table_name)

    ddb_client = ddb_resource.meta.client

    try:
        player = random.choice(players)
        game = random.choice(games)

        segment_1 = player + "_" + game + "_" + str(random.randint(1, 10))
        segment_2 = game + "_" + player + "_" + str(random.randint(1, 10))
        score = random.randint(33, 99)

        print(segment_1)
        print(segment_2)

        table.put_item(
            Item={"partition_key": segment_1,
                  "player": player,
                  "game": game,
                  "score": score},
            ConditionExpression="attribute_not_exists(partition_key)"
        )

        table.put_item(
            Item={"partition_key": segment_2,
                  "player": player,
                  "game": game,
                  "score": score},
            ConditionExpression="attribute_not_exists(partition_key)"
        )

        table.update_item(
            Key={"partition_key": segment_1},
            UpdateExpression="SET score = score + :val",
            ExpressionAttributeValues={':val': score}
        )

        table.update_item(
            Key={"partition_key": segment_2},
            UpdateExpression="SET score = score + :val",
            ExpressionAttributeValues={':val': score}
        )

    except ClientError as e:
        print(f'{e.response["Error"]["Code"]}: {e.response["Error"]["Message"]}')
    else:
        print("Records entered")


if __name__ == "__main__":
    for _ in range(1000):
        put_items_ddb("leaderboard")
