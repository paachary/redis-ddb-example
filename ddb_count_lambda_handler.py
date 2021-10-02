import boto3
from botocore.exceptions import ClientError

ddb_resource = boto3.resource("dynamodb", endpoint_url="http://localhost:8000")
table_name = "leaderboard"

table = ddb_resource.Table(table_name)

no_of_players = 5

try:
    items = table.scan()["Items"]

    #print(items)

    for player_count in range(no_of_players):
        player_total = 0
        player_game_1_total = 0
        player_game_2_total = 0
        player_game_3_total = 0
        for i in items:
            if "score" in i:
                if i["partition_key"].startswith("Player-" + str(player_count)):
                    player_total = player_total + i["score"]
                if i["partition_key"].startswith("Player-" + str(player_count) + "_Game-1"):
                    player_game_1_total = player_game_1_total + i["score"]
                if i["partition_key"].startswith("Player-" + str(player_count) + "_Game-2"):
                    player_game_2_total = player_game_2_total + i["score"]
                if i["partition_key"].startswith("Player-" + str(player_count) + "_Game-3"):
                    player_game_3_total = player_game_3_total + i["score"]

        table.update_item(
            Key={"partition_key": "Player-" + str(player_count)},
            UpdateExpression="Set total_score = :v1, game1_score= :v2, game2_score= :v3 , game3_score= :v4",
            ExpressionAttributeValues={":v1": player_total, ":v2": player_game_1_total,
                                       ":v3": player_game_2_total, ":v4": player_game_3_total})

        print(f"Total count of scores for Player-{str(player_count)} = {player_total}")
        print(f"Total count of scores for each games Player-{player_count} > "
              f"Game1 = {player_game_1_total}; "
              f"Game2 = {player_game_2_total}; "
              f"Game3 = {player_game_3_total}")

except ClientError as e:
    print(f'{e.response["Error"]["Code"]}: {e.response["Error"]["Message"]}')
