import boto3
import json
import pandas as pd
import re
import requests
from PIL import Image
from io import BytesIO

dynamodb = boto3.client("dynamodb", region_name="us-east-2")


def query_dynamodb_table(benchmark_id):
    # Initial query parameters
    query_params = {
        "TableName": "benchmark-data",  # replace with your table name
        "KeyConditionExpression": "benchmark_id = :benchmarkValue",
        "IndexName": "benchmark_id-timestamp-index",
        "ExpressionAttributeValues": {":benchmarkValue": {"S": benchmark_id}},
    }

    while True:
        # Execute the query
        response = dynamodb.query(**query_params)
        # Yield each item
        for item in response["Items"]:
            yield item

        # If there's more data to be retrieved, update the ExclusiveStartKey
        if "LastEvaluatedKey" in response:
            query_params["ExclusiveStartKey"] = response["LastEvaluatedKey"]
        else:
            break


def get_rows_for_pd(benchmark_id):
    for item in query_dynamodb_table(benchmark_id):
        timestamp = item["timestamp"]["N"]
        data = json.loads(item["data"]["S"])
        system = data["system_info"]
        del data["system_info"]

        row = {**data, **system, "timestamp": timestamp}
        yield row


def get_df_for_benchmark(benchmark_id):
    df = pd.DataFrame(get_rows_for_pd(benchmark_id))
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    return df


def performance_score(gpu_name):
    # Extract the number part using regex
    match = re.search(r"(\d+)", gpu_name)
    if match:
        number = int(match.group(1))
    else:
        return 0  # Default performance score in case no number is found

    # Check for 'Ti', 'Laptop', and combinations
    if "Ti" in gpu_name and "Laptop" in gpu_name:
        return number + 0.3
    elif "Ti" in gpu_name:
        return number + 0.5
    elif "Laptop" in gpu_name:
        return number
    else:
        return number + 0.1


def shorten_gpu_name(full_name):
    shortened = []
    for name in full_name.split("\n"):
        # Extract the GPU model number, any 'Ti' suffix, and "Laptop GPU" distinction
        match = re.search(r"(RTX|GTX) (\d{3,4})( Ti)?( Laptop GPU)?", name)
        if match:
            shortened.append(
                match.group(1)
                + " "
                + match.group(2)
                + (match.group(3) or "")
                + (" Laptop" if match.group(4) else "")
            )
        else:
            shortened.append(name)
    return " & ".join(shortened)


# A function to load an image from a url
def load_image(url):
    response = requests.get(url)
    img = Image.open(BytesIO(response.content))
    return img


def dict_to_md_list(dictionary: dict):
    if dictionary is None:
        return None
    return "\n".join(["- **{}**: {}".format(k, v) for k, v in dictionary.items()])


def dict_to_html_list(dictionary: dict):
    if dictionary is None:
        return None
    return (
        "<ul>"
        + "".join([f"<li><b>{k}</b>: {v}</li>" for k, v in dictionary.items()])
        + "</ul>"
    )
