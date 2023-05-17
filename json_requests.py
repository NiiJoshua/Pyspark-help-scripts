# 1. send dataframe to an api using loop
import requests
import json

# create a SparkSession object
spark = SparkSession.builder.appName("ConvertTableToJSON").getOrCreate()

# define the input table schema
schema = "user_id INT, user_name STRING, points INT"

# create the input DataFrame
input_data = [(1, "Alice", 100), (2, "Bob", 200), (3, "Charlie", 300)]
df = spark.createDataFrame(input_data, schema)

# convert the DataFrame to JSON format
json_data = df.toJSON().collect()

# loop through the user IDs and send user_name and points to an endpoint
endpoint_url = "https://example.com/api/user/"
for row in df.rdd.collect():
    user_id = row.user_id
    user_name = row.user_name
    points = row.points
    endpoint = endpoint_url + str(user_id)
    data = {"user_name": user_name, "points": points}
    response = requests.post(endpoint, json.dumps(data))
    print(f"Sent data for user {user_id}, response: {response.text}")

# convert df to json
json_strings = df.toJSON().collect()
payload = json.dumps([json.loads(js) for js in json_strings])

# Create a json file
dictionary = {
    "name": "monterosa",
    "number": 56,
    "phonenumber": "9976770500"
}
 
with open("sample.json", "w") as outfile:
    json.dump(dictionary, outfile)

   #Read the file
with open('sample.json', 'r') as openfile:
 
    # Reading from json file
    json_object = openfile.read()#json.load(openfile)
 
print(json_object)
print(type(json_object))

# Check if a package is installed 
import pkg_resources

def is_package_installed(package_name):
    try:
        pkg_resources.get_distribution(package_name)
        return True
    except pkg_resources.DistributionNotFound:
        return False
    
      # Test the function above
is_kafka_installed = is_package_installed("confluent-kafka")
if is_kafka_installed:
    print("Kafka is installed.")
else:
    print("Kafka is not installed.")