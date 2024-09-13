fastest_player_list = silver_leaderboard_overall_df.where("leaderboard_id = 2 AND rank = 1").rdd.map(lambda x:x['user_id']).collect()
# using when and otherwise
users_df_with_filters = (users_sliced_df.withColumn("fastest_player_filters", when(users_sliced_df.user_id.isin(fastest_player_list),"fastest-player").otherwise(None)))

# create a new column of array type
concat_udf = udf(lambda x: [i for i in x if i is not None], ArrayType(StringType()))
concat_col = concat_ws(',',"fastest_player_filters","top_predictor_filters","top_quizzer_filters","top_player_filters")
array_col = concat_udf(array(concat_col))
leaderboard_avatar_df = users_df_with_filters.withColumn("avatar_label", array_col).withColumn("attributes",split(col("avatar_label")[0],','))

# Filter an array column with attributes
filtered_table = (new_silver_users
                    .filter(array_contains(col("attributes"), "top-quizzer") | array_contains(col("attributes"), "top-predictor") | array_contains(col("attributes"), "fastest-player") | array_contains(col("attributes"), "top-player"))
)

# write a udf and use in a column
top_player_udf = udf(lambda questions_answered, questions_correct: (questions_correct * 10000) + (10000 - questions_answered), IntegerType())
top_player = (questions_asked.join(questions_correct.selectExpr("user_id","questions_correct"),["user_id"])
                              .selectExpr("project_id","user_id","questions_answered","questions_correct")
                              .withColumn("top_player_score",top_player_udf(col("questions_answered"), col("questions_correct")))
                  )

# array to create an empty list of string type
df = df.withColumn("attributes",array().cast(ArrayType(StringType())))

# return only value of psyspark dataframe 
first_col_value = int(df.select("col1").collect()[0][0]) # case of an integer

# read json without using sc.paralelize after making a request
df = request.json()

# add nulls for attributes when there's none in the json api call
dataDf['attributes'] = dataDf.apply(lambda row: row.get('username', 'null'), axis=1)  # pandas
when(col("user_record").isNull(), None).otherwise(col("user_record.externalId")).alias("external_id") #spark
# example with spark
df.select("project_id",
                when(col("user_record").isNull(), None).otherwise(col("user_record.UserId")).alias("user_id"),
                when(col("user_record").isNull(), None).otherwise(col("user_record.username")).alias("display_name"),
                when(col("user_record").isNull(), None).otherwise(col("user_record.externalId")).alias("external_id"),
                when(col("user_record.attributes").isNotNull(), "user_record.attributes").otherwise(None).alias("attributes")
# attributes example with pandas
data = response.json()["data"]
dataDf = pd.DataFrame(data)
# userId = dataDf[["userId"].unique()
# userId
dataDf['attributes'] = dataDf.apply(lambda row: row.get('username', 'null'), axis=1)

Get metadata of a project from content_etl.silver_contentmap

# managing streaming with static data
streamingDF = spark.readStream.table("orders")
staticDF = spark.read.table("customers")

query = (streamingDF
  .join(staticDF, streamingDF.customer_id==staticDF.id, "inner")
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .table("orders_with_customer_info")
)

# rugby scratch pad
hashed = (joined_df.withColumn("event_name", when(col("event_name") == "Jakarta - Round 10", "World Cup Day 1").otherwise(col("event_name")))
           )

hashed_df = hashed.withColumn("content_type", when(hashed["content_type"] == "formula-e-group-driver-element", "team-selector-element")
                                 .when(hashed["content_type"] == "formula-e-fastest-lap-element", "predictor-element")
                                 .when(hashed["content_type"] == "formula-e-podium-position-element", "podium-predictor-element")
                                 .otherwise(hashed["content_type"]))

sampled_data = hashed_df.sample(withReplacement=False, fraction=0.005, seed=42)
json_data = sampled_data.toJSON().collect()

producer = ProducerTask(eventStreamsProducerConfig, kafka_topic_name, 3) 
producer.connect()

for row in json_data:
    # producer.send('topic_name', line.encode('utf-8'))
    producer.sendMessage(row.encode('utf-8'),None) 
------
fe_silv_users_df = (fe_silv_users
                                 .withColumn("external_idd",concat(lit('auth0|'),md5(fe_silv_users['external_id'])))
                                 .selectExpr("user_id","external_idd")
                                 .withColumnRenamed("external_idd", "external_id")
) 

# raise an error and stop workflow
class NoLeaderboardError(Exception):
    pass

if quiz_df.isEmpty():
      raise NoLeaderboardError(f"No Leaderboard data found for event ID: {event_id}")

# Is in
events_df = spark.read.format("delta").load(silver_events_path).select("event_id").distinct().collect()
events_list =  [row.event_id for row in events_df]

# Fanatics stream test
stream_run = (stream_df.writeStream
    .format("kinesis") 
    .option("checkpointLocation", firehose_checkpoint)
    .option("maxBytesPerTrigger", "2097152")
    .option("maxRecordsPerTrigger", "10")
    .trigger(processingTime='30 seconds')
    .foreachBatch(writeToFirehose) 
    .start()
)