fastest_player_list = silver_leaderboard_overall_df.where("leaderboard_id = 2 AND rank = 1").rdd.map(lambda x:x['user_id']).collect()
# using when and otherwise
users_df_with_filters = (users_sliced_df.withColumn("fastest_player_filters", when(users_sliced_df.user_id.isin(fastest_player_list),"fastest-player").otherwise(None))

# create a new column of array type
concat_udf = udf(lambda x: [i for i in x if i is not None], ArrayType(StringType()))
concat_col = concat_ws(',',"fastest_player_filters","top_predictor_filters","top_quizzer_filters","top_player_filters")
array_col = concat_udf(array(concat_col))
leaderboard_avatar_df = users_df_with_filters.withColumn("avatar_label", array_col).withColumn("attributes",split(col("avatar_label")[0],','))

# Filter an array column with attributes
filtered_table = (new_silver_users
                    .filter(array_contains(col("attributes"), "top-quizzer") | array_contains(col("attributes"), "top-predictor") | array_contains(col("attributes"), "fastest-player") | array_contains(col("attributes"), "top-player"))
)