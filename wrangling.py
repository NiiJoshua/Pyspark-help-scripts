fastest_player_list = silver_leaderboard_overall_df.where("leaderboard_id = 2 AND rank = 1").rdd.map(lambda x:x['user_id']).collect()
# using when and otherwise
users_df_with_filters = (users_sliced_df.withColumn("fastest_player_filters", when(users_sliced_df.user_id.isin(fastest_player_list),"fastest-player").otherwise(None))
