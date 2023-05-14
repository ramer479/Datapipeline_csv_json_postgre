from pyspark.sql.functions import split, lit, col, trim, lower, md5, concat_ws, date_format, current_date, \
    current_timestamp, to_date


# Create the user_dimension DataFrame


def create_user_dimension(df, dta_src):
    if dta_src == 'wwc':
        user_dimension = df.select(
            md5(concat_ws("", trim(col("first")),
                          trim(col("last")), col("email"))).alias("user_id"),
            col("gender"),
            trim(col("title")).alias("title"),
            trim(col("first")).alias("first_name"),
            trim(col("last")).alias("last_name"),
            col("email"),
            to_date(col("dob")).alias("dob"),
            to_date(col("registered")).alias("registered_date"),
            col("phone"),
            col("cell"),
            lit("").alias("ip_address"),  # As mentioned, the ip_address column will be empty
            lit("wwc").alias("source")  # Adding a constant column 'source' with value 'wwc'
        )
        user_dimension = user_dimension.withColumn("load_ingstn_id", date_format(current_date(), "yyyyMMdd")) \
            .withColumn("load_dtm", current_timestamp())

        # Display the DataFrame
        user_dim = user_dimension.dropDuplicates(['user_id'])
        user_dim.show()
        return user_dim
    elif dta_src == 'hb':
        user_dimension = df.select(
            md5(concat_ws("", trim(col("first_name")),
                          trim(col("last_name")), col("email"))).alias("user_id"),
            lower(col("gender")),
            lit("").alias("title"),
            trim(lower(col("first_name"))).alias("first_name"),
            trim(lower(col("last_name"))).alias("last_name"),
            col("email"),
            to_date(col("dob"), "M/d/yyyy").alias("dob"),
            to_date(lit("")).alias("registered_date"),
            lit("").alias("phone"),
            lit("").alias("cell"),
            col("ip_address"),
            lit("hb").alias("source")  # Adding a constant column 'source' with value 'hb'
        )
        user_dimension = user_dimension.withColumn("load_ingstn_id", date_format(current_date(), "yyyyMMdd")) \
            .withColumn("load_dtm", current_timestamp())
        user_dimension.show()
        user_dim = user_dimension.dropDuplicates(['user_id'])
        user_dim.show()
        return user_dim


def create_location_dimension(df, dta_src):
    if dta_src == "wwc":
        dim_location = df.select(
            split(df["location"]["street"], ', ')[0].alias("street"),
            split(df["location"]["city"], ', ')[0].alias("city"),
            split(df["location"]["state"], ', ')[0].alias("state"),
            split(df["location"]["postcode"], ', ')[0].alias("postcode"),
            col("nat").alias("nationality")
        ).withColumn("load_ingstn_id", date_format(current_date(), "yyyyMMdd")) \
            .withColumn("load_dtm", current_timestamp())
        dim_location = dim_location.select(
            md5(concat_ws("", trim(col("street")),
                          trim(col("city")), col("state"), col("postcode"), col("nationality"))).alias("location_id"),
            "*"
        )
        dim_loc = dim_location.dropDuplicates(['location_id'])
        dim_loc.show(truncate=False)
        return dim_loc
    elif dta_src == "hb":
        dim_location = df.select(
            lit("street").alias("street"),
            lit("city").alias("city"),
            lit("state").alias("state"),
            lit("postcode").alias("postcode"),
            lit("nat").alias("nationality")
        ).withColumn("load_ingstn_id", date_format(current_date(), "yyyyMMdd")) \
            .withColumn("load_dtm", current_timestamp())
        dim_location = dim_location.select(
            md5(concat_ws("", trim(col("street")),
                          trim(col("city")), col("state"), col("postcode"), col("nationality"))).alias("location_id"),
            "*"
        )
        dim_loc = dim_location.dropDuplicates(['location_id'])
        dim_loc.show(truncate=False)
        return dim_loc


def create_fact_table(df, dim_user, dim_loc, src):
    # Join original dataframe with user_dim and location_dim
    if src == "wwc":
        fact_df = df.join(dim_user, (df['first'] == dim_user['first_name']) &
                          (df['last'] == dim_user['last_name']) &
                          (df['email'] == dim_user['email']), 'left') \
            .join(dim_loc, (df['location']['street'] == dim_loc['street']) &
                  (df['location']['city'] == dim_loc['city']) &
                  (df['location']['state'] == dim_loc['state']) &
                  (df['location']['postcode'] == dim_loc['postcode']) &
                  (df['nat'] == dim_loc['nationality']), 'left')

        # Select only the required columns
        fact_table = fact_df.select(
            dim_user['user_id'],
            dim_loc['location_id']) \
            .withColumn("source", lit(f"{src}")) \
            .withColumn("load_ingstn_id", date_format(current_date(), "yyyyMMdd")) \
            .withColumn("load_dtm", current_timestamp()) \
            .filter(dim_user['user_id'].isNotNull()) \
            .dropDuplicates(['user_id'])

        fact_table.show(truncate=False)
        return fact_table
    elif src == "hb":

        df_alias = df.withColumnRenamed("email", "df_email")
        dim_user_alias = dim_user.withColumnRenamed("email", "dim_email")
        fact_df = df_alias.join(dim_user_alias, (lower(df_alias['first_name']) == lower(dim_user_alias['first_name'])) &
                                (lower(df_alias['last_name']) == lower(dim_user_alias['last_name'])) &
                                (lower(df_alias['df_email']) == lower(dim_user_alias['dim_email'])), 'left')

        print("%%%%%%%%%%%% Fact df after ;left join")
        fact_df.show()
        # Select only the required columns
        fact_table = fact_df.select(dim_user['user_id']) \
            .withColumn("location_id", lit("")) \
            .withColumn("source", lit(f"{src}")) \
            .withColumn("load_ingstn_id", date_format(current_date(), "yyyyMMdd")) \
            .withColumn("load_dtm", current_timestamp()) \
            .filter(dim_user['user_id'].isNotNull()) \
            .dropDuplicates(['user_id'])

        fact_table.show(truncate=False)
        return fact_table
