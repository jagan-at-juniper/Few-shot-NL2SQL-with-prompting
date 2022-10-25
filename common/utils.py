
def get_correlation_matrix_from_df(df_input, input_cols=[], vector_col="corr_features"):
    """
    return correlation matrix from pyspark dataframe
    :param df_input:
    :param input_cols:
    :param vector_col:
    :return:
    """
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.stat import Correlation

    assembler = VectorAssembler(inputCols=input_cols,
                                outputCol=vector_col)
    myGraph_vector = assembler.transform(df_input).select(vector_col)
    # matrix = Correlation.corr(myGraph_vector, vector_col)
    corrmatrix = Correlation.corr(myGraph_vector, vector_col).collect()[0][0]
    # corrmatrix = matrix.toArray().tolist()
    corrmatrix = [[round(x1, 4) for x1 in x ] for x in corrmatrix]

    print(corrmatrix)
    df_corr = spark.createDataFrame(corrmatrix, input_cols)
    df_corr.show()
    return df_corr


def flatten_radios(df):
    """
    flatten radios: r0/r1/r2
    :param df:
    :return:
    """
    df_radios = df.select("org_id", "site_id", "id", "terminator_timestamp",  "hostname", "firmware_version", "model",
                          "cpu_total_time", "cpu_user", "cpu_system",
                          *[F.col('radios')[i].alias(f'r{i}') for i in range(3)]
                          ) \
        .withColumn("date_hour", F.from_unixtime(F.col('terminator_timestamp')/1_000_000, format='yyyy-MM-dd HH:mm:ss')) \
        .select('*', F.col("r2.re_init").alias("r2_re_init"),
                F.col("r0.interrupt_stats_tx_bcn_succ").alias("r0_interrupt_stats_tx_bcn_succ"),
                F.col("r1.interrupt_stats_tx_bcn_succ").alias("r1_interrupt_stats_tx_bcn_succ")
                )
    return df_radios


def get_df_name(fs):
    """
      Get Org and site name
    :param fs:
    :return:
    """
    df_org = spark.read.parquet("{fs}://mist-secorapp-production/dimension/org".format(fs=fs)) \
        .select(F.col("id").alias("OrgID"),F.col("name").alias("org_name"))
    df_name = spark.read.parquet("{fs}://mist-secorapp-production/dimension/site/site.parquet".format(fs=fs)) \
        .select(F.col("id").alias("SiteID"),F.col("name").alias("site_name"),F.col("org_id").alias("OrgID")).join(df_org,["OrgID"]) \
        .select("org_name","site_name","OrgID","SiteID")

    t_org_1 = df_name.select(['OrgID','org_name']).withColumnRenamed('OrgID', 'org_id').dropDuplicates()
    t_org_2 = df_name.select(['SiteID','site_name']).withColumnRenamed('SiteID', 'site_id').dropDuplicates()
    return df_name


