
def get_correlation_matrix_from_df(df_input, input_cols=[], vector_col = "corr_features"):
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
    matrix = Correlation.corr(myGraph_vector, vector_col)
    matrix = Correlation.corr(myGraph_vector, vector_col).collect()[0][0]
    corrmatrix = matrix.toArray().tolist()

    print(corrmatrix)
    df_corr = spark.createDataFrame(corrmatrix,input_cols)
    df_corr.show()
    return df_corr
