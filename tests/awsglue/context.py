class _SparkSession:
    pass


class GlueContext:
    def __init__(self, sc=None):
        self.spark_session = _SparkSession()
