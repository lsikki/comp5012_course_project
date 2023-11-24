from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

def my_udf_function(value):
    return len(str(value))

my_udf = udf(my_udf_function, IntegerType())