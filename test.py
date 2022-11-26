from timeit import default_timer as timer
from pyspark import SparkContext
import random

N = 100_000_000_000
start_time = timer()
def sample(p):
    x, y = random.random(), random.random()
    return 1 if x*x + y*y < 1 else 0

sc = SparkContext("local[*]", "Test App")
count = sc.parallelize(range(0, N)).map(sample).reduce(lambda a, b: a + b)
end_time = timer()
result = end_time-start_time
print(f"***************************** Time is: {result}*****************************************")
print("Pi is roughly %f" % (4.0 * count / N))