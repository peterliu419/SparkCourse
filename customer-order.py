from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("SpendByCustomer")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customer_id = int(fields[0])
    amount = float(fields[2])
    return (customer_id, amount)

line = sc.textFile("/Users/peter/SparkCourse/customer-orders.csv")
customer = line.map(parseLine).reduceByKey(lambda x, y: x + y)
sorted_customer = customer.map(lambda x: (round(x[1],2),x[0])).sortByKey()
results = sorted_customer.collect()

for result in results:
    print(result)