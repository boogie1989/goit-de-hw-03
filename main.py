from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round

spark = SparkSession.builder.appName("MyGoitSparkSandbox").getOrCreate()

products_df = spark.read.csv("./data/products.csv", header=True)
products_df.show(10)
purchases_df = spark.read.csv("./data/purchases.csv", header=True)
purchases_df.show(10)
users_df = spark.read.csv("./data/users.csv", header=True)
users_df.show(10)

products_df.describe().show()
purchases_df.describe().show()
users_df.describe().show()

products_df = products_df.dropna()
purchases_df = purchases_df.dropna()
users_df = users_df.dropna()

products_df.describe().show()
purchases_df.describe().show()
users_df.describe().show()

purch_prod_df = (purchases_df
                 .join(products_df, on="product_id", how="inner")
                 .withColumn("total_price", round(col("quantity") * col("price"), 2)))
purch_prod_df.show(10)

categories_sum = purch_prod_df.groupBy("category").sum("total_price")
categories_sum_rnd = (categories_sum
                      .withColumn("rounded_sum", round(categories_sum["sum(total_price)"], 2))
                      .drop("sum(total_price)"))
categories_sum_rnd.show()

all_df = purch_prod_df.join(users_df, on="user_id", how="inner")
all_df.show(10)

age_limit_category_sum = (all_df
                          .filter((col("age") >= 18) & (col("age") <= 25))
                          .groupBy("category")
                          .sum("total_price"))
age_limit_category_sum_rnd = (age_limit_category_sum
                              .withColumn("rounded_sum_18_25", round(age_limit_category_sum["sum(total_price)"], 2))
                              .drop("sum(total_price)"))
age_limit_category_sum_rnd.show()

age_limit_sum = age_limit_category_sum_rnd.groupBy().sum().collect()[0][0]
percents_sales = (age_limit_category_sum_rnd
                  .withColumn("percents", round((col("rounded_sum_18_25") / age_limit_sum) * 100, 2)))
percents_sales.show()

top_3_categories = percents_sales.orderBy(col("percents").desc()).limit(3)
top_3_categories.show()

spark.stop()
