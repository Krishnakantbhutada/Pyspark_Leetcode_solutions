# Databricks notebook source
# DBTITLE 1,Imports
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select questions

# COMMAND ----------

# DBTITLE 1,Recyclable and Low Fat Products
data = [['0', 'Y', 'N'], ['1', 'Y', 'Y'], ['2', 'N', 'Y'], ['3', 'Y', 'Y'], ['4', 'N', 'N']]
products = pd.DataFrame(data, columns=['product_id', 'low_fats', 'recyclable']).astype({'product_id':'int64', 'low_fats':'category', 'recyclable':'category'})

products = spark.createDataFrame(products)

products_1 = products.filter((products['low_fats'] == 'Y') & (products['recyclable'] == 'Y')).select('product_id')


products_1.display()

# COMMAND ----------

# DBTITLE 1,Find Customer Referee
import pandas as pd


data = [[1, 'Will', None], [2, 'Jane', None], [3, 'Alex', 2], [4, 'Bill', None], [5, 'Zack', 1], [6, 'Mark', 2]]
customer = pd.DataFrame(data, columns=['id', 'name', 'referee_id']).astype({'id':'Int64', 'name':'object', 'referee_id':'Int64'})

customer = spark.createDataFrame(customer)
customer.display()
customer.filter((customer['referee_id'] != 2) | (customer['referee_id'].isNull() == True)).select('name').display()

# COMMAND ----------

# DBTITLE 1,Big Countries
data = [['Afghanistan', 'Asia', 652230, 25500100, 20343000000], ['Albania', 'duropd', 28748, 2831741, 12960000000], ['Algdria', 'Africa', 2381741, 37100000, 188681000000], ['Andorra', 'duropd', 468, 78115, 3712000000], ['Angola', 'Africa', 1246700, 20609294, 100990000000]]

world = pd.DataFrame(data, columns=['name', 'continent', 'area', 'population', 'gdp']).astype({'name':'object', 'continent':'object', 'area':'Int64', 'population':'Int64', 'gdp':'Int64'})

spark_df = spark.createDataFrame(world)

spark_df_1 = spark_df[(spark_df['area']>= 3000000 ) | (spark_df['population']>=25000000)]

spark_df_2 = spark_df_1[['name', 'area', 'population']].dropDuplicates()

spark_df_2.display()

# COMMAND ----------

# DBTITLE 1,Article Views 1
data = [[1, 3, 5, '2019-08-01'], [1, 3, 6, '2019-08-02'], [2, 7, 7, '2019-08-01'], [2, 7, 6, '2019-08-02'], [4, 7, 1, '2019-07-22'], [3, 4, 4, '2019-07-21'], [3, 4, 4, '2019-07-21']]
views = pd.DataFrame(data, columns=['article_id', 'author_id', 'viewer_id', 'view_date']).astype({'article_id':'Int64', 'author_id':'Int64', 'viewer_id':'Int64', 'view_date':'datetime64[ns]'})

views1 = spark.createDataFrame(views)

ans = views1[(views1['author_id'] == views1['viewer_id'])][['author_id']].dropDuplicates().orderBy('author_id')

ans1 = ans.select(col('author_id').alias('id'))

ans1.display()

# COMMAND ----------

# DBTITLE 1,Invalid Tweets
data = [[1, 'Vote for Biden'], [2, 'Let us make America great again!']]
tweets = pd.DataFrame(data, columns=['tweet_id', 'content']).astype({'tweet_id':'Int64', 'content':'object'})

tw = spark.createDataFrame(tweets)

ans = tw.filter(length(tw['content']) > 15).select(tw['tweet_id'])
ans.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### # Basic Joins

# COMMAND ----------

# DBTITLE 1,Replace Employee ID With The Unique Identifier
data = [[1, 'Alice'], [7, 'Bob'], [11, 'Meir'], [90, 'Winston'], [3, 'Jonathan']]
employees = pd.DataFrame(data, columns=['id', 'name']).astype({'id':'int64', 'name':'object'})
data = [[3, 1], [11, 2], [90, 3]]
employee_uni = pd.DataFrame(data, columns=['id', 'unique_id']).astype({'id':'int64', 'unique_id':'int64'})

employees = spark.createDataFrame(employees)
employee_uni = spark.createDataFrame(employee_uni)

employees_1 = employees.alias('a').join(employee_uni.alias('b'), employees['id'] == employee_uni['id'], 'left').select('b.unique_id','a.name')

employees_1.display()




# COMMAND ----------

# DBTITLE 1,Product Sales Analysis I
data = [[1, 100, 2008, 10, 5000], [2, 100, 2009, 12, 5000], [7, 200, 2011, 15, 9000]]
sales = pd.DataFrame(data, columns=['sale_id', 'product_id', 'year', 'quantity', 'price']).astype({'sale_id':'Int64', 'product_id':'Int64', 'year':'Int64', 'quantity':'Int64', 'price':'Int64'})
data = [[100, 'Nokia'], [200, 'Apple'], [300, 'Samsung']]
product = pd.DataFrame(data, columns=['product_id', 'product_name']).astype({'product_id':'Int64', 'product_name':'object'})


sales = spark.createDataFrame(sales)
product = spark.createDataFrame(product)

sales_1 = sales.alias('a').join(product.alias('b'), sales['product_id'] == product['product_id'], 'left').select('b.product_name','a.year','a.price')
sales_1.display()



# COMMAND ----------

# DBTITLE 1,Customer Who Visited but Did Not Make Any Transactions
import pandas as pd
from pyspark.sql import functions as F

data = [[1, 23], [2, 9], [4, 30], [5, 54], [6, 96], [7, 54], [8, 54]]
visits = pd.DataFrame(data, columns=['visit_id', 'customer_id']).astype({'visit_id':'Int64', 'customer_id':'Int64'})

visits = spark.createDataFrame(visits)

data = [[2, 5, 310], [3, 5, 300], [9, 5, 200], [12, 1, 910], [13, 2, 970]]
transactions = pd.DataFrame(data, columns=['transaction_id', 'visit_id', 'amount']).astype({'transaction_id':'Int64', 'visit_id':'Int64', 'amount':'Int64'})

transactions = spark.createDataFrame(transactions)

visits_1 = visits.alias('a').join(transactions.alias('b'), visits['visit_id'] == transactions['visit_id'], 'left').select('a.*', 'b.transaction_id')
visits_2 = visits_1.filter(visits_1['transaction_id'].isNull()).select('customer_id')
visits_3 = visits_2.groupBy('customer_id').count()
visits_3 = visits_3.orderBy(F.desc('count'))
visits_3.display()





# COMMAND ----------

# DBTITLE 1,Rising Temperature
import pandas as pd
from pyspark.sql import functions as F

data = [[1, '2015-01-01', 10], [2, '2015-01-02', 25], [3, '2015-01-03', 20], [4, '2015-01-04', 30]]
weather = pd.DataFrame(data, columns=['id', 'recordDate', 'temperature']).astype({'id':'Int64', 'recordDate':'datetime64[ns]', 'temperature':'Int64'})

weather = spark.createDataFrame(weather)

weather_1 = weather.alias('a').join(weather.alias('b'))

weather_2 = weather_1.filter((weather_1["a.id"] != weather_1["b.id"]) & (weather_1['b.recordDate']>weather_1['a.recordDate'] ) )
                             
weather_3 = weather_2.withColumn("date_diff", F.datediff(weather_1['b.recordDate'], weather_1['a.recordDate']))

weather_4 = weather_3.filter((weather_3['date_diff'] == 1) & (weather_2['b.temperature'] > weather_2['a.temperature'] ))
                         

weather_4.display()


# COMMAND ----------

# DBTITLE 1,Average Time of Process per Machine
import pandas as pd
from  pyspark.sql import functions as F

data = [[0, 0, 'start', 0.712], [0, 0, 'end', 1.52], [0, 1, 'start', 3.14], [0, 1, 'end', 4.12], [1, 0, 'start', 0.55], [1, 0, 'end', 1.55], [1, 1, 'start', 0.43], [1, 1, 'end', 1.42], [2, 0, 'start', 4.1], [2, 0, 'end', 4.512], [2, 1, 'start', 2.5], [2, 1, 'end', 5]]
activity = pd.DataFrame(data, columns=['machine_id', 'process_id', 'activity_type', 'timestamp']).astype({'machine_id':'Int64', 'process_id':'Int64', 'activity_type':'object', 'timestamp':'Float64'})
activity = spark.createDataFrame(activity)

a = activity.alias('a')
b = activity.alias('b')


activity_1 = a.join(b, ((a['a.machine_id'] == b['b.machine_id']) & (a['a.process_id'] == b['b.process_id'])), 'left')
activity_2 = activity_1.filter((activity_1['a.activity_type'] != activity_1['b.activity_type']) & (activity_1['b.timestamp'] >= activity_1['a.timestamp']))
activity_3 = activity_2.withColumn('time_diff', (activity_2['b.timestamp'] - activity_2['a.timestamp']))
activity_4 = activity_3.groupBy(activity_3['a.machine_id']).agg(F.avg('time_diff'))
activity_4.display()

# COMMAND ----------

# DBTITLE 1,Employee Bonus
import pandas as pd

data = [[3, 'Brad', None, 4000], [1, 'John', 3, 1000], [2, 'Dan', 3, 2000], [4, 'Thomas', 3, 4000]]
employee = pd.DataFrame(data, columns=['empId', 'name', 'supervisor', 'salary']).astype({'empId':'Int64', 'name':'object', 'supervisor':'Int64', 'salary':'Int64'})
data = [[2, 500], [4, 2000]]
bonus = pd.DataFrame(data, columns=['empId', 'bonus']).astype({'empId':'Int64', 'bonus':'Int64'})

em = spark.createDataFrame(employee)
bns = spark.createDataFrame(bonus)

em = em.join(bns, on = 'empId', how = 'left')
em = em.filter((em['bonus']<=1000) | (em['bonus'].isNull()))
em.display()

# COMMAND ----------

# DBTITLE 1,Students and Examinations
from pyspark.sql import functions as F

data = [[1, 'Alice'], [2, 'Bob'], [13, 'John'], [6, 'Alex']]
students = pd.DataFrame(data, columns=['student_id', 'student_name']).astype({'student_id':'Int64', 'student_name':'object'})
data = [['Math'], ['Physics'], ['Programming']]
subjects = pd.DataFrame(data, columns=['subject_name']).astype({'subject_name':'object'})
data = [[1, 'Math'], [1, 'Physics'], [1, 'Programming'], [2, 'Programming'], [1, 'Physics'], [1, 'Math'], [13, 'Math'], [13, 'Programming'], [13, 'Physics'], [2, 'Math'], [1, 'Math']]
examinations = pd.DataFrame(data, columns=['student_id', 'subject_name']).astype({'student_id':'Int64', 'subject_name':'object'})

students = spark.createDataFrame(students)
subjects = spark.createDataFrame(subjects)
exa = spark.createDataFrame(examinations)

exa_1 = exa.groupBy(exa['student_id'],exa['subject_name']).count()
exa_2 = exa_1.alias('a').join(students.alias('b'),exa_1['student_id'] == students['student_id'], 'left').select('a.*', 'b.student_name')

students_1 = students.join(subjects)
students_2 = students_1.alias('a').join(exa_2.alias('b'), (students_1['student_id'] == exa_2['student_id']) & (students_1['subject_name'] == exa_2['subject_name']),'left').select('a.*', 'b.count')

students_3 = students_2.withColumn('new_count', F.when((students_2['count'].isNull()), 0).otherwise(students_2['count'])).select(students_2['student_id'], students_2['student_name'], students_2['subject_name'],'new_count')
students_3.display()

# COMMAND ----------

# DBTITLE 1,Managers with at Least 5 Direct Reports
data = [[101, 'John', 'A', None], [102, 'Dan', 'A', 101], [103, 'James', 'A', 101], [104, 'Amy', 'A', 101], [105, 'Anne', 'A', 101], [106, 'Ron', 'B', 101]]
employee = pd.DataFrame(data, columns=['id', 'name', 'department', 'managerId']).astype({'id':'Int64', 'name':'object', 'department':'object', 'managerId':'Int64'})

employee = spark.createDataFrame(employee)

manager_df = employee.filter(employee['managerId'].isNotNull()).groupBy('managerId').agg(F.count('id').alias('count')).withColumnRenamed('managerId','id')
employee_ans = employee.join(manager_df, on = 'id', how = 'inner').select('name')
employee_ans.display()



# COMMAND ----------

# DBTITLE 1,Confirmation Rate
import pandas as pd
from pyspark.sql import functions as F
data = [[3, '2020-03-21 10:16:13'], [7, '2020-01-04 13:57:59'], [2, '2020-07-29 23:09:44'], [6, '2020-12-09 10:39:37']]
signups = pd.DataFrame(data, columns=['user_id', 'time_stamp']).astype({'user_id':'Int64', 'time_stamp':'datetime64[ns]'})
data = [[3, '2021-01-06 03:30:46', 'timeout'], [3, '2021-07-14 14:00:00', 'timeout'], [7, '2021-06-12 11:57:29', 'confirmed'], [7, '2021-06-13 12:58:28', 'confirmed'], [7, '2021-06-14 13:59:27', 'confirmed'], [2, '2021-01-22 00:00:00', 'confirmed'], [2, '2021-02-28 23:59:59', 'timeout']]
confirmations = pd.DataFrame(data, columns=['user_id', 'time_stamp', 'action']).astype({'user_id':'Int64', 'time_stamp':'datetime64[ns]', 'action':'object'})

sig = spark.createDataFrame(signups)

sig_o = sig
cnf = spark.createDataFrame(confirmations)

sig = sig.join(cnf, on = 'user_id', how = 'left')
sig = sig.select(sig['user_id'],sig['action'])

sig_cnf  = sig.filter(sig['action'] == 'confirmed')
sig_cnf = sig_cnf.groupBy('user_id','action').count()
sig_cnf = sig_cnf.withColumn('sig_cnf', sig_cnf['count'])

sig_tim  = sig.filter(sig['action'] == 'timeout')
sig_tim = sig_tim.groupBy('user_id','action').count()
sig_tim = sig_tim.withColumn('sig_tim', sig_tim['count'])

sig_1 = sig_o.join(sig_cnf, on = 'user_id', how = 'left')
sig_1 = sig_1.join(sig_tim, on = 'user_id', how = 'left')
sig_1 = sig_1.select('user_id','sig_cnf','sig_tim')
sig_1 = sig_1.withColumn('act_f', F.when(sig_1['sig_cnf'].isNull(), sig_1['sig_tim'])\
                                   .when(sig_1['sig_cnf'].isNotNull() & sig_1['sig_tim'].isNotNull(), sig_1['sig_tim']+sig_1['sig_cnf'])
                                   .otherwise(sig_1['sig_cnf']))
# sig_1 = sig_1.select('user_id','act_f')
sig_1 = sig_1.withColumn('cnf%', sig_1['sig_cnf']/sig_1['act_f'])
sig_1 = sig_1.withColumn('ans', F.when(sig_1['cnf%'].isNull(), 0.00).otherwise(F.round(sig_1['cnf%'],2)))

sig_1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Basic Aggregate Functions

# COMMAND ----------

# DBTITLE 1,Not Boring Movies
data = [[1, 'War', 'great 3D', 8.9], [2, 'Science', 'fiction', 8.5], [3, 'irish', 'boring', 6.2], [4, 'Ice song', 'Fantacy', 8.6], [5, 'House card', 'Interesting', 9.1]]
cinema = pd.DataFrame(data, columns=['id', 'movie', 'description', 'rating']).astype({'id':'Int64', 'movie':'object', 'description':'object', 'rating':'Float64'})

cinema = spark.createDataFrame(cinema)

cinema_1 = cinema.filter((cinema['id']%2 != 0) & (cinema['description'] != 'boring'))
cinema_1.display()

# COMMAND ----------

# DBTITLE 1,Average Selling Price
import pandas as pd
from pyspark.sql import functions as F

data = [[1, '2019-02-17', '2019-02-28', 5], [1, '2019-03-01', '2019-03-22', 20], [2, '2019-02-01', '2019-02-20', 15], [2, '2019-02-21', '2019-03-31', 30]]
prices = pd.DataFrame(data, columns=['product_id', 'start_date', 'end_date', 'price']).astype({'product_id':'Int64', 'start_date':'datetime64[ns]', 'end_date':'datetime64[ns]', 'price':'Int64'})
data = [[1, '2019-02-25', 100], [1, '2019-03-01', 15], [2, '2019-02-10', 200], [2, '2019-03-22', 30]]
units_sold = pd.DataFrame(data, columns=['product_id', 'purchase_date', 'units']).astype({'product_id':'Int64', 'purchase_date':'datetime64[ns]', 'units':'Int64'})

pr = spark.createDataFrame(prices)
us = spark.createDataFrame(units_sold)

us_1 = us.join(pr, on = 'product_id',how = 'inner')

us_2 = us_1.filter(us_1['purchase_date'].between(us_1['start_date'],us_1['end_date'])).withColumn('amount',us_1['price']*us_1['units'] )
us_3 = us_2.groupBy('product_id').agg(F.sum('amount'), F.sum('units'))
us_4 = us_3.withColumn('aavgg',F.round(us_3['sum(amount)']/us_3['sum(units)'],2))
us_4.display()

# COMMAND ----------

# DBTITLE 1,Project Employees I
data = [[1, 1], [1, 2], [1, 3], [2, 1], [2, 4]]
project = pd.DataFrame(data, columns=['project_id', 'employee_id']).astype({'project_id':'Int64', 'employee_id':'Int64'})
data = [[1, 'Khaled', 3], [2, 'Ali', 2], [3, 'John', 1], [4, 'Doe', 2]]
employee = pd.DataFrame(data, columns=['employee_id', 'name', 'experience_years']).astype({'employee_id':'Int64', 'name':'object', 'experience_years':'Int64'})

project = spark.createDataFrame(project)
employee = spark.createDataFrame(employee)

project_1 = project.join(employee, project['employee_id'] == employee['employee_id'], 'left').select('project_id', 'experience_years')

project_2 = project_1.groupBy(project_1['project_id']).agg(F.avg(project_1['experience_years']))

project_2.display()


# COMMAND ----------

# DBTITLE 1,Percentage of Users Attended a Contest
import pandas as pd
from pyspark.sql import functions as F

data = [[6, 'Alice'], [2, 'Bob'], [7, 'Alex']]
users = pd.DataFrame(data, columns=['user_id', 'user_name']).astype({'user_id':'Int64', 'user_name':'object'})
data = [[215, 6], [209, 2], [208, 2], [210, 6], [208, 6], [209, 7], [209, 6], [215, 7], [208, 7], [210, 2], [207, 2], [210, 7]]
register = pd.DataFrame(data, columns=['contest_id', 'user_id']).astype({'contest_id':'Int64', 'user_id':'Int64'})

users = spark.createDataFrame(users)
register = spark.createDataFrame(register)

temp = users.count()
reg_1 = register.groupBy('contest_id').count()

reg_2 = reg_1.withColumn('reg_per', F.round((reg_1['count']*100)/temp)).orderBy('contest_id').orderBy('reg_per', ascending  = False)



reg_2.display()


# COMMAND ----------

# DBTITLE 1,Queries Quality and Percentage
data = [['Dog', 'Golden Retriever', 1, 5], ['Dog', 'German Shepherd', 2, 5], ['Dog', 'Mule', 200, 1], ['Cat', 'Shirazi', 5, 2], ['Cat', 'Siamese', 3, 3], ['Cat', 'Sphynx', 7, 4]]
queries = pd.DataFrame(data, columns=['query_name', 'result', 'position', 'rating']).astype({'query_name':'object', 'result':'object', 'position':'Int64', 'rating':'Int64'})

q = spark.createDataFrame(queries)
q = q.withColumn('pos', q['rating']/q['position']).withColumn('flag',F.when(q['rating'] < 3,1).otherwise(0))
q_1 = q.groupBy('query_name').agg(
  F.round(F.avg('pos'),2),
  F.round(F.avg('flag')*100,2),
  )

q_1.display()


# COMMAND ----------

# DBTITLE 1,Monthly Transactions I
data = [[121, 'US', 'approved', 1000, '2018-12-18'], [122, 'US', 'declined', 2000, '2018-12-19'], [123, 'US', 'approved', 2000, '2019-01-01'], [124, 'DE', 'approved', 2000, '2019-01-07']]
transactions = pd.DataFrame(data, columns=['id', 'country', 'state', 'amount', 'trans_date']).astype({'id':'Int64', 'country':'object', 'state':'object', 'amount':'Int64', 'trans_date':'datetime64[ns]'})

txn = spark.createDataFrame(transactions)
txn_1 = txn.withColumn('txn_date', F.date_format(txn['trans_date'], 'yyyy-MM')).select('txn_date','country','state','amount')

txn_2a = txn_1.filter(txn_1['state'] == 'approved') 
txn_2a_1 = txn_2a.groupBy('txn_date', 'country').agg(F.count('*'), F.sum('amount'))


txn_2 = txn_1.groupBy('txn_date', 'country').agg(F.count('*'), F.sum('amount'))

txn_3 = txn_2.join(txn_2a_1, ((txn_2['txn_date'] == txn_2a_1['txn_date']) &(txn_2['country'] == txn_2a_1['country'])), 'left')

txn_3.display()

# COMMAND ----------

# DBTITLE 1,Immediate Food Delivery II
import pandas as pd
from pyspark.sql.window import Window
from pyspark.sql import functions as F

data = [[1, 1, '2019-08-01', '2019-08-02'], [2, 2, '2019-08-02', '2019-08-02'], [3, 1, '2019-08-11', '2019-08-12'], [4, 3, '2019-08-24', '2019-08-24'], [5, 3, '2019-08-21', '2019-08-22'], [6, 2, '2019-08-11', '2019-08-13'], [7, 4, '2019-08-09', '2019-08-09']]
delivery = pd.DataFrame(data, columns=['delivery_id', 'customer_id', 'order_date', 'customer_pref_delivery_date']).astype({'delivery_id':'Int64', 'customer_id':'Int64', 'order_date':'datetime64[ns]', 'customer_pref_delivery_date':'datetime64[ns]'})

delh = spark.createDataFrame(delivery)

window_spec = Window.partitionBy('customer_id').orderBy('order_date')

delh = delh.withColumn('rnk', F.row_number().over(window_spec))
delh = delh.withColumn('ans',F.when((delh['rnk'] == 1) & (delh['order_date'] == delh['customer_pref_delivery_date']), 1).otherwise(0))

delh_ans = delh.groupBy('customer_id').agg(F.max('ans').alias('delivery_status'))
ans = delh_ans.select(F.sum('delivery_status')/F.count('delivery_status'))

delh.display()
delh_ans.display()
ans.display()

# COMMAND ----------

# DBTITLE 1,Game Play Analysis IV
data = [[1, 2, '2016-03-01', 5], [1, 2, '2016-03-02', 6], [2, 3, '2017-06-25', 1], [3, 1, '2016-03-02', 0], [3, 4, '2018-07-03', 5]]
activity = pd.DataFrame(data, columns=['player_id', 'device_id', 'event_date', 'games_played']).astype({'player_id':'Int64', 'device_id':'Int64', 'event_date':'datetime64[ns]', 'games_played':'Int64'})

activity = spark.createDataFrame(activity)
window_spec = Window.partitionBy('player_id').orderBy('event_date')

activity_1 = activity.withColumn('rnk', F.row_number().over(window_spec))
activity_1 = activity_1.filter(activity_1['rnk'] <= 2)
activity_1 = activity_1.select('player_id','event_date')

activity_1_a = activity_1.withColumnRenamed('event_date', 'event_date_a')
activity_1_b = activity_1.withColumnRenamed('event_date', 'event_date_b')

activity_1 = activity_1_a.join(activity_1_b, on='player_id', how='inner')
activity_1 = activity_1.filter(F.datediff('event_date_a','event_date_b') == 1)

distinct_player_id = activity.select('player_id').dropDuplicates().count()

total_player_id = activity_1.count()

print(total_player_id/distinct_player_id)

# activity_1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sorting and Grouping

# COMMAND ----------

# DBTITLE 1,Number of Unique Subjects Taught by Each Teacher
import pandas as pd
data = [[1, 2, 3], [1, 2, 4], [1, 3, 3], [2, 1, 1], [2, 2, 1], [2, 3, 1], [2, 4, 1]]
teacher = pd.DataFrame(data, columns=['teacher_id', 'subject_id', 'dept_id']).astype({'teacher_id':'Int64', 'subject_id':'Int64', 'dept_id':'Int64'})

tech = spark.createDataFrame(teacher)

tech_1 = tech.select('teacher_id', 'subject_id').distinct()
tech_2 = tech_1.groupBy('teacher_id').count()

tech_2.display()

# COMMAND ----------

# DBTITLE 1,User Activity for the Past 30 Days I
import pandas as pd

data = [[1, 1, '2019-07-20', 'open_session'], [1, 1, '2019-07-20', 'scroll_down'], [1, 1, '2019-07-20', 'end_session'], [2, 4, '2019-07-20', 'open_session'], [2, 4, '2019-07-21', 'send_message'], [2, 4, '2019-07-21', 'end_session'], [3, 2, '2019-07-21', 'open_session'], [3, 2, '2019-07-21', 'send_message'], [3, 2, '2019-07-21', 'end_session'], [4, 3, '2019-06-25', 'open_session'], [4, 3, '2019-06-25', 'end_session']]
activity = pd.DataFrame(data, columns=['user_id', 'session_id', 'activity_date', 'activity_type']).astype({'user_id':'Int64', 'session_id':'Int64', 'activity_date':'datetime64[ns]', 'activity_type':'object'})

activity = spark.createDataFrame(activity)

activity_2 = activity.withColumn('date_diff', F.datediff(F.lit('2019-07-27'),activity['activity_date']))

activity_3 = activity_2.filter(activity_2['date_diff'] < 30 ).select(activity_2['user_id'], activity_2['activity_date']).distinct()
activity_4 = activity_3.groupBy(activity_3['activity_date']).count()

activity_4.display()

# COMMAND ----------

# DBTITLE 1,Product Sales Analysis III
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


data = [[1, 100, 2008, 10, 5000], [2, 100, 2009, 12, 5000], [7, 200, 2011, 15, 9000]]
sales = pd.DataFrame(data, columns=['sale_id', 'product_id', 'year', 'quantity', 'price']).astype({'sale_id':'Int64', 'product_id':'Int64', 'year':'Int64', 'quantity':'Int64', 'price':'Int64'})
data = [[100, 'Nokia'], [200, 'Apple'], [300, 'Samsung']]
product = pd.DataFrame(data, columns=['product_id', 'product_name']).astype({'product_id':'Int64', 'product_name':'object'})

sales = spark.createDataFrame(sales)

window_spec = Window.partitionBy('product_id').orderBy('year')

sales_1 = sales.withColumn('rnk', row_number().over(window_spec)).filter('rnk == 1')

sales_1.display()


# COMMAND ----------

# DBTITLE 1,Classes More Than 5 Students
data = [['A', 'Math'], ['B', 'English'], ['C', 'Math'], ['D', 'Biology'], ['E', 'Math'], ['F', 'Computer'], ['G', 'Math'], ['H', 'Math'], ['I', 'Math']]
c = pd.DataFrame(data, columns=['student', 'class']).astype({'student':'object', 'class':'object'})

c_1 = spark.createDataFrame(c)

c_2 = c_1.select('class')
c_3 = c_2.groupBy('class').count()
c_4 = c_3.filter(c_3['count'] >= 5).select('class')
c_4.display()

# COMMAND ----------

# DBTITLE 1,Find Follower Count
data = [['0', '1'], ['1', '0'], ['2', '0'], ['2', '1']]
followers = pd.DataFrame(data, columns=['user_id', 'follower_id']).astype({'user_id':'Int64', 'follower_id':'Int64'})

followers = spark.createDataFrame(followers)
followers = followers.groupBy('user_id').agg(F.count('follower_id')).orderBy('user_id')
followers.display()

# COMMAND ----------

# DBTITLE 1,Biggest Single Number
data = [[8], [8], [3], [3], [1], [4], [5], [6]]
my_numbers = pd.DataFrame(data, columns=['num']).astype({'num':'Int64'})

my_numbers = spark.createDataFrame(my_numbers)
my_numbers = my_numbers.groupBy('num').agg(F.count('num').alias('freq'))
my_numbers = my_numbers.filter(my_numbers['freq'] == 1)
my_numbers = my_numbers.agg(F.max(my_numbers['num']))

my_numbers.display()


# COMMAND ----------

# DBTITLE 1,Customers Who Bought All Products
data = [[1, 5], [2, 6], [3, 5], [3, 6], [1, 6]]
customer = pd.DataFrame(data, columns=['customer_id', 'product_key']).astype({'customer_id':'Int64', 'product_key':'Int64'})
data = [[5], [6]]
product = pd.DataFrame(data, columns=['product_key']).astype({'product_key':'Int64'})

cust =  spark.createDataFrame(customer)
product =  spark.createDataFrame(product)

window_spec = Window.partitionBy('customer_id','product_key').orderBy('product_key')

cust = cust.withColumn('rnk', F.row_number().over(window_spec)).filter('rnk == 1')
cust_1 = cust.groupBy('customer_id').agg(F.count('product_key').alias('cnt'))
pr_cnt = product.dropDuplicates().count()
cust_2 = cust_1.filter(cust_1['cnt'] == pr_cnt)
cust_2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Advanced Select and Joins

# COMMAND ----------

# DBTITLE 1,The Number of Employees Which Report to Each Employee
data = [[9, 'Hercy', None, 43], [6, 'Alice', 9, 41], [4, 'Bob', 9, 36], [2, 'Winston', None, 37]]
employees = pd.DataFrame(data, columns=['employee_id', 'name', 'reports_to', 'age']).astype({'employee_id':'Int64', 'name':'object', 'reports_to':'Int64', 'age':'Int64'})

emp = spark.createDataFrame(employees)

emp_1 = emp.filter(emp['reports_to'].isNotNull())
emp_1 = emp_1.groupBy(emp_1['reports_to']).agg(F.count(emp_1['name']).alias('reports_count'),F.round(F.avg(emp_1['age']),0).alias('average_age'))

emp_2 = emp_1.join(emp, emp['employee_id'] == emp_1['reports_to'], 'left')
emp_2 = emp_2.select('employee_id','name','reports_count','average_age')
emp_2.display()



# COMMAND ----------

# DBTITLE 1,Primary Department for Each Employee
data = [['1', '1', 'N'], ['2', '1', 'Y'], ['2', '2', 'N'], ['3', '3', 'N'], ['4', '2', 'N'], ['4', '3', 'Y'], ['4', '4', 'N']]
employee = pd.DataFrame(data, columns=['employee_id', 'department_id', 'primary_flag']).astype({'employee_id':'Int64', 'department_id':'Int64', 'primary_flag':'object'})

emp = spark.createDataFrame(employee)

window_spec = Window.partitionBy('employee_id').orderBy(emp['primary_flag'].desc())

emp_1 = emp.withColumn('rnk', F.row_number().over(window_spec)).filter('rnk == 1').select('employee_id','department_id')
emp_1.display()


# COMMAND ----------

# DBTITLE 1,Triangle Judgement
data = [[13, 15, 30], [10, 20, 15]]
triangle = pd.DataFrame(data, columns=['x', 'y', 'z']).astype({'x':'Int64', 'y':'Int64', 'z':'Int64'})

tri = spark.createDataFrame(triangle)

tri = tri.withColumn('fact_check', when((tri['x']+tri['y'] > tri['z']) & (tri['z']+tri['y'] > tri['x']) & ( tri['x']+tri['z'] > tri['y']) ,'Yes').otherwise('No'))

tri.display()

# COMMAND ----------

# DBTITLE 1,Consecutive Numbers
import pandas as pd

data = [[1, 1], [2, 1], [3, 1], [4, 2], [5, 1], [6, 2], [7, 2]]
logs = pd.DataFrame(data, columns=['id', 'num']).astype({'id':'Int64', 'num':'Int64'})

logs = spark.createDataFrame(logs)

# logs_1 = logs.select('num').collect()

# temp = logs_1[0]['num']
# cnt = 0

# lst = []
# for rows in logs_1:
#   if rows['num'] == temp:
#     cnt+=1
#     if cnt >= 3 and rows['num'] not in lst:
#       lst.append(rows['num'])
#       print(rows['num'])
#   else :
#     cnt = 1
#     temp = rows['num']
  
logs_1 = logs.alias('a').join(logs.alias('b'), on ='num',how = 'left')
logs_1 = logs_1.select('num',logs_1['a.id'].alias('a_id'),logs_1['b.id'].alias('b_id'))
logs_1 = logs_1.alias('a').join(logs.alias('b'), on ='num',how = 'left')
logs_1 = logs_1.select('num','a_id','b_id',logs_1['b.id'].alias('c_id'))
logs_1 = logs_1.filter((logs_1['a_id']+1 == logs_1['b_id'])&(logs_1['b_id']+1 ==logs_1['c_id']))
# logs_1 = logs.alias('c').join(logs.alias('d'), on ='num',how = 'left')

logs.display()
logs_1.display()
logs_1.select(logs_1['num']).distinct().display()

# COMMAND ----------

# DBTITLE 1,Product Price at a Given Date
data = [
    (1, '2019-08-15', 20),
    (1, '2019-08-10', 15),
    (2, '2019-08-16', 25),
    (2, '2019-08-14', 20),
    (3, '2019-08-12', None),
]
columns = ['product_id', 'change_date', 'new_price']

products = spark.createDataFrame(data, schema=columns).withColumn("change_date", F.to_date("change_date"))

window_spec = Window.partitionBy("product_id").orderBy(F.col("change_date").desc())
filtered_products = products.filter(F.col("change_date") <= "2019-08-16")
ranked_products = filtered_products.withColumn("rnk", F.row_number().over(window_spec))
latest_products = ranked_products.filter(F.col("rnk") == 1).select("product_id", "new_price")
final_df = products.select("product_id").distinct() \
    .join(latest_products, on="product_id", how="left") \
    .withColumn("price", F.when(F.col("new_price").isNull(), F.lit(10)).otherwise(F.col("new_price"))) \
    .select("product_id", "price") \
    .orderBy("product_id")

final_df.show()


# COMMAND ----------

# DBTITLE 1,Last Person to Fit in the Bus
data = [[5, 'Alice', 250, 1], [4, 'Bob', 175, 5], [3, 'Alex', 350, 2], [6, 'John Cena', 400, 3], [1, 'Winston', 500, 6], [2, 'Marie', 200, 4]]
queue = pd.DataFrame(data, columns=['person_id', 'person_name', 'weight', 'turn']).astype({'person_id':'Int64', 'person_name':'object', 'weight':'Int64', 'turn':'Int64'})

queue = spark.createDataFrame(queue)

window_spec = Window.orderBy('turn')

queue_1 = queue.withColumn('cumulative_sum', F.sum('weight').over(window_spec)).filter('cumulative_sum <= 1000')
queue_1 = queue_1.orderBy(queue_1['turn'].desc())
queue_1.display()

# COMMAND ----------

# DBTITLE 1,Count Salary Categories
data = [[3, 108939], [2, 12747], [8, 87709], [6, 91796]]
cat = ['Low Salary','Average Salary','High Salary']

accounts = pd.DataFrame(data, columns=['account_id', 'income']).astype({'account_id':'Int64', 'income':'Int64'})
cat = pd.DataFrame(cat,columns = ['cat'])

acc = spark.createDataFrame(accounts)
cat_df = spark.createDataFrame(cat)

acc = acc.withColumn('cat', F.when(acc['income']<20000,'Low Salary')
                             .when(acc['income']<=50000,'Average Salary')
                             .otherwise('High Salary'))

acc = acc.groupBy('cat').agg(F.count('account_id').alias('cnt'))

new_df = cat_df.join(acc, on = 'cat', how = 'left')

acc.display()
new_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Subqueries

# COMMAND ----------

# DBTITLE 1,Employees Whose Manager Left the Company
data = [[3, 'Mila', 9, 60301], [12, 'Antonella', None, 31000], [13, 'Emery', None, 67084], [1, 'Kalel', 11, 21241], [9, 'Mikaela', None, 50937], [11, 'Joziah', 6, 28485]]
employees = pd.DataFrame(data, columns=['employee_id', 'name', 'manager_id', 'salary']).astype({'employee_id':'Int64', 'name':'object', 'manager_id':'Int64', 'salary':'Int64'})

emp = spark.createDataFrame(employees)

emp_1 = emp.filter((emp['salary'] < 30000) & emp['manager_id'].isNotNull()).select('manager_id', 'employee_id')
emp_2 = emp_1.join(emp, emp_1['manager_id'] == emp['employee_id'], 'left')
emp_2.filter(emp_2['name'].isNull()).display()

# COMMAND ----------

# DBTITLE 1,Exchange Seats
data = [[1, 'Abbot'], [2, 'Doris'], [3, 'Emerson'], [4, 'Green'], [5, 'Jeames']]
seat = pd.DataFrame(data, columns=['id', 'student']).astype({'id':'Int64', 'student':'object'})

seat = spark.createDataFrame(seat)

seat = seat.withColumn('new_col', F.round((seat['id']/2),0))

window_spec = Window.partitionBy('new_col').orderBy('id')
seat = seat.withColumn('lead_name', F.lead('student',1).over(window_spec))\
           .withColumn('lag_name', F.lag('student',1).over(window_spec))
seat = seat.withColumn('final_student_name', when(seat['lead_name'].isNotNull(), seat['lead_name'])
                                        .when(seat['lag_name'].isNotNull(), seat['lag_name'])
                                        .otherwise(seat['student']))

seat.display()


# COMMAND ----------

# DBTITLE 1,Movie Rating
data = [[1, 'Avengers'], [2, 'Frozen 2'], [3, 'Joker']]
movies = pd.DataFrame(data, columns=['movie_id', 'title']).astype({'movie_id':'Int64', 'title':'object'})
data = [[1, 'Daniel'], [2, 'Monica'], [3, 'Maria'], [4, 'James']]
users = pd.DataFrame(data, columns=['user_id', 'name']).astype({'user_id':'Int64', 'name':'object'})
data = [[1, 1, 3, '2020-01-12'], [1, 2, 4, '2020-02-11'], [1, 3, 2, '2020-02-12'], [1, 4, 1, '2020-01-01'], [2, 1, 5, '2020-02-17'], [2, 2, 2, '2020-02-01'], [2, 3, 2, '2020-03-01'], [3, 1, 3, '2020-02-22'], [3, 2, 4, '2020-02-25']]
movie_rating = pd.DataFrame(data, columns=['movie_id', 'user_id', 'rating', 'created_at']).astype({'movie_id':'Int64', 'user_id':'Int64', 'rating':'Int64', 'created_at':'datetime64[ns]'})

movies = spark.createDataFrame(movies)
users = spark.createDataFrame(users)
movie_rating_o = spark.createDataFrame(movie_rating)

movie_rating = movie_rating_o.groupBy('user_id').agg(F.count('movie_id').alias('cnt'))
movie_rating = movie_rating.join(users, on = 'user_id', how = 'left')
movie_rating = movie_rating.orderBy(movie_rating['cnt'].desc()).limit(1)
movie_rating.display()

movie_rating_n = movie_rating_o.filter((movie_rating_o['created_at']>="2020-02-01") & (movie_rating_o['created_at']<="2020-02-29"))
movie_rating_n = movie_rating_n.groupBy('movie_id').agg(F.avg('rating').alias('avg_rating'))
movie_rating_n = movie_rating_n.join(movies, on  = 'movie_id', how = 'left')
movie_rating_n = movie_rating_n.orderBy(movie_rating_n['avg_rating'].desc(),movie_rating_n['title'])
movie_rating_n.limit(1).display()


# COMMAND ----------

# DBTITLE 1,Restaurant Growth
data = [[1, 'Jhon', '2019-01-01', 100], [2, 'Daniel', '2019-01-02', 110], [3, 'Jade', '2019-01-03', 120], [4, 'Khaled', '2019-01-04', 130], [5, 'Winston', '2019-01-05', 110], [6, 'Elvis', '2019-01-06', 140], [7, 'Anna', '2019-01-07', 150], [8, 'Maria', '2019-01-08', 80], [9, 'Jaze', '2019-01-09', 110], [1, 'Jhon', '2019-01-10', 130], [3, 'Jade', '2019-01-10', 150]]
customer = pd.DataFrame(data, columns=['customer_id', 'name', 'visited_on', 'amount']).astype({'customer_id':'Int64', 'name':'object', 'visited_on':'datetime64[ns]', 'amount':'Int64'})

cust = spark.createDataFrame(customer)

cust = cust.groupBy('visited_on').agg(F.sum('amount').alias('sales_sum'))
# cust = cust_n.join(cust, on = 'visited_on', how = 'left')

window_spec = Window.orderBy('visited_on')

cust = cust.withColumn('last_1_day_sales', F.lag('sales_sum',1).over(window_spec))\
           .withColumn('last_2_day_sales', F.lag('sales_sum',2).over(window_spec))\
           .withColumn('last_3_day_sales', F.lag('sales_sum',3).over(window_spec))\
           .withColumn('last_4_day_sales', F.lag('sales_sum',4).over(window_spec))\
           .withColumn('last_5_day_sales', F.lag('sales_sum',5).over(window_spec))\
           .withColumn('last_6_day_sales', F.lag('sales_sum',6).over(window_spec))

cust = cust.withColumn('required_sales', F.when(cust['last_6_day_sales'].isNotNull(), cust['last_1_day_sales']+cust['last_2_day_sales']+cust['last_3_day_sales']+cust['last_6_day_sales']+cust['last_5_day_sales']+cust['last_4_day_sales']+cust['sales_sum']).otherwise(None))

cust = cust.withColumn('required_sales_avg', F.round(cust['required_sales']/7,2)).filter(cust['required_sales'].isNotNull()).select('visited_on','required_sales','required_sales_avg')                           

cust.display()


# COMMAND ----------

# DBTITLE 1,Friend Requests II: Who Has the Most Friends
data = [[1, 2, '2016/06/03'], [1, 3, '2016/06/08'], [2, 3, '2016/06/08'], [3, 4, '2016/06/09']]
request_accepted = pd.DataFrame(data, columns=['requester_id', 'accepter_id', 'accept_date']).astype({'requester_id':'Int64', 'accepter_id':'Int64', 'accept_date':'datetime64[ns]'})

req = spark.createDataFrame(request_accepted)

req_1 = req.groupBy('requester_id').agg(F.count('*').alias('cnt_1'))
req_2 = req.groupBy('accepter_id').agg(F.count('*').alias('cnt_2'))

req_f = req_1.join(req_2, req_1['requester_id'] == req_2['accepter_id'] , how = 'left')
req_f = req_f.withColumn('total_friends', req_f['cnt_1']+req_f['cnt_2']).orderBy('total_friends', ascending = False)
req_f = req_f.select(req_f['requester_id'].alias('id'), req_f['total_friends'].alias('num')).limit(1)
req_f.display()

# COMMAND ----------

# DBTITLE 1,Investments in 2016
data = [[1, 10, 5, 10, 10], [2, 20, 20, 20, 20], [3, 10, 30, 20, 20], [4, 10, 40, 40, 40]]
insurance = pd.DataFrame(data, columns=['pid', 'tiv_2015', 'tiv_2016', 'lat', 'lon']).astype({'pid':'Int64', 'tiv_2015':'Float64', 'tiv_2016':'Float64', 'lat':'Float64', 'lon':'Float64'})

ins = spark.createDataFrame(insurance)

ins_1 = ins.groupBy('lat', 'lon').agg(F.count('*').alias('cnt'))
ins_1 = ins_1.filter(ins_1['cnt'] == 1)

ins_n = ins.join(ins_1, (ins['lat'] == ins_1['lat']) & (ins['lon'] == ins_1['lon']),'inner')
ins_ans = ins_n.groupBy(ins_n['tiv_2015']).agg(F.sum('tiv_2016').alias('tiv_2016'))
ins_ans.display()


# COMMAND ----------

# DBTITLE 1,Department Top Three Salaries
data = [[1, 'Joe', 85000, 1], [2, 'Henry', 80000, 2], [3, 'Sam', 60000, 2], [4, 'Max', 90000, 1], [5, 'Janet', 69000, 1], [6, 'Randy', 85000, 1], [7, 'Will', 70000, 1]]
employee = pd.DataFrame(data, columns=['id', 'name', 'salary', 'departmentId']).astype({'id':'Int64', 'name':'object', 'salary':'Int64', 'departmentId':'Int64'})
data = [[1, 'IT'], [2, 'Sales']]
department = pd.DataFrame(data, columns=['departmentId', 'name']).astype({'departmentId':'Int64', 'name':'object'})

emp_o = spark.createDataFrame(employee)
dep = spark.createDataFrame(department)

window_spec = Window.partitionBy('departmentId','salary').orderBy(emp_o['salary'].desc())
emp = emp_o.withColumn('rnk', F.row_number().over(window_spec)).filter('rnk = 1')

window_spec_1 = Window.partitionBy('departmentId').orderBy(emp['salary'].desc())
emp_1 = emp.withColumn('rnk', F.row_number().over(window_spec_1)).filter('rnk <= 3')

emp_2 = emp_1.groupBy('departmentId').agg(F.min('salary').alias('min_top3_salary'))

emp_3 = emp_o.join(emp_2, on = 'departmentId', how = 'left')\
             .join(dep, on = 'departmentId', how = 'left')
             
emp_3 = emp_3.filter(emp_3['salary'] >= emp_3['min_top3_salary'])

emp_1.display()
emp_3.display()
# emp_ans.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Advanced String Functions / Regex / Clause

# COMMAND ----------

# DBTITLE 1,Fix Names in a Table
data = [[1, 'aLice'], [2, 'bOB']]
users = pd.DataFrame(data, columns=['user_id', 'name']).astype({'user_id':'Int64', 'name':'object'})

users = spark.createDataFrame(users)

users = users.withColumn("name",F.initcap("name"))

users = users.select("user_id", "name").orderBy("user_id")
users.display()


# COMMAND ----------

# DBTITLE 1,Patients With a Condition
data = [[1, 'Daniel', 'YFEV COUGH'], [2, 'Alice', ''], [3, 'Bob', 'DIAB100 MYOP'], [4, 'George', 'ACNE DIAB100'], [5, 'Alain', 'DIAB201']]
patients = pd.DataFrame(data, columns=['patient_id', 'patient_name', 'conditions']).astype({'patient_id':'int64', 'patient_name':'object', 'conditions':'object'})

patients = spark.createDataFrame(patients)

patients = patients.filter(
    (F.col("CONDITIONS").like("DIAB1%")) | 
    (F.col("CONDITIONS").like("% DIAB1%"))
)
patients.display()

# COMMAND ----------

# DBTITLE 1,Delete Duplicate Emails
data = [[1, 'john@example.com'], [2, 'bob@example.com'], [3, 'john@example.com']]
person = pd.DataFrame(data, columns=['id', 'email']).astype({'id':'int64', 'email':'object'})

df = spark.createDataFrame(person)
window_spec = Window.partitionBy('email').orderBy(df['id'])

df = df.withColumn('rnk', F.row_number().over(window_spec)).filter('rnk == 1')

df.display()

# COMMAND ----------

# DBTITLE 1,Second Highest Salary
data = [[1, 100], [2, 200], [3, 300]]
employee = pd.DataFrame(data, columns=['id', 'salary']).astype({'id':'int64', 'salary':'int64'})

employee = spark.createDataFrame(employee)

distinct_salaries = employee.select("salary").distinct().orderBy(F.desc("salary"))

second_highest_salary = distinct_salaries.limit(2).collect()

print(second_highest_salary[1]['salary'])

# COMMAND ----------

# DBTITLE 1,Group Sold Products By The Date
data = [['2020-05-30', 'Headphone'], ['2020-06-01', 'Pencil'], ['2020-06-02', 'Mask'], ['2020-05-30', 'Basketball'], ['2020-06-01', 'Bible'], ['2020-06-02', 'Mask'], ['2020-05-30', 'T-Shirt']]
activities = pd.DataFrame(data, columns=['sell_date', 'product']).astype({'sell_date':'datetime64[ns]', 'product':'object'})

act_o = spark.createDataFrame(activities)

act = act_o.groupBy('sell_date').agg(F.countDistinct('product').alias('cnt'), F.collect_list('product').alias('product_list'))\
           .withColumn('products',F.expr("array_join(sort_array(product_list), ',')"))
act.display()

# COMMAND ----------

# DBTITLE 1,List the Products Ordered in a Period
data = [[1, 'Leetcode Solutions', 'Book'], [2, 'Jewels of Stringology', 'Book'], [3, 'HP', 'Laptop'], [4, 'Lenovo', 'Laptop'], [5, 'Leetcode Kit', 'T-shirt']]
products = pd.DataFrame(data, columns=['product_id', 'product_name', 'product_category']).astype({'product_id':'Int64', 'product_name':'object', 'product_category':'object'})

data = [[1, '2020-02-05', 60], [1, '2020-02-10', 70], [2, '2020-01-18', 30], [2, '2020-02-11', 80], [3, '2020-02-17', 2], [3, '2020-02-24', 3], [4, '2020-03-01', 20], [4, '2020-03-04', 30], [4, '2020-03-04', 60], [5, '2020-02-25', 50], [5, '2020-02-27', 50], [5, '2020-03-01', 50]]
orders = pd.DataFrame(data, columns=['product_id', 'order_date', 'unit']).astype({'product_id':'Int64', 'order_date':'datetime64[ns]', 'unit':'Int64'})

products = spark.createDataFrame(products)
orders = spark.createDataFrame(orders)


orders_filtered = orders.filter((F.col("order_date") >= "2020-02-01") & (F.col("order_date") <= "2020-02-29"))

orders_aggregated = (orders_filtered.groupBy("product_id").agg(F.sum("unit").alias("unit")).filter(F.col("unit") >= 100))

result = (orders_aggregated.join(products, on="product_id", how="left").select(F.col("product_name"), F.col("unit")))

result.display()

# COMMAND ----------

# DBTITLE 1,Find Users With Valid E-Mails
data = [[1, 'Winston', 'winston@leetcode.com'], [2, 'Jonathan', 'jonathanisgreat'], [3, 'Annabelle', 'bella-@leetcode.com'], [4, 'Sally', 'sally.come@leetcode.com'], [5, 'Marwan', 'quarz#2020@leetcode.com'], [6, 'David', 'david69@gmail.com'], [7, 'Shapiro', '.shapo@leetcode.com']]
users = pd.DataFrame(data, columns=['user_id', 'name', 'mail']).astype({'user_id':'int64', 'name':'object', 'mail':'object'})

valid_emails = spark.createDataFrame(users)


valid_email_regex = r'^[A-Za-z][A-Za-z0-9_\.\-]*@leetcode\.com$'
valid_emails = valid_emails.filter((F.col("mail").rlike(valid_email_regex)))

valid_emails.show()