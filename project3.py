# Databricks notebook source
spark.version

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --drop the table if exists
# MAGIC DROP TABLE IF EXISTS data_mentalhealth2;
# MAGIC  
# MAGIC --create table
# MAGIC CREATE TABLE  IF NOT EXISTS data_mentalhealth2 ( 
# MAGIC                     Age string, Gender string,
# MAGIC                     Country string,
# MAGIC                     state string ,
# MAGIC                     self_employed string,
# MAGIC                     family_history string,
# MAGIC                     treatment string,
# MAGIC                     work_interfere string,
# MAGIC                     no_employees string,
# MAGIC                     remote_work string,
# MAGIC                     tech_company string,
# MAGIC                     benefits string,
# MAGIC                     care_options string,
# MAGIC                     wellness_program string,
# MAGIC                     seek_help string,
# MAGIC                     anonymity string,
# MAGIC                     leave string,
# MAGIC                     mental_health_consequence string,
# MAGIC                     phys_health_consequence string,
# MAGIC                     coworkers string,
# MAGIC                     supervisor string,
# MAGIC                     mental_health_interview string,
# MAGIC                     phys_health_interview string,
# MAGIC                     mental_vs_physical string,
# MAGIC                     obs_consequence string)
# MAGIC USING CSV
# MAGIC OPTIONS (path "/FileStore/tables/mentalhealth_data_clean-1.csv", header "true");
# MAGIC  
# MAGIC /* check results */
# MAGIC select * from data_mentalhealth2 limit 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select 
# MAGIC   treatment,
# MAGIC   count(1) as treat_count
# MAGIC from
# MAGIC   data_mentalhealth2
# MAGIC group by
# MAGIC   treatment

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select 
# MAGIC   gender,
# MAGIC   treatment,
# MAGIC   count(1) as treat_count
# MAGIC from
# MAGIC   data_mentalhealth2
# MAGIC group by
# MAGIC   treatment, gender

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select 
# MAGIC   age,
# MAGIC   treatment,
# MAGIC   count(1) as treat_count
# MAGIC from
# MAGIC   data_mentalhealth2
# MAGIC group by
# MAGIC   treatment, age

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select 
# MAGIC   family_history,
# MAGIC   treatment,
# MAGIC   count(1) as treat_count
# MAGIC from
# MAGIC   data_mentalhealth2
# MAGIC group by
# MAGIC   treatment, family_history

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select 
# MAGIC   work_interfere,
# MAGIC   treatment,
# MAGIC   count(1) as treat_count
# MAGIC from
# MAGIC   data_mentalhealth2
# MAGIC group by
# MAGIC   treatment, work_interfere

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select 
# MAGIC   company_support,
# MAGIC   treatment,
# MAGIC   count(1) as treat_count
# MAGIC from
# MAGIC   mentalhealth_stat_csv
# MAGIC group by
# MAGIC   treatment, company_support

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select 
# MAGIC   mental_health_consequence,
# MAGIC   treatment,
# MAGIC   count(1) as treat_count
# MAGIC from
# MAGIC   data_mentalhealth2
# MAGIC group by
# MAGIC   treatment, mental_health_consequence

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select 
# MAGIC   obs_consequence,
# MAGIC   treatment,
# MAGIC   count(1) as treat_count
# MAGIC from
# MAGIC   data_mentalhealth2
# MAGIC group by
# MAGIC   treatment, obs_consequence

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select state, count(IF(work_interfere = 'Often' or work_interfere = 'Sometimes', 1, NULL)) 
# MAGIC       / count(*) as oftensometimespercent 
# MAGIC from mentalhealth_stat_csv  
# MAGIC where country = 'United States' and state != 'NA'
# MAGIC group by state

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC   select state, count(IF(treatment = 'Yes', 1, NULL)) / count(*) as treatpercent 
# MAGIC   from mentalhealth_stat_csv  
# MAGIC   where country = 'United States' and state != 'NA'
# MAGIC   group by state

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC   select state, count(IF(company_support = 'High', 1, NULL)) / count(*) as highsupportpercent 
# MAGIC   from mentalhealth_stat_csv  
# MAGIC   where country = 'United States' and state != 'NA'
# MAGIC   group by state

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select state, company_support, cnt, 
# MAGIC   cast(( cnt * 100/(sum(cnt) over (partition by state)) ) as INT)
# MAGIC from 
# MAGIC   (select state, company_support, count(*) as cnt 
# MAGIC   from mentalhealth_stat_csv  
# MAGIC   where country = 'United States' and state != 'NA'
# MAGIC   group by state, company_support) 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select 
# MAGIC   'mental' as mental_physical,
# MAGIC   mental_health_consequence as consequence,
# MAGIC   count(mental_health_consequence) as cnt
# MAGIC from
# MAGIC   data_mentalhealth2
# MAGIC group by
# MAGIC   mental_health_consequence
# MAGIC   
# MAGIC union
# MAGIC 
# MAGIC select 
# MAGIC   'physical' as mental_physical,
# MAGIC   phys_health_consequence as consequence,
# MAGIC   count(phys_health_consequence) as cnt
# MAGIC from
# MAGIC   data_mentalhealth2  
# MAGIC group by
# MAGIC   phys_health_consequence

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select 
# MAGIC   'mental' as mental_physical,
# MAGIC   mental_health_interview as interview,
# MAGIC   count(mental_health_interview) as cnt
# MAGIC from
# MAGIC   data_mentalhealth2
# MAGIC group by
# MAGIC   mental_health_interview
# MAGIC   
# MAGIC union
# MAGIC 
# MAGIC select 
# MAGIC   'physical' as mental_physical,
# MAGIC   phys_health_interview as interview,
# MAGIC   count(phys_health_interview) as cnt
# MAGIC from
# MAGIC   data_mentalhealth2  
# MAGIC group by
# MAGIC   phys_health_interview

# COMMAND ----------

data = spark.read.format('csv')\
    .load( '/FileStore/tables/mentalhealth_stat.csv', header=True, inferSchema = True)
display(data)

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer, VectorAssembler, OneHotEncoder
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
 
# Load the data stored in csv format as a DataFrame.
data = spark.read.format("csv").load("/FileStore/tables/mentalhealth_stat.csv", header=True, 
                                     inferSchema = True)
#data.show()
data2 =data.select(data.company_support,data.work_interfere, data.Age, data.Gender, data.mental_health_consequence, data.family_history, data.treatment.alias('label'))
 
# Split the data into training and test sets (30% held out for testing)
(trainingData, testData) = data2.randomSplit([0.7, 0.3])

display(trainingData)

# COMMAND ----------

# Index labels, adding metadata to the label column.
# Fit on whole dataset to include all labels in index.
labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(data2)
# string attributes to indexer
company_supportIndexer =StringIndexer(inputCol='company_support', outputCol="indexedcompany_support")
work_interfereIndexer =StringIndexer(inputCol='work_interfere', outputCol="indexedwork_interfere")
AgeIndexer =StringIndexer(inputCol='Age', outputCol="indexedAge")
GenderIndexer =StringIndexer(inputCol='Gender', outputCol="indexedGender")
mental_health_consequenceIndexer =StringIndexer(inputCol='mental_health_consequence', outputCol="indexedmental_health_consequence")
family_historyIndexer =StringIndexer(inputCol='family_history', outputCol="indexedfamily_history")

company_supportEncoder = OneHotEncoder( inputCol='indexedcompany_support', outputCol= 'company_supportVec')
work_interfereEncoder = OneHotEncoder( inputCol='indexedwork_interfere', outputCol= 'work_interfereVec')
AgeEncoder = OneHotEncoder( inputCol='indexedAge', outputCol= 'AgeVec')
GenderEncoder = OneHotEncoder( inputCol='indexedGender', outputCol= 'GenderVec')
mental_health_consequenceEncoder = OneHotEncoder( inputCol='indexedmental_health_consequence', outputCol= 'mental_health_consequenceVec')
family_historyEncoder = OneHotEncoder( inputCol='indexedfamily_history', outputCol= 'family_historyVec')

featureAssembler = VectorAssembler().setInputCols(['company_supportVec','work_interfereVec','AgeVec','GenderVec','mental_health_consequenceVec', 'family_historyVec']).setOutputCol('features')

# COMMAND ----------

# Train a DecisionTree model.
dt = DecisionTreeClassifier(labelCol="indexedLabel", featuresCol="features")

# Chain indexers and tree in a Pipeline
pipeline = Pipeline(stages=[labelIndexer, company_supportIndexer,work_interfereIndexer, AgeIndexer, GenderIndexer, mental_health_consequenceIndexer, family_historyIndexer, company_supportEncoder,work_interfereEncoder, AgeEncoder, GenderEncoder, mental_health_consequenceEncoder, family_historyEncoder, featureAssembler, dt])
 
# Train model.  This also runs the indexers.
model = pipeline.fit(trainingData)
 
# Make predictions.
predictions = model.transform(testData)
 
# Select example rows to display.
predictions.select("prediction", "indexedLabel", "features").show(10)
 
# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
 
print("Accuracy = %g " % (accuracy))
print("Test Error = %g " % (1.0 - accuracy))
 
treeModel = model.stages
# summary only
print(treeModel)

# COMMAND ----------

tree = model.stages[-1]

display(tree) #visualize the decision tree model

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer, VectorAssembler, OneHotEncoder
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
 
# Load the data stored in csv format as a DataFrame.
data = spark.read.format("csv").load("/FileStore/tables/mentalhealth_stat.csv", header=True, 
                                     inferSchema = True)
#data.show()
data2 =data.select(data.company_support,data.work_interfere, data.Age, data.Gender, data.mental_health_consequence, data.family_history, data.treatment.alias('label'))
 
# Split the data into training and test sets (30% held out for testing)
(trainingData, testData) = data2.randomSplit([0.7, 0.3])

# Index labels, adding metadata to the label column.
# Fit on whole dataset to include all labels in index.
labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(data2)
# string attributes to indexer
company_supportIndexer =StringIndexer(inputCol='company_support', outputCol="indexedcompany_support")
work_interfereIndexer =StringIndexer(inputCol='work_interfere', outputCol="indexedwork_interfere")
AgeIndexer =StringIndexer(inputCol='Age', outputCol="indexedAge")
GenderIndexer =StringIndexer(inputCol='Gender', outputCol="indexedGender")
mental_health_consequenceIndexer =StringIndexer(inputCol='mental_health_consequence', outputCol="indexedmental_health_consequence")
family_historyIndexer =StringIndexer(inputCol='family_history', outputCol="indexedfamily_history")

featureAssembler = VectorAssembler().setInputCols(['indexedcompany_support','indexedwork_interfere','indexedAge','indexedGender','indexedmental_health_consequence', 'indexedfamily_history']).setOutputCol('features')

# Train a DecisionTree model.
dt = DecisionTreeClassifier(labelCol="indexedLabel", featuresCol="features")

# Chain indexers and tree in a Pipeline
pipeline = Pipeline(stages=[labelIndexer, company_supportIndexer,work_interfereIndexer, AgeIndexer, GenderIndexer, mental_health_consequenceIndexer, family_historyIndexer,  featureAssembler, dt])
 
# Train model.  This also runs the indexers.
model = pipeline.fit(trainingData)
 
# Make predictions.
predictions = model.transform(testData)
 
# Select example rows to display.
predictions.select("prediction", "indexedLabel", "features").show(15)
 
# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
 
print("Accuracy = %g " % (accuracy))
print("Test Error = %g " % (1.0 - accuracy))


# COMMAND ----------


tree = model.stages[-1]

display(tree) #visualize the decision tree model

# COMMAND ----------

from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.feature import StringIndexer, VectorIndexer, VectorAssembler, OneHotEncoder

from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml import Pipeline
 
# Prepare training and test data.
# Load the data stored in csv format as a DataFrame.
data = spark.read.format("csv").load("/FileStore/tables/mentalhealth_stat.csv", header=True, 
                                     inferSchema = True)

data2 =data.select(data.company_support,data.work_interfere, data.Age, data.Gender, data.mental_health_consequence, data.family_history, data.treatment.alias('label'))

train, test = data2.randomSplit([0.9, 0.1], seed=12345)


# COMMAND ----------

# Index labels, adding metadata to the label column.
# Fit on whole dataset to include all labels in index.
labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(data2)

#labelEncoder = OneHotEncoder( inputCol='indexedLabel', outputCol= 'labelVec')
# Automatically identify categorical features, and index them.
# We specify maxCategories so features with > 4 distinct values are treated as continuous.
company_supportIndexer =StringIndexer(inputCol='company_support', outputCol="indexedcompany_support")
work_interfereIndexer =StringIndexer(inputCol='work_interfere', outputCol="indexedwork_interfere")
AgeIndexer =StringIndexer(inputCol='Age', outputCol="indexedAge")
GenderIndexer =StringIndexer(inputCol='Gender', outputCol="indexedGender")
mental_health_consequenceIndexer =StringIndexer(inputCol='mental_health_consequence', outputCol="indexedmental_health_consequence")
family_historyIndexer =StringIndexer(inputCol='family_history', outputCol="indexedfamily_history")


featureAssembler = VectorAssembler().setInputCols(['indexedcompany_support','indexedwork_interfere','indexedAge','indexedGender','indexedmental_health_consequence', 'indexedfamily_history']).setOutputCol('features')

# COMMAND ----------


# Train a DecisionTree model.
dt = DecisionTreeClassifier(labelCol="indexedLabel", 
                            featuresCol="features", maxDepth=2)

paramGrid = ParamGridBuilder() \
    .addGrid(dt.maxDepth, [2, 5, 10, 20, 30]) \
    .addGrid(dt.maxBins, [10, 20, 40, 80, 100]) \
    .build()
 
# A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
crossval  = CrossValidator(estimator=dt,
                     estimatorParamMaps=paramGrid,
                     evaluator=MulticlassClassificationEvaluator(
                       labelCol="indexedLabel", 
                       predictionCol="prediction", 
                       metricName="accuracy"),
                     numFolds=5)

# Chain indexers and tree in a Pipeline
pipelineCV = Pipeline(stages=[labelIndexer,company_supportIndexer,
                              work_interfereIndexer, 
                              AgeIndexer, GenderIndexer, 
                              mental_health_consequenceIndexer, 
                              family_historyIndexer, 
                              featureAssembler, crossval])
# Run cross-validation, and choose the best set of parameters.
modelCV = pipelineCV.fit(train)

#va = modelCV.stages[-2]
treeCV = modelCV.stages[-1].bestModel

display(treeCV) #visualize the best decision tree model


# COMMAND ----------

print(treeCV.toDebugString) #print the nodes of the decision tree model
 
# Make predictions on test data. model is the model with combination of parameters
# that performed best.
# compute accuracy on the test set
 
result = modelCV.transform(test)
# Select example rows to display.
result.select("prediction", "indexedLabel","label","features").show(15)
 
evaluator = MulticlassClassificationEvaluator(
    labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(result)
 
print("Accuracy = %g " % (accuracy))
print("Test Error = %g " % (1.0 - accuracy))

# COMMAND ----------


