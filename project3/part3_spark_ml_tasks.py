import warnings

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, DecisionTreeRegressor, GBTRegressor
from pyspark.sql import SparkSession

warnings.filterwarnings('ignore')

if __name__ == '__main__':
    my_spark = SparkSession.builder.getOrCreate()
    customers = my_spark.read.csv('/Users/janet/PycharmProjects/ds503_project3/customers.csv',
                                  header=True,
                                  inferSchema=True)
    purchases = my_spark.read.csv('/Users/janet/PycharmProjects/ds503_project3/purchases.csv',
                                  header=True,
                                  inferSchema=True)

    # Task 2.5
    # Generate a dataset composed of customer ID, TransID, Age, Salary, TransNumItems and TransTotald
    purchase_all = purchases.join(customers, purchases.CustID == customers.ID)
    new_df = purchase_all.select("CustID", "TransID", 'Age', 'Salary', 'ItemNum', 'TransTotal')
    new_df.show()

    # prepare Xs and Y for ML model training
    # features: Age, Salary, TransNumItems
    # Y: TransTotal
    vectorAssembler = VectorAssembler(inputCols=['Age', 'Salary', 'ItemNum'],
                                      outputCol='features')
    new_df = vectorAssembler.transform(new_df)
    new_df = new_df.select(['features', 'TransTotal'])
    new_df.show()

    # Task 2.6
    # Randomly split to Trainset (80%) and Testset (20%)
    print('original dataset rows: ', new_df.count())
    (train_data, test_data) = new_df.randomSplit([0.8, 0.2], seed=0)
    print("train data rows: ", train_data.count())
    print("test data rows:  ", test_data.count())

    # Task 2.7 Predict the price
    # Simple Linear Regression
    lr = LinearRegression(featuresCol='features', labelCol='TransTotal',
                          maxIter=10, regParam=0.3, elasticNetParam=0.8)
    lr_model = lr.fit(train_data)
    lr_predictions = lr_model.transform(test_data)

    # Decision Tree Regression
    dt = DecisionTreeRegressor(featuresCol='features', labelCol='TransTotal')
    dt_model = dt.fit(train_data)
    dt_predictions = dt_model.transform(test_data)

    # GradientBoosted Tree Regression
    gbt = GBTRegressor(featuresCol='features', labelCol='TransTotal', maxIter=10)
    gbt_model = gbt.fit(train_data)
    gbt_predictions = gbt_model.transform(test_data)

    # Task 2.8 Evaluate the price
    # Root Mean Squared Error (RMSE)
    eval_rmse = RegressionEvaluator(labelCol='TransTotal', predictionCol='prediction', metricName='rmse')
    lr_rmse = eval_rmse.evaluate(lr_predictions)
    print("Root Mean Squared Error (RMSE) on test data using Linear Regression = %g" % lr_rmse)
    dt_rmse = eval_rmse.evaluate(dt_predictions)
    print("Root Mean Squared Error (RMSE) on test data using Decision Tree Regression = %g" % dt_rmse)
    gbt_rmse = eval_rmse.evaluate(gbt_predictions)
    print("Root Mean Squared Error (RMSE) on test data using GradientBoosted Tree Regression = %g" % gbt_rmse)
    # R2
    eval_r2 = RegressionEvaluator(labelCol='TransTotal', predictionCol='prediction', metricName='r2')
    lr_r2 = eval_r2.evaluate(lr_predictions)
    print("R Squared (R2) on test data using Linear Regression = %g" % lr_r2)
    dt_r2 = eval_r2.evaluate(dt_predictions)
    print("R Squared (R2) on test data using Decision Tree Regression = %g" % dt_r2)
    gbt_r2 = eval_r2.evaluate(gbt_predictions)
    print("R Squared (R2) on test data using GradientBoosted Tree Regression = %g" % gbt_r2)
    # MAE
    eval_mae = RegressionEvaluator(labelCol='TransTotal', predictionCol='prediction', metricName='mae')
    lr_mae = eval_mae.evaluate(lr_predictions)
    print("Mean Absolute Error (MAE) on test data using Linear Regression = %g" % lr_mae)
    dt_mae = eval_mae.evaluate(dt_predictions)
    print("Mean Absolute Error (MAE) on test data using Decision Tree Regression = %g" % dt_mae)
    gbt_mae = eval_mae.evaluate(gbt_predictions)
    print("Mean Absolute Error (MAE) on test data using GradientBoosted Tree Regression = %g" % gbt_mae)

    my_spark.stop()





