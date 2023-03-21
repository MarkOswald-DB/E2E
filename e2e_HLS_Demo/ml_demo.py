# Databricks notebook source
df = spark.read.parquet('dbfs:/home/riley.rustad@databricks.com/hlse2e/source/encounters')

# COMMAND ----------

df.sample(.02).coalesce(1).write.mode('overwrite').csv('dbfs:/home/riley.rustad@databricks.com/encounters', header=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from riley_test

# COMMAND ----------

# hello from Mark hello from riley
# Standard scientific Python imports
import matplotlib.pyplot as plt

# Import datasets, classifiers and performance metrics
from sklearn import datasets, svm, metrics
from sklearn.model_selection import train_test_split

# COMMAND ----------

digits = datasets.load_digits()

_, axes = plt.subplots(nrows=1, ncols=4, figsize=(10, 3))
for ax, image, label in zip(axes, digits.images, digits.target):
    ax.set_axis_off()
    ax.imshow(image, cmap=plt.cm.gray_r, interpolation="nearest")
    ax.set_title("Training: %i" % label)

# COMMAND ----------

import pandas as pd
import numpy as np
pdf = pd.DataFrame(data= np.c_[digits['data'], digits['target']],
                     columns= digits['feature_names'] + ['target'])
pdf.head()

# COMMAND ----------

import pyspark.pandas as ps

sdf = spark.createDataFrame(pdf)  # PySpark DataFrame
psdf = sdf.pandas_api()  # pandas-on-Spark DataFrame
# Query via SQL
psdf.head()

# COMMAND ----------

psdf.shape

# COMMAND ----------

psdf[psdf['pixel_1_3'] > 10].head()

# COMMAND ----------

psdf[psdf['pixel_1_3'] > 15].shape

# COMMAND ----------


