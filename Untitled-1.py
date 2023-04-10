# %%
import pip
def import_or_install(package):
    try:
        __import__(package)
    except ImportError:
        pip.main(['install', package])   

# %%
import_or_install("datasets")
import_or_install("pyspark.sql")

import warnings

warnings.filterwarnings("ignore")

# %%
from datasets import load_dataset
from pyspark.sql import SparkSession
dataset = load_dataset("jxm/the_office_lines")

warnings.filterwarnings("ignore")

# %%
spark = SparkSession.builder.appName("LoadDataset").getOrCreate()

dataset = load_dataset("jxm/the_office_lines")

df_test, df_train, df_valid = spark.createDataFrame(dataset["test"]), spark.createDataFrame(dataset["train"]), spark.createDataFrame(dataset["val"])

warnings.filterwarnings("ignore")

# %%
df_test.show()

# df = spark.createDataFrame(dataset["train"])

# %%
df_train.show()

# %%
df_valid.show()




#Note: The jupyter notebook has been turned into a pdf and is available in the repository so that the code can be viewed without running it.
# %%
