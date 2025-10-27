import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("TestVentes").getOrCreate()

def test_calcul_revenu(spark):
    data = [("Choco", 10, 2.5), ("Cafe", 5, 3.0)]
    df = spark.createDataFrame(data, ["produit", "quantite", "prix_unitaire"])
    df = df.withColumn("revenu", col("quantite") * col("prix_unitaire"))
    result = df.filter(col("produit")=="Choco").collect()[0]["revenu"]
    assert result == 25.0
