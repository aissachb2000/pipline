from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit

spark = SparkSession.builder.getOrCreate()

# Charger dataset
df = spark.read.csv("/dbfs/FileStore/tables/ventes.csv", header=True, inferSchema=True)

# Nettoyer les valeurs nulles
df_clean = df.withColumn("quantite", coalesce(col("quantite"), lit(0))) \
             .withColumn("prix_unitaire", coalesce(col("prix_unitaire"), lit(0.0)))

# Calculer revenu
df_clean = df_clean.withColumn("revenu", col("quantite") * col("prix_unitaire"))

# Agrégation
df_result = df_clean.groupBy("produit").sum("revenu")

# Sauvegarder
df_result.write.mode("overwrite").csv("/dbfs/FileStore/tables/ventes_result.csv")
