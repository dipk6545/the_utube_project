from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    from_json,
    explode,
    from_unixtime,
    current_timestamp,
)
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType

# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.
catalog_name=spark.conf.get("catalog_name")
volume_path = f"/Volumes/{catalog_name}/bronze/earquake_data_volumne"
primary_key = "id"
geometry_schema = StructType([StructField("coordinates", ArrayType(DoubleType()))])
properties_schema = StructType(
    [
        StructField("mag", StringType()),
        StructField("place", StringType()),
        StructField("time", StringType()),
        StructField("status", StringType()),
        StructField("tsunami", StringType()),
        StructField("type", StringType()),
        StructField("url", StringType()),
        StructField("detail", StringType()),
        StructField("felt", StringType()),
        StructField("cdi", StringType()),
        StructField("mmi", StringType()),
        StructField("alert", StringType()),
        StructField("sig", StringType()),
        StructField("net", StringType()),
        StructField("code", StringType()),
        StructField("ids", StringType()),
        StructField("sources", StringType()),
        StructField("types", StringType()),
        StructField("nst", StringType()),
        StructField("dmin", StringType()),
        StructField("rms", StringType()),
        StructField("gap", StringType()),
        StructField("magType", StringType()),
        StructField("title", StringType()),
    ]
)

feature_schema = StructType(
    [
        StructField("id", StringType()),
        StructField("properties", properties_schema),
        StructField("geometry", geometry_schema),
    ]
)
schema = ArrayType(feature_schema)


@dp.view(name="earthquake_data_vw")
def earthquake_data():
    df = (
        spark.readStream.format("cloudfiles")
        .option("cloudFiles.format", "json")
        .load(volume_path)
        .withColumn("_load_timestamp", current_timestamp())
    )
    df = df.withColumn("parsed_data", from_json(col("features"), schema))
    df = df.select(explode(col("parsed_data")).alias("features"),"_load_timestamp")
    df = df.select(
        "features.properties.*",
        "features.id",
        col("features.geometry.coordinates")[0].alias("longitude"),
        col("features.geometry.coordinates")[1].alias("latitude"),
        col("features.geometry.coordinates")[2].alias("depth"),
        "_load_timestamp"
    )

    df = (
        df.withColumn("time", from_unixtime(col("time") / 1000).cast("timestamp"))
        .withColumn("mag", col("mag").cast("double"))
        .withColumn("nst", col("nst").cast("double"))
        .withColumn("sig", col("sig").cast("double"))
        .withColumn("tsunami", col("tsunami").cast("int"))
        .withColumn("felt", col("felt").cast("double"))
    )

    return df


dp.create_streaming_table(name="earthquake_data_final")

dp.apply_changes(
    target="earthquake_data_final",
    source="earthquake_data_vw",
    keys=[primary_key],
    sequence_by="_load_timestamp",
    stored_as_scd_type="1",
)
