from pyspark.sql import SparkSession
import os
import time
import urllib.error
import urllib.request

KAFKA_BOOTSTRAP = os.environ["FLOWPLANE_KAFKA_BOOTSTRAP"]
RAW_TOPIC = os.environ["FLOWPLANE_RAW_TOPIC"]
TRANSFORMED_TOPIC = os.environ["FLOWPLANE_TRANSFORMED_TOPIC"]
DLQ_TOPIC = os.environ["FLOWPLANE_DLQ_TOPIC"]
GROUP_ID = os.environ["FLOWPLANE_GROUP_ID"]
TRANSFORM_URL = os.environ["FLOWPLANE_TRANSFORM_URL"]
CHECKPOINT = os.environ.get("FLOWPLANE_CHECKPOINT", "/tmp/flowplane-spark-checkpoint")


def call_runtime(key, payload):
    last_failure = None
    for attempt in range(1, 5):
        request = urllib.request.Request(
            TRANSFORM_URL,
            data=payload.encode("utf-8"),
            method="POST",
            headers={
                "Content-Type": "application/json",
                "X-FlowPlane-Source-Topic": RAW_TOPIC,
                "X-FlowPlane-Source-Key": key or "spark-streaming-evidence",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                status = response.status
                result = response.headers.get("X-FlowPlane-Result")
                body = response.read().decode("utf-8")
        except urllib.error.HTTPError as error:
            status = error.code
            result = error.headers.get("X-FlowPlane-Result")
            body = error.read().decode("utf-8")
        except (urllib.error.URLError, TimeoutError, OSError) as error:
            last_failure = str(error)
            if attempt < 4:
                time.sleep(2 ** (attempt - 1))
                continue
            raise RuntimeError(f"Flowplane transport failed after {attempt} attempts: {last_failure}")
        if status == 200 and result == "SUCCESS":
            return "SUCCESS", body
        if status == 422 and result == "ERROR":
            return "DLQ", body
        last_failure = f"unexpected status={status} result={result!r}"
        if attempt < 4 and (status >= 500 or status in (408, 429)):
            time.sleep(2 ** (attempt - 1))
            continue
        raise RuntimeError(last_failure)
    raise RuntimeError(last_failure or "Flowplane call failed")


spark = SparkSession.builder.appName("Flowplane Spark evidence").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
source = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", RAW_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .option("kafka.group.id", GROUP_ID)
    .load()
)


def process_batch(batch_df, batch_id):
    rows = batch_df.selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value").collect()
    success_rows = []
    dlq_rows = []
    for row in rows:
        disposition, body = call_runtime(row["key"], row["value"])
        (success_rows if disposition == "SUCCESS" else dlq_rows).append((row["key"] or "", body))
    for records, topic in ((success_rows, TRANSFORMED_TOPIC), (dlq_rows, DLQ_TOPIC)):
        if records:
            (
                spark.createDataFrame(records, ["key", "value"])
                .selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value")
                .write.format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                .option("topic", topic)
                .save()
            )
    print(f"batch={batch_id} input={len(rows)} success={len(success_rows)} dlq={len(dlq_rows)}", flush=True)


query = (
    source.writeStream.foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="1 second")
    .start()
)
query.awaitTermination()
