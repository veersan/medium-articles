---
title: Handling Bad Records in Streaming Pipelines Using Dead Letter Queues in PySpark
author: Santhosh Kumar Veeramalla
published: 2025
license: CC BY-NC 4.0
---------------------

# Handling Bad Records in Streaming Pipelines Using Dead Letter Queues in PySpark

## Introduction

Real-time data pipelines are foundational to today’s data platforms, enabling rapid analytics and data-driven decisions. However, real-world data is messy — malformed, incomplete, or schema-breaking records can disrupt your pipeline.

That’s where **Dead Letter Queues (DLQs)** come in. In this article, we’ll explore how to implement DLQs in **PySpark Structured Streaming** to isolate bad records and ensure pipeline resilience.

---

## What is a Dead Letter Queue?

A **Dead Letter Queue (DLQ)** is a fallback storage or topic where records that fail processing are sent. DLQs are common in systems like Kafka, Azure Event Hubs, and AWS SQS, and they can be applied to data engineering workflows to:

* Preserve and inspect bad records
* Avoid pipeline crashes
* Enable post-processing or retries

---

## Use Case: PySpark + Kafka Streaming

You’re consuming JSON data from Kafka, expecting a certain schema. But incoming messages might:

* Be invalid JSON
* Miss required fields
* Have unexpected types

Without a DLQ, these errors can lead to dropped data or job failure. Here’s how to separate good vs bad records using PySpark.

---

## Implementation Example

```python
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType

schema = StructType().add("id", StringType()).add("event_time", StringType())

raw_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "<broker>") \
    .option("subscribe", "events_topic") \
    .load()

parsed_df = raw_df.withColumn("parsed", from_json(col("value"), schema))

valid_df = parsed_df.filter(col("parsed").isNotNull())
dlq_df = parsed_df.filter(col("parsed").isNull())

valid_df.select("parsed.*").writeStream \
    .format("delta") \
    .option("checkpointLocation", "/mnt/checkpoints/valid") \
    .start("/mnt/output/valid")

dlq_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/mnt/checkpoints/dlq") \
    .start("/mnt/output/dlq")
```

---

## Other DLQ Techniques

### 1. `badRecordsPath` (File Sources)

```python
spark.read.option("badRecordsPath", "/mnt/dlq").json("/mnt/input/events")
```

### 2. Kafka DLQ Topic

Write bad records to a Kafka topic instead of a file sink.

### 3. Permissive Mode

In batch processing, Spark supports options like mode(“PERMISSIVE”) to handle schema mismatches gracefully. However, Structured Streaming doesn’t support permissive mode directly.

To achieve schema evolution tolerance in streaming, you can:

• Define nullable fields in evolving schemas

• Use from_json() with schema_of_json to dynamically handle schema changes

• Implement custom logic to catch and route mismatched records into a Dead Letter Queue (DLQ)

---

## Best Practices

* Add monitoring/alerting on DLQ volume
* Include error reason in DLQ record schema
* Automate reprocessing pipelines
* Periodically clean DLQ storage to save cost

---

## Conclusion

DLQs are a critical part of building production-grade streaming pipelines. With PySpark, Delta Lake, and Kafka, you can design resilient workflows that separate bad data without interrupting the entire process.

---

## 📖 How to Cite This Article

**Plain Text:**

Veeramalla, Santhosh Kumar. (2025). *Handling Bad Records in Streaming Pipelines Using Dead Letter Queues in PySpark*. Medium.
[https://medium.com/@santhoshkumarv/handling-bad-records-in-streaming-pipelines-using-dead-letter-queues-in-pyspark-265e7a55eb29](https://medium.com/@santhoshkumarv/handling-bad-records-in-streaming-pipelines-using-dead-letter-queues-in-pyspark-265e7a55eb29)

**Google Scholar / BibTeX Format:**

```bibtex
@misc{veeramalla2025dlq,
  author       = {Santhosh Kumar Veeramalla},
  title        = {Handling Bad Records in Streaming Pipelines Using Dead Letter Queues in PySpark},
  year         = {2025},
  howpublished = {\url{https://medium.com/@santhoshkumarv/handling-bad-records-in-streaming-pipelines-using-dead-letter-queues-in-pyspark-265e7a55eb29}},
  note         = {Published on Medium}
}
```

---

## 🔗 Stay Connected

* [Google Scholar]()
* [Medium](https://medium.com/@santhoshkumarv)
* [GitHub](https://github.com/veersan)

> Follow for insights on PySpark, streaming pipelines, and real-time data engineering.
