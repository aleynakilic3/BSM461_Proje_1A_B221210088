# -*- coding: utf-8 -*-
"""BSM 461 - Şablon 2 (Satış ve Stok) Gerçek Zamanlı Akış İşleme Hattı
Kapsanan Zorunlu Bileşenler: Z8 (Kafka & Streaming), Z3 (Bölümleme), Z10 (Veri Kalitesi)
Senaryo Kartı (B221210088): SLA <= 600 sn, Geç veri %1 (<=30 dk), Kayıp <= %0.1
"""
import argparse
from pyspark.sql import SparkSession, functions as F, types as T

ap = argparse.ArgumentParser()
ap.add_argument("--cikti", default="konsol", choices=["konsol", "s3"],
                help="konsol: ekrana yaz | s3: Parquet olarak s3a://islenmis/akis/")
a = ap.parse_args()

spark = (SparkSession.builder
         .appName("bsm461-akis-satis")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# ── 1. Şema ve Başlangıç Stok Tablosu ────────────────────────────────────────
SEMA = T.StructType([
    T.StructField("olay_zamani", T.StringType(), True),
    T.StructField("siparis_id", T.StringType(), True),
    T.StructField("magaza_id", T.IntegerType(), True),
    T.StructField("urun_id", T.IntegerType(), True),
    T.StructField("adet", T.IntegerType(), True),
    T.StructField("birim_fiyat", T.DoubleType(), True),
    T.StructField("tur", T.StringType(), True),
])

baslangic_stok = (spark.read.option("header", True).option("inferSchema", True)
                  .csv("s3a://ham/satis_baslangic_stok.csv"))

# ── 2. Kafka'dan Olayları Oku (Z8) ───────────────────────────────────────────
ham = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "kafka:29092")
       .option("subscribe", "satislar")
       .option("startingOffsets", "earliest")
       .option("failOnDataLoss", "false")
       .load())

olaylar = (ham.select(F.from_json(F.col("value").cast("string"), SEMA).alias("o"),
                      F.col("timestamp").alias("kafka_zamani"))
           .select("o.*", "kafka_zamani")
           .withColumn("olay_zamani", F.to_timestamp("olay_zamani"))
           .filter(
               F.col("olay_zamani").isNotNull() &
               F.col("siparis_id").isNotNull() &
               F.col("magaza_id").isNotNull() &
               F.col("urun_id").isNotNull() &
               (F.col("adet") > 0) &
               (F.col("birim_fiyat") > 0)
           ))

# ── 3. Watermark (30 dk) ve Tekrar Önleme (Idempotentlik) ────────────────────
tekil_olaylar = (olaylar
                 .withWatermark("olay_zamani", "30 minutes")
                 .dropDuplicates(["olay_zamani", "siparis_id"]))

# ── Akış Sorusu 1: Mağaza Başına 10 Dakikalık Pencerede Ciro ve Satış ─────────
magaza_ciro_akis = (tekil_olaylar
                    .filter(F.col("tur") == "satis")
                    .groupBy(
                        F.window("olay_zamani", "10 minutes"),
                        F.col("magaza_id")
                    )
                    .agg(
                        F.count("siparis_id").alias("satis_islem_sayisi"),
                        F.sum("adet").alias("toplam_satilan_adet"),
                        F.round(F.sum(F.col("adet") * F.col("birim_fiyat")), 2).alias("pencere_ciro")
                    ))

# ── Akış Sorusu 2: 10 Dakikalık Pencerede Net Stok ve Kritik Stok Uyarısı ────
net_stok_akis = (tekil_olaylar
                 .withColumn("net_degisim",
                             F.when(F.col("tur") == "stok_girisi", F.col("adet"))
                              .when(F.col("tur") == "iade", F.col("adet"))
                              .when(F.col("tur") == "satis", -F.col("adet"))
                              .otherwise(0))
                 .groupBy(
                     F.window("olay_zamani", "10 minutes"),
                     F.col("urun_id")
                 )
                 .agg(F.sum("net_degisim").alias("pencere_net_degisim"))
                 .join(F.broadcast(baslangic_stok), on="urun_id", how="left")
                 .withColumn("tahmini_stok", F.coalesce(F.col("baslangic_stok"), F.lit(0)) + F.col("pencere_net_degisim"))
                 .filter(F.col("tahmini_stok") <= 15))

# ── 4. Çıktı Akışlarını Başlat ───────────────────────────────────────────────
if a.cikti == "konsol":
    q1 = (magaza_ciro_akis.writeStream
          .queryName("magaza_ciro")
          .outputMode("update")
          .format("console")
          .option("truncate", False)
          .option("numRows", 10)
          .trigger(processingTime="15 seconds")
          .start())

    q2 = (net_stok_akis.writeStream
          .queryName("kritik_stok")
          .outputMode("update")
          .format("console")
          .option("truncate", False)
          .option("numRows", 10)
          .trigger(processingTime="15 seconds")
          .start())
else:
    q1 = (magaza_ciro_akis.writeStream
          .queryName("magaza_ciro_s3")
          .outputMode("append")
          .format("parquet")
          .option("path", "s3a://islenmis/akis/satis_ciro/")
          .option("checkpointLocation", "s3a://islenmis/_checkpoint/satis_ciro/")
          .trigger(processingTime="20 seconds")
          .start())

    q2 = (net_stok_akis.writeStream
          .queryName("kritik_stok_s3")
          .outputMode("append")
          .format("parquet")
          .option("path", "s3a://islenmis/akis/satis_stok/")
          .option("checkpointLocation", "s3a://islenmis/_checkpoint/satis_stok/")
          .trigger(processingTime="20 seconds")
          .start())

spark.streams.awaitAnyTermination()
