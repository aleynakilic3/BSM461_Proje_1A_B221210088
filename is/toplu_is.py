# -*- coding: utf-8 -*-
"""BSM 461 - Şablon 2 (Satış ve Stok) Toplu İşleme (Batch) Hattı
Kapsanan Zorunlu Bileşenler: Z2, Z3, Z5, Z7, Z10
"""
import argparse
import time
from pyspark.sql import SparkSession, functions as F, types as T

ap = argparse.ArgumentParser()
ap.add_argument("--sablon", default="satis", choices=["satis"])
a = ap.parse_args()

spark = (SparkSession.builder
         .appName("bsm461-toplu-satis")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

HAM_OLAY = "s3a://ham/satis/tarih=*/*.jsonl"
HAM_URUN = "s3a://ham/boyut/satis/urunler.csv"
HAM_MAGAZA = "s3a://ham/boyut/satis/magazalar.csv"
HAM_STOK = "s3a://ham/satis_baslangic_stok.csv"

PARQUET_BASE = "s3a://islenmis/satis_parquet"
DELTA_TABLO = "s3a://tablolar/satis_delta"

# ── 1. Elle Tanımlanmış Şema ile Okuma (Z2 / H2) ─────────────────────────────
olay_semasi = T.StructType([
    T.StructField("olay_zamani", T.StringType(), True),
    T.StructField("siparis_id", T.StringType(), True),
    T.StructField("magaza_id", T.IntegerType(), True),
    T.StructField("urun_id", T.IntegerType(), True),
    T.StructField("adet", T.IntegerType(), True),
    T.StructField("birim_fiyat", T.DoubleType(), True),
    T.StructField("tur", T.StringType(), True),
])

t0 = time.time()
ham = (spark.read.schema(olay_semasi).json(HAM_OLAY)
       .withColumn("olay_zamani", F.to_timestamp("olay_zamani"))
       .withColumn("tarih", F.to_date("olay_zamani")))
ham.cache()
ham_sayisi = ham.count()
print(f"\n[1] Ham kayıt sayısı: {ham_sayisi:,} ({time.time()-t0:.2f} sn)")

# ── 2. Veri Kalitesi ve Temizleme (Z10) ──────────────────────────────────────
eksik_sayisi = ham.filter(
    F.col("olay_zamani").isNull() |
    F.col("siparis_id").isNull() |
    F.col("magaza_id").isNull() |
    F.col("urun_id").isNull() |
    F.col("adet").isNull() |
    F.col("birim_fiyat").isNull() |
    F.col("tur").isNull() |
    (F.col("adet") <= 0) |
    (F.col("birim_fiyat") <= 0)
).count()

gecerli = ham.filter(
    F.col("olay_zamani").isNotNull() &
    F.col("siparis_id").isNotNull() &
    F.col("magaza_id").isNotNull() &
    F.col("urun_id").isNotNull() &
    F.col("adet").isNotNull() &
    F.col("birim_fiyat").isNotNull() &
    F.col("tur").isNotNull() &
    (F.col("adet") > 0) &
    (F.col("birim_fiyat") > 0)
)

temiz = gecerli.dropDuplicates(["siparis_id"])
temiz.cache()
temiz_sayisi = temiz.count()
tekrar_sayisi = gecerli.count() - temiz_sayisi

print(f"[2] Veri Kalitesi Raporu (Z10):")
print(f"    - Eksik/bozuk kayıt (atıldı)    : {eksik_sayisi:,}")
print(f"    - Tekrarlanan siparis_id (atıldı): {tekrar_sayisi:,}")
print(f"    - Temiz ve tekil kayıt sayısı    : {temiz_sayisi:,}")

# ── 3. Parquet Sıkıştırma Karşılaştırması (Z2) ───────────────────────────────
print("\n[3] Parquet Sıkıştırma Süreleri (Z2):")
for codec in ["snappy", "zstd", "gzip"]:
    hedef = f"{PARQUET_BASE}_{codec}"
    t_c = time.time()
    temiz.write.mode("overwrite").option("compression", codec).partitionBy("tarih").parquet(hedef)
    print(f"    - Codec: {codec:<6} | Yazma süresi: {time.time()-t_c:.2f} sn | Hedef: {hedef}")

# ── 4. Delta Lake Yazma ve Zaman Yolculuğu (Z7) ──────────────────────────────
print("\n[4] Delta Lake ve Zaman Yolculuğu (Z7):")
# Sürüm 0: İlk toplu yazma
temiz.write.format("delta").mode("overwrite").partitionBy("tarih").save(DELTA_TABLO)

# Sürüm 1: Örnek düzeltme / ek kayıt yazma (Time Travel kanıtı için)
duzeltme_satiri = temiz.limit(5).withColumn("siparis_id", F.concat(F.col("siparis_id"), F.lit("_REV")))
duzeltme_satiri.write.format("delta").mode("append").partitionBy("tarih").save(DELTA_TABLO)

spark.sql(f"DESCRIBE HISTORY delta.`{DELTA_TABLO}`").select("version", "timestamp", "operation").show(truncate=False)

v0_sayisi = spark.read.format("delta").option("versionAsOf", 0).load(DELTA_TABLO).count()
v1_sayisi = spark.read.format("delta").option("versionAsOf", 1).load(DELTA_TABLO).count()
print(f"    - Sürüm 0 satır sayısı: {v0_sayisi:,} | Sürüm 1 satır sayısı: {v1_sayisi:,}")

# ── 5. Boyut Tabloları ile JOIN ve Analiz Sorguları (Z5) ─────────────────────
print("\n[5] Toplu Analizler ve JOIN Sonuçları (Z5):")
delta_df = spark.read.format("delta").load(DELTA_TABLO)
urunler = spark.read.option("header", True).option("inferSchema", True).csv(HAM_URUN)
magazalar = spark.read.option("header", True).option("inferSchema", True).csv(HAM_MAGAZA)
baslangic_stok = spark.read.option("header", True).option("inferSchema", True).csv(HAM_STOK)

# Analiz 1: En çok ciro ve kâr getiren 10 ürün (Ürünler tablosu ile JOIN)
top_urunler = (delta_df.filter(F.col("tur") == "satis")
               .groupBy("urun_id")
               .agg(F.sum("adet").alias("toplam_satis_adet"),
                    F.round(F.sum(F.col("adet") * F.col("birim_fiyat")), 2).alias("toplam_ciro"))
               .join(F.broadcast(urunler), on="urun_id", how="left")
               .orderBy(F.desc("toplam_ciro")))

print("\n--- Analiz 1: En Çok Ciro Yapan İlk 10 Ürün (Broadcast JOIN) ---")
top_urunler.explain(mode="formatted")
t_q1 = time.time()
top_urunler.show(10, truncate=False)
print(f"Analiz 1 süresi: {time.time()-t_q1:.2f} sn")

# Analiz 2: Mağaza ve Şehir Bazında Satış ve İade Oranları
magaza_analiz = (delta_df.groupBy("magaza_id")
                 .agg(
                     F.sum(F.when(F.col("tur") == "satis", F.col("adet")).otherwise(0)).alias("satis_adet"),
                     F.sum(F.when(F.col("tur") == "iade", F.col("adet")).otherwise(0)).alias("iade_adet"),
                     F.round(F.sum(F.when(F.col("tur") == "satis", F.col("adet") * F.col("birim_fiyat")).otherwise(0)), 2).alias("net_satis_ciro")
                 )
                 .withColumn("iade_orani_yuzde", F.round((F.col("iade_adet") / F.col("satis_adet")) * 100, 2))
                 .join(F.broadcast(magazalar), on="magaza_id", how="left")
                 .orderBy(F.desc("net_satis_ciro")))

print("\n--- Analiz 2: Mağaza Bazlı Ciro ve İade Oranları ---")
magaza_analiz.show(10, truncate=False)

# Analiz 3: Net Güncel Stok Hesabı (Başlangıç + Stok Girişi + İade - Satış)
stok_hareket = (delta_df.groupBy("urun_id")
                .agg(
                    F.sum(F.when(F.col("tur") == "stok_girisi", F.col("adet")).otherwise(0)).alias("giris"),
                    F.sum(F.when(F.col("tur") == "iade", F.col("adet")).otherwise(0)).alias("iade"),
                    F.sum(F.when(F.col("tur") == "satis", F.col("adet")).otherwise(0)).alias("satis")
                ))

guncel_stok = (baslangic_stok.join(stok_hareket, on="urun_id", how="left").fillna(0)
               .withColumn("guncel_stok", F.col("baslangic_stok") + F.col("giris") + F.col("iade") - F.col("satis"))
               .join(F.broadcast(urunler.select("urun_id", "kategori", "marka")), on="urun_id", how="left")
               .orderBy(F.asc("guncel_stok")))

print("\n--- Analiz 3: Stoğu En Düşük (Kritik) 10 Ürün ---")
guncel_stok.show(10, truncate=False)

# ── 6. Anahtar Çarpıklığı (Skew) Kontrolü (Z3) ────────────────────────────────
print("\n[6] Anahtar Çarpıklığı (Skew) Analizi (Z3):")
urun_dagilim = delta_df.groupBy("urun_id").count().orderBy(F.desc("count"))
en_yogun = urun_dagilim.first()
ortalama_pay = v1_sayisi / 500.0
print(f"    - En yoğun urun_id={en_yogun['urun_id']} | Kayıt: {en_yogun['count']:,} | Toplam pay: %{100*en_yogun['count']/v1_sayisi:.2f}")
print(f"    - Ortalama ürün başına kayıt: {ortalama_pay:.1f} | Çarpıklık oranı (Max/Ort): {en_yogun['count']/ortalama_pay:.2f}x")

spark.stop()
