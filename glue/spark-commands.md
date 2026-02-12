# Run these to check if our setup is working 

# 1. Become the "Root" user for the terminal
export AWS_ACCESS_KEY_ID=testing
export AWS_SECRET_ACCESS_KEY=testing
export AWS_REGION=us-east-1

# 2. Register your Glue User in Moto
# This will create a new user in Moto and give it an access key and secret key
```
aws iam create-user --user-name glue_user --endpoint-url http://localhost:5001
aws iam create-access-key --user-name glue_user --endpoint-url http://localhost:5001
```
# 3. Grant ALL permissions to glue_user directly
```
aws iam put-user-policy \
  --user-name glue_user \
  --policy-name OlakeFullAccess \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": "*",
        "Resource": "*"
      }
    ]
  }' \
  --endpoint-url http://localhost:5001
  ```

# 4. Create your S3 Bucket
# We use 'admin' keys here because they match your MinIO setup
export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=password
aws s3 mb s3://warehouse --endpoint-url http://localhost:9000

> you can also create bucket using localhost:9001


## Cell 1

```
import os
from pyspark.sql import SparkSession

# 1. CLEANUP
try:
    spark.stop()
except:
    pass

# 2. HOUSE B CONFIG (Glue / Moto)
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "<add generated access key here>"
os.environ["AWS_SECRET_ACCESS_KEY"] = "<add generated secret key here>"

catalog_name = "olake_glue"

# 3. INITIALIZE SPARK
spark = (SparkSession.builder
    .appName("Olake-Split-Auth-Test")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.iceberg:iceberg-aws-bundle:1.5.0")
    
    .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config(f"spark.sql.catalog.{catalog_name}.warehouse", "s3://warehouse/")
    
    # --- HOUSE B: GLUE CONFIG (Metadata) ---
    .config(f"spark.sql.catalog.{catalog_name}.glue.endpoint", "http://moto:5001")
    
    # --- HOUSE A: STORAGE CONFIG (Data) ---
    .config(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config(f"spark.sql.catalog.{catalog_name}.s3.endpoint", "http://minio:9000")
    .config(f"spark.sql.catalog.{catalog_name}.s3.access-key-id", "admin")
    .config(f"spark.sql.catalog.{catalog_name}.s3.secret-access-key", "password")
    
    # --- CRITICAL FIX: FORCE PATH STYLE ACCESS ---
    # This prevents 'UnknownHostException' by using 'minio:9000/bucket' 
    # instead of 'bucket.minio:9000'
    .config(f"spark.sql.catalog.{catalog_name}.s3.path-style-access", "true")
    
    .config("spark.sql.defaultCatalog", catalog_name)
    
    # --- HADOOP S3A CONFIG ---
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate())

print("✅ Spark Session is Active! (Path-Style Access Enabled)")
```


## Cell 2

```
spark.sql("CREATE NAMESPACE IF NOT EXISTS my_ns")
spark.sql("CREATE TABLE IF NOT EXISTS my_ns.my_table (id bigint, data string) USING iceberg")
print("✅ Table Created Successfully")
``` 

## Cell 3

```
# 1. Write Data (Uses 'admin' creds -> MinIO Port 9000)
spark.sql("INSERT INTO my_ns.my_table VALUES (1, 'Split Auth Works!'), (2, 'Separate Keys Success')")
print("✅ Data Written to S3...")

# 2. Read Data (Uses 'glue_user' -> Moto, then 'admin' -> MinIO)
print("👇 Reading Data:")
df = spark.sql("SELECT * FROM my_ns.my_table")
df.show()
```
