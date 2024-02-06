# %%
import sys
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_date, DataFrame
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

job.init(args["JOB_NAME"], args)
s3 = boto3.client('s3')

PATIENT_DATABASE = "cmsdesynpuf1k"
VOCABULARY_DATABASE = "vocab-omop"

# %%

def apply_sql_script(
    table_out_name: str,
    sql_script_name: str
    ) -> DataFrame:
    sql_script = load_sql_script(sql_script_name)
    df = spark.sql(sql_script)
    df.createOrReplaceTempView(table_out_name)
    return df

def create_medical_data_view(
    table_id: str,
    database_name: str = PATIENT_DATABASE,
    ) -> DataFrame:
    df = load_table_to_df(
        table_name=f"cdm_{table_id}_csv_bz2",
        database_name=database_name,
    )
    df = cast_date_columns(df)
    df.createOrReplaceTempView(table_id)
    return df

def create_vocabulary_view(
    table_id: str,
    database_name: str = VOCABULARY_DATABASE,
    ) -> DataFrame:
    df = load_table_to_df(
        table_name=f"{table_id}_csv",
        database_name=database_name,
    )
    df.createOrReplaceTempView(table_id)
    return df 

def load_sql_script(name: str) -> str:
    s3_object = s3.get_object(Bucket='synth-medical', Key=f'scripts/{name}')
    return s3_object['Body'].read().decode('utf-8')    

def load_table_to_df(
    table_name: str,
    database_name: str,
    ) -> DataFrame:
    check_table_exists(table_name, database_name)
    df = glueContext.create_data_frame_from_catalog(
        database=database_name,
        table_name=table_name,
        transformation_ctx=f"load_{table_name}_DF",
        useSparkDataSource=True,
    )
    return df

def check_table_exists(table_name: str, database_name: str) -> str:
    glue_client = boto3.client("glue")
    matching_tables = glue_client.get_tables(
        DatabaseName=database_name,
        Expression=table_name
    )['TableList']
    
    if len(matching_tables) == 0:
        raise ValueError(f"Table {table_name} not found")
    if len(matching_tables) > 1:
        raise ValueError(f"Multiple tables found for {table_name}")

def cast_date_columns(df: DataFrame) -> DataFrame:
    """Transform columns ending in '_date' from string to date"""
    columns_out = []
    for c in df.columns:
        if c.endswith("_date"):
            c_out = to_date(col(c), 'yyyyMMdd').alias(c)
        else:
            c_out = col(c)
        columns_out.append(c_out)
    return df.select(columns_out)

# %%

create_medical_data_view("death")
create_medical_data_view("person")
create_medical_data_view("condition_occurrence")
create_medical_data_view("drug_exposure5_2_2")
create_medical_data_view("observation_period")

create_vocabulary_view("icd10-categories")
create_vocabulary_view("atc-categories")

# %%

apply_sql_script("cohort", "select-cohort.sql")
apply_sql_script("features", "add-features.sql")
data = apply_sql_script("data", "map-features-to-categories.sql")

# %%

train_data, test_data = data.randomSplit([0.9, 0.1])

# %%

(train_data.write.format("parquet").option("compression", "gzip")
 .mode("overwrite").save("s3://synth-medical/data/train/"))
(test_data.write.format("parquet").option("compression", "gzip")
 .mode("overwrite").save("s3://synth-medical/data/test/"))

# %%

job.commit()