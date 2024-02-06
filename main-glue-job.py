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

person = create_medical_data_view("person")
create_medical_data_view("death")
create_medical_data_view("condition_occurrence")
create_medical_data_view("drug_exposure5_2_2")
create_medical_data_view("observation_period")

create_vocabulary_view("icd10_categories")
create_vocabulary_view("atc_categories")

# %%

cohort = apply_sql_script("cohort", "select-cohort.sql")
common_conditions = apply_sql_script("common_conditions", "select-common-conditions.sql")
common_drugs = apply_sql_script("common_drugs", "select-common-drugs.sql")

data = cohort.join(
    common_conditions.groupBy("person_id").pivot("condition_category").count(),
    on="person_id",
    how="left",
).join(
    common_drugs.groupBy("person_id").pivot("drug_category").count(),
    on="person_id",
    how="left",
).join(
    person.select(["person_id", "gender_concept_id", "year_of_birth", "race_concept_id"]),
    on="person_id",
    how="left",
).fillna(0)

# %%

train_data, test_data = data.randomSplit([0.9, 0.1], seed=42)

# %%
for name, df in [("train", train_data), ("test", test_data)]:    
    s3_data_sink = glueContext.getSink(
        path=f"s3://synth-medical/data/{name}/",
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=[],
        enableUpdateCatalog=True,
    )
    s3_data_sink.setCatalogInfo(
        catalogDatabase=PATIENT_DATABASE, catalogTableName=f"{name}_data"
    )
    s3_data_sink.setFormat("parquet", compression="None")
    s3_data_sink.writeDataFrame(df)


# %%

job.commit()

# https://stackoverflow.com/questions/35879372/pyspark-matrix-with-dummy-variables
# https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/api/pyspark.pandas.get_dummies.html