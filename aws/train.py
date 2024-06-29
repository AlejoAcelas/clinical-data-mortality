# %%
import sagemaker
import boto3

role_name = 'AmazonSageMaker-ExecutionRole-20240201T143680'
iam = boto3.client('iam')

role = iam.get_role(RoleName=role_name)['Role']['Arn']
region = 'us-east-1'
bucket = 'synth-medical'
prefix = 'model'

# %%


from sagemaker.debugger import Rule, ProfilerRule, rule_configs
from sagemaker.session import TrainingInput

s3_output_location='s3://{}/{}/{}'.format(bucket, prefix, 'xgboost_model')

container=sagemaker.image_uris.retrieve("xgboost", region, "1.2-1")
print(container)

xgb_model=sagemaker.estimator.Estimator(
    image_uri=container,
    role=role,
    instance_count=1,
    instance_type='ml.m4.xlarge',
    volume_size=5,
    output_path=s3_output_location,
    sagemaker_session=sagemaker.Session(),
    rules=[
        Rule.sagemaker(rule_configs.create_xgboost_report()),
        ProfilerRule.sagemaker(rule_configs.ProfilerReport())
    ]
)

# %%

xgb_model.set_hyperparameters(
    max_depth = 5,
    eta = 0.2,
    gamma = 4,
    min_child_weight = 6,
    subsample = 0.7,
    objective = "binary:logistic",
    num_round = 10
)

# %%

from sagemaker.session import TrainingInput


data = TrainingInput(
    "s3://synth-medical/data/part-00000-258c44d7-1412-4e42-ae69-78c853b454b5-c000.snappy.parquet",
    content_type="parquet",
    compression="snappy",
)

# %%

xgb_model.fit({"train": data, "validation": data}, wait=True)

# %%