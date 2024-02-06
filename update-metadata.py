# %%
import boto3

# %% Change dtype of columns ending with '_date' from 'bigint' to 'string'

glue_client = boto3.client('glue', region_name='us-east-1') 
database_name = 'cmsdesynpuf1k'
tables = glue_client.get_tables(DatabaseName=database_name)['TableList']

for table in tables:
    for column in table['StorageDescriptor']['Columns']:
        if column['Name'].endswith('_date'):
            column['Type'] = 'string'
    try:
        glue_client.update_table(DatabaseName=database_name, TableInput={
            'Name': table['Name'],
            'StorageDescriptor': table['StorageDescriptor'],
        })
        print(f"Table '{table['Name']}' in database '{database_name}' updated successfully.")
    except Exception as e:
        print(f"Error updating table: {e}")

# %%

# glue_client = boto3.client('glue', region_name='us-east-1') 
# database_name = 'cmsdesynpuf1k'
# tables = glue_client.get_tables(DatabaseName=database_name)['TableList']

# for table in tables:
#     print(table['Name'])

#     for column in table['StorageDescriptor']['Columns']:
#         if column['Name'].endswith('_date'):
#             column['Type'] = 'string'
#     try:
#         glue_client.update_table(DatabaseName=database_name, TableInput={
#             'Name': table['Name'],
#             'StorageDescriptor': table['StorageDescriptor'],
#             # 'PartitionKeys': table.get('PartitionKeys', []),
#         })
#         print(f"Table '{table['Name']}' in database '{database_name}' updated successfully.")
#     except Exception as e:
#         print(f"Error updating table: {e}")