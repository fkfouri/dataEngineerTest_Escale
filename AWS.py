#!/usr/bin/env python
# coding: utf-8

# # Sequencia para processamento em Cluster AWS
# 
# Todo o código foi baseado na biblioteca boto3. Para executar é necessário ter na máquina configurada as credencias da AWS conforme descrito no link https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html. 

# In[1]:


try:
    get_ipython().system('pip install boto3=="1.13.1" --quiet')
except:
    print("Running throw py file.")


# In[2]:


import boto3
import os
import json


# In[3]:


dirpath = os.getcwd()


# ## Configurando serviços AWS
# Sequencia de atividads para configuração de ambiente AWS para armazenamento e processamento do modelo PySpark.

# #### Definindo Variáveis usados na configuração de ambiente AWS.

# In[4]:


my_bucket = "escale-fk"
app_key = "escale-test-fk"
my_tag = [{'Key': app_key, 'Value': ''}]
my_resource_group = "rg-escale-test-fk"
my_emr_cluster = "spark-escale-test-fk"
files_to_upload = ['Escale_Challenge.py', 'Escale_Challenge.html']


# ### Criação de um Bucket S3 "data-sprints-fk" para armazenamento do modelo PySpark.

# In[5]:


s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
s3_client.create_bucket(Bucket=my_bucket)


# Definição de uma TAG para o Bucket criado

# In[6]:


s3_client.put_bucket_tagging(Bucket=my_bucket, Tagging= {'TagSet': my_tag} )


# Upload do modelo para o bucket na pasta model.

# In[7]:


for file in files_to_upload: 
    file_name = dirpath + "/" + file
    try:
        if '.html' in file_name:
            response = s3_client.upload_file(file_name, my_bucket, file)
            
            
            #Modificando o ContentType
            object = s3.Object(my_bucket, file)
            object.copy_from(CopySource={'Bucket': my_bucket, 'Key': file},
                             MetadataDirective="REPLACE",
                             ContentType="text/html",
                             ACL = 'public-read')

        else:
            response = s3_client.upload_file(file_name, my_bucket, "model/" + file, ExtraArgs={'ACL':'public-read', })
            
        print("It was uploaded the file", "'" + file + "'", ".")
    except ClientError as e:
        logging.error(e)


# Abrir em um Browser o site: https://escale-fk.s3.amazonaws.com/Escale_Challenge.html

# In[ ]:





# ### Configuração de um Resource Group

# In[8]:


RG_client = boto3.client('resource-groups')

#AWS::AllSupported
#AWS::S3::Bucket
query = {
    "ResourceTypeFilters": ["AWS::AllSupported"],
    "TagFilters":  [{
        "Key": my_tag[0].get("Key"),
        "Values": [""]
    }] 
}
resource_query = {
    'Type': 'TAG_FILTERS_1_0',
    'Query': json.dumps(query)
}

try:
    resp = RG_client.create_group(Name=my_resource_group,ResourceQuery=resource_query)
    print("Resource Group was created.")
except Exception as e:
    print(e)

#print(query)
#print(my_tag)


# ### Criação de um  EMR Cluster

# In[9]:


emr_client = boto3.client('emr') #region_name='us-east-1'

cluster_id = emr_client.run_job_flow(Name=my_emr_cluster, 
    ReleaseLabel='emr-5.30.1',
    LogUri='s3://' + my_bucket + '/log/',
    Applications=[
        {
            'Name': 'Spark'
        },
    ],
    Instances={
        'InstanceGroups': [
            {
                'Name': "Master",
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm4.large',
                'InstanceCount': 1,
            },
            {
                'Name': "Slave",
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': 'm4.large',
                'InstanceCount': 2,
            }
        ],
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
    },
    Steps=[
        {
            'Name': 'Spark application',   
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ["spark-submit","--deploy-mode","cluster","s3://" + my_bucket + "/model/" + files_to_upload[0]]
                    }
        }        
    ],                                    
    VisibleToAllUsers=True,
    JobFlowRole='EMR_EC2_DefaultRole',
    ServiceRole='EMR_DefaultRole',
    Tags=my_tag
)


# In[10]:


from datetime import datetime, date

clusters = emr_client.list_clusters(
        CreatedAfter = datetime.today()
)
my_cluster = [i for i in clusters['Clusters'] if i['Name'] == my_emr_cluster][0]
my_cluster


# In[11]:


import time

response = emr_client.describe_cluster(ClusterId = my_cluster['Id'])
print('The current state is', response['Cluster']['Status']['State'], '-', datetime.today())
i = 0

while response['Cluster']['Status']['State'] != 'TERMINATED' and i < 30:
    response = emr_client.describe_cluster(ClusterId = my_cluster['Id'])
    print('The current state is', response['Cluster']['Status']['State'], '-', datetime.today(), i)
    i += 1
    time.sleep(60)


# ## Desativação/Remoção das configurações da AWS

# Remoção do Resource Group

# In[12]:


RG_client.delete_group(GroupName=my_resource_group)


# Remoção de todos os arquivos do Bucket

# In[13]:


bucket = s3.Bucket(my_bucket)
bucket.objects.all().delete()


# Remoção do Bucket

# In[14]:


s3_client.delete_bucket(Bucket=my_bucket)


# In[ ]:





# In[ ]:




