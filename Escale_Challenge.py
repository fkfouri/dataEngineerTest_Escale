#!/usr/bin/env python
# coding: utf-8

# # Desafio Escale
# 
# Este <i>notebook</i> traz a resolução do desafio proposto pela Escale (https://escaletech.github.io/dataplatform/data-engineer-test).
# 
# Foram criadas diversas funções, para permitir a construção de uma <i>storytelling</i> clara e objetiva.

# ## Setup
# 
# Instalação e importação de bibliotecas.

# In[1]:


try:
    get_ipython().system('pip install pyspark=="2.4.5" --quiet')
    get_ipython().system('pip install pandas=="1.0.4" --quiet')
except:
    print("Running throw py file.")


# In[2]:


from pyspark import SparkContext as sc
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark import SparkFiles
from pyspark.sql.types import StringType, FloatType
import pyspark
import json
import pandas as pd
import numpy as np


# Criação de uma sessão Spark

# In[3]:


spark = SparkSession        .builder        .appName("Desafio Data Engineer Escale - Fabio Kfouri")        .getOrCreate()
spark


# Para otimizar a resolução, foram realizados downloads dos datasets para máquina local.
# 
# Esta lógica é para identificar se esta solução esta rodando na máquina do autor, caso positivo, utilizará o dataset local, do contrário, utilizará o dataset da núvem.
# 
# Definido também a quantidade de arquivos do DataSet. A intenção do Autor é rodar a aplicação na AWS, porém, para não gerar custo desnecessario, será processado somente um arquivo do DataSet, do contrario, processará todos.

# In[4]:


import os

dataPath = 'https://d3l36jjwr70u5l.cloudfront.net/data-engineer-test/'
outPath = 's3://data-sprints-fk/output/'
quantity_of_datasets = 1

if 'E:\\' in os.getcwd() and 'dataEngineerTest_Escale' in os.getcwd():
    dataPath = os.getcwd() + "/data/"
    outPath = os.getcwd() + "/output/"
    quantity_of_datasets = 10

print(dataPath)
print(outPath)
print('Quantidade de Arquivos do Dataset', quantity_of_datasets)


# # Funções
# Funcão de leitura do Json que retorna um Datafame Spark

# In[5]:


def read_json(filename):
    return spark.read.json(dataPath + filename)


# Função que converte o 30 minutos em miliegundos para ser usado como referência de delta para data em formado de <b>Epoch Time</b>.

# In[6]:


def get_epoch_time_limit():
    return (30*60)*1000


# Função que cria o dataFrame <b>ClickStream</b>, que atende os criterios de sessionamento defindo pelo desafio.
# 
# Para calculo do tempo de sessão foi utilizado a função <b>LAG()</b> que tem por objetivo trazer no registro corrente o dado de timestamp do registro anterior.
# 
# O autor assumiu como premissa não declarada para definir uma sessão, que além do tempo limite de 30 minutos desde a última utilização, que uma sessão precisaria considerar o <b>device_family</b> e <b>os_family</b>.
# 
# Ou seja, mesmo que não tenha excedido o tempo limite de 30 minutos, mas se for caracterizado que houve uma mudança de device_family ou os_family, trata-se de uma sessão nova.
# 
# Para a criação das Sessions_ID foi usado as funções ROW_NUMBER() para criacão ordenada de ids, e em seguida utilizada a função LAST_VALUE() para o preenchimento do session_id nos eventos da mesma sessão.

# In[7]:


def get_clickstream_dataframe(df):
    overCategory = Window.partitionBy('anonymous_id','device_family','os_family','browser_family')                         .orderBy('anonymous_id','device_family','os_family','browser_family','device_sent_timestamp')
    
    time_limit = get_epoch_time_limit()
    
    df = df.withColumn("lag", F.lag('device_sent_timestamp', 1).over(overCategory))            .withColumn('delta_seg', (F.col('device_sent_timestamp') - F.col('lag'))/1000)            .withColumn('same_section', (F.col('device_sent_timestamp') - F.col('lag')) < time_limit)             .withColumn('event_time', (F.col('device_sent_timestamp')/1000).cast('timestamp'))
    
    df.createOrReplaceTempView("raw_table")
    
    df_sessioned = spark.sql("""
        with temp as (--
              SELECT anonymous_id, browser_family, device_family, device_sent_timestamp, event, event_time, n, os_family, --
                     platform, nvl(same_section,false) same_section, version,
                     case when nvl(same_section,false) = false then
                         0
                     else
                        delta_seg
                     end delta_seg,             
                     case when nvl(same_section,false) = false then
                     'session_' || ROW_NUMBER() OVER (PARTITION BY anonymous_id,nvl(same_section,false) ORDER BY device_sent_timestamp )
                     else
                      null
                     end
                     as partial_session
                FROM raw_table t --
        ), table_section_id as (--
            select anonymous_id, browser_family, delta_seg, device_family, device_sent_timestamp, event, event_time, n, os_family, --
                     platform, nvl(same_section,false) same_section, version,
                     LAST_VALUE(partial_session,True) OVER (PARTITION BY anonymous_id ORDER BY device_sent_timestamp ) || '_' || anonymous_id session_id
            from temp t
        )
        select * from table_section_id
        order by anonymous_id, device_sent_timestamp 
        """)
        
    df_sessioned.createOrReplaceTempView("clickstream")
    
    #Remove the view raw_table
    spark.catalog.dropTempView('raw_table')


# Funcao que convert as saídas da Questão 2 no formato desejado pelo Desafio.
# 
# Esta função cria um dicionário que de forma incremental, vai adicionando as quantidades conforme forem sendo carregados novos datasets.

# In[8]:


def convert_to_Json_challenge2(dados_pd, question):   
    for index, row in dados_pd.iterrows():
        items = str(row["collection"]).split(',')

        if not row["what"] in question:
            question[row["what"]] = {}

        for item in items:
            element = str(item).split(':')

            if not element[0] in question[row["what"]]:
                question[row["what"]][element[0]] = 0

            if len(element) > 1:
                question[row["what"]][element[0]] = float(element[1]) + question[row["what"]][element[0]]
            else:
                question[row["what"]][element[0]] = float(0) + question[row["what"]][element[0]]

                
    return question


# Funcao que convert as saídas da Questão 3 no formato desejado pelo Desafio.
# 
# Esta função cria um dicionário que de forma incremental, vai calculando a mediana das medianas conforme forem sendo carregados novos datasets.

# In[9]:


def convert_to_Json_challenge3(dados_pd, question):   
    for index, row in dados_pd.iterrows():

        items = str(row["collection"]).split(',')

        if not row["what"] in question:
            question[row["what"]] = {}

        for item in items:
            element = str(item).split(':')

            if not element[0] in question[row["what"]]:
                question[row["what"]][element[0]] = 0


            if len(element) > 1:
                question[row["what"]][element[0]] = np.median([ float(element[1]), question[row["what"]][element[0]] ])
            else:
                question[row["what"]][element[0]] = question[row["what"]][element[0]]

            
    return question


# # Desafio 1
# 
# Calcular a quantidade total de sessões únicas por arquivo do conjunto de dados e apresentar no formato JSON.
# 
# Nesta função foi realizado um filtro para desconsiderar as sessoes abertas para descobrir o total de sessoes únicas por dataset.

# In[10]:


def first_challenge(filename, question1):
    df_question_1 = spark.sql("""
        SELECT COUNT(session_id) qtd_session
        FROM clickstream
        where same_section = false
        """)
    question1[filename] = df_question_1.collect()[0]['qtd_session']
    return question1


# # Desafio 2
# Calcular a quantidade de sessões únicas que ocorreram em cada Browser, Sistema Operacional e Dispositivo dentro de todo o conjunto de dados.
# 
# Nesta função foi identificado as famílias (<b>browser_family</b>, <b>os_family</b> e <b>device_family</b>) e em seguida por quantidade de tipo de cada família. 
# 
# Estáo sendo consideradas somente as sessões únicas.

# In[11]:


def second_challenge(question2):
    df_question_2 = spark.sql("""
        with table_temp as (--
            SELECT *
            FROM clickstream
            where same_section = false
        ), 
        table_union as (--
            SELECT 'device_family' what, device_family ref, COUNT(session_id)  qtd_session
              FROM table_temp
             group by device_family
            union
            SELECT 'os_family' what, os_family ref, COUNT(session_id)  qtd_session
              FROM table_temp
            group by os_family
            union
            SELECT 'browser_family' what, browser_family ref, COUNT(session_id)  qtd_session
              FROM table_temp
             group by browser_family --
        ),
        table_collection as (--
            select what, nvl(ref, 'Not Identified') || ':' || qtd_session collection 
            from table_union
            order by what, ref--
        )
        select what, array_join(collect_list(trim(collection)),',')  collection
        from table_collection
        group by what
        """)
    
    dados2 = df_question_2.toPandas()
    return convert_to_Json_challenge2(dados2, question2)


# # Desafio 3
# 
# Calcular a mediana da duração (em segundos) entre todas sessões únicas para cada segmento.
# 
# De forma análoga ao desafio 2, esta função foram identificado as quantidades por família (<b>browser_family</b>, <b>os_family</b> e <b>device_family</b>).
# 
# Estáo sendo consideradas somente as sessões que ficaram abertas.
# 
# 
# ### Observação sobre o cálculo da Mediana
# Bem, a solicitação da Mediana requer na prática que todas as durações de um segmento fossem ordenadas, para assim poder realizar a escolha do ponto central. O que na prática parece ser inviável por se tratar de um Streaming, ou seja, o dataset em um ambiente de produção crescerá ao longo do tempo.
# 
# O Autor resolveu este problema da seguinte forman:
# - Calcular a mediana de cada segmento por arquivo do dataSet. 
# - Cada vez que um novo arquivo era processado, uma nova mediana seria calculada.
# - E usando o numpy, calculava-se a mediana das medianas, ou seja, o resultado do dataSet anterior com o resultado do Dataset atual.
# 
# Bem, a mediana neste caso não é precisa, porém para o Author, o resultado calculado atende em ordem de grandeza/aproximação a proposta do desafio. 

# In[12]:


def third_challenge(question3):
    df_question_3 = spark.sql("""
        with table_temp as (--
            SELECT *
            FROM clickstream
            where same_section = true
        ), 
        table_union as (--
            SELECT 'device_family' what, device_family ref, percentile_approx(delta_seg , 0.5) median   
              FROM table_temp
             group by device_family
            union
            SELECT 'os_family' what, os_family ref, percentile_approx(delta_seg , 0.5) median 
              FROM table_temp
            group by os_family
            union
            SELECT 'browser_family' what, browser_family ref, percentile_approx(delta_seg , 0.5) median 
              FROM table_temp
             group by browser_family --
        ),
        table_collection as (--
            select what, nvl(ref, 'Not Identified') || ':' || median collection 
              from table_union
             order by what, ref--
        )
        select what, array_join(collect_list(trim(collection)),',')  collection
          from table_collection
        group by what
        """)
    
    dados3 = df_question_3.toPandas()
    return convert_to_Json_challenge3(dados3, question3)


# # Aplicação
# Função que realiza o processamento de um dataset por vez.
# - Leitura do arquivo
# - criação do ClickStream (sessionamento)
# - dispara os algorítimos da primero, segundo e terceiro desafio
# - retorna os resultados dos desafios

# In[13]:


def process_dataset(filename, questions):
    question1, question2, question3 = questions
    df = read_json(filename)
    get_clickstream_dataframe(df)
    question1 = first_challenge(filename, question1)
    question2 = second_challenge(question2)
    question3 = third_challenge(question3)
    
    return question1, question2, question3


# Algorítimo que realiza a leitura em loop de todos os arquivos.

# In[14]:


from datetime import datetime 

questions = {}, {}, {}

for i in range(quantity_of_datasets):
    filename = 'part-0000' + str(i) +'.json.gz'
    print(filename, 'Start at', datetime.today())
    questions = process_dataset(filename, questions)

question1, question2, question3 = questions

print('Finished at', datetime.today())


# # Resultados
# 
# O primeiro resultado possui a quantidade de sessões unicas por arquivo de conjunto de dados em formato Json.

# In[15]:


print(question1)


# O segundo resultado possui a quantidade de sessões únicas que ocorreram em cada Browser, Sistema Operacional e Dispositivo.

# In[16]:


print(question2)


# O terceiro resultado possui o cálculo da mediana da duração em segundos entre todas as sessões únicas. 

# In[17]:


print(question3)


# Criação de arquivos tipo json na pasta output.

# In[18]:


def save_file(filename, content):
    if not os.path.exists(outPath):
        os.makedirs(outPath)
    with open(outPath+ "/" + filename, "w") as outfile:  
        json.dump(content, outfile) 


# In[19]:


save_file('question1.json', question1)
save_file('question2.json', question2)
save_file('question3.json', question3)


# In[ ]:




