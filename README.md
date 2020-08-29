# Teste de Engenheiro de dados para Escale

Este material é parte do processo de seleção para Engenheiros de Dados na Escale (https://escaletech.github.io/dataplatform/data-engineer-test). 

Para este desafio foram disponibilizados um conjuntos de dados particionados por 10 arquivosm contendo dados sobre o comportamento (eventos) do usuário em um site ou aplicativo.
- sequência de cliques;
- quantidade de tempo que o usuário na aplicação;
- locais onde começa a navegação e como termina.

Este repositório contém a resolução dos desafios propostos.

## Instruções para Reprodução de Análises (Processamento Local)
Para reprodução local, é necessario o Python ^3.5  e o Spark ^2.4.5.

Na máquina do autor (i7 16Gb) o processamento de cada arquivo levou aproximadamente 30 minutos, ou seja, o dataset todo levou quase 5 horas para processar.

Na root deste repositorio, execute o seguinte comando para execução dos desafios:

```python Escale_Challenge.py```

Será criado uma pasta '/output' com os resultados das Questões 1, 2 e 3.

Para visualização e reprodução de cada passo do desafio, abrir o arquivo **Escale_Challenge.ipynb** através do Jupyter Notebook.

```jupyter notebook Escale_Challenge.ipynb```

Obviamente houve um estudo e uma preparação prévia no DataSet para cumprir o desafio. O passo a passo de preparação e as hipoteses de resolução estão no arquivo **Escale_study.ipynb**, para visualizar utilize o Jupyter Notebook.

```jupyter notebook Escale_study.ipynb```


## Processamento em Cluster AWS 
O arquivo "AWS.ipynb" contém uma sequencia de configuração dos servidos da Amazon (S3, Group Resource, EMR) para a criação de um cluster Spark On-Demand formado por um Master e dois Slaves.

A execução deste script requer que o Engenheiro de Dados possua uma conta ativa na AWS e que possua configurado localmente o AWS CLI. 

Esta execução pode causar COBRANÇA da AWS pelo uso dos serviços. É necessario observar o tempo de duração dos serviços para não causar prejuízo. 

Para efeito de referencia, durante os meus testes, a configuração e a utilização do cluster levou no melhor tempo 40 minutos e no pior tempo cerca de 50 minutos para o processamento de um único arquivo do DataSet.

Foi criado um step no cluster da EMR para a execução do Script Python. 

Para a execução, rode o seguinte comando:

```python aws.py```

**O código aws.py além de criar todos os serviços necessários para execução, ao final, também remove todos os serviços. Sugere-se avaliar os passos através do Jupyter Notebook.**

Para visualização e reprodução de cada passo do desafio, abrir o arquivo desafio.ipynb através do Jupyter Notebook.

```jupyter notebook AWS.ipynb```
