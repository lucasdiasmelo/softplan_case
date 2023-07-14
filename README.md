# softplan_case

# Projeto de Pipeline de ETL

Este projeto consiste em um pipeline de ETL (Extração, Transformação e Carregamento) que extrai dados de uma API e transforma os dados conforme as necessidades do desafio e os carrega em um arquivo .parquet.

## Pré-requisitos

* Python 3.8+
* pandas
* pyarrow
* requests
* numpy

Para instalar todas as dependências necessárias, execute:

pip install -r requirements.txt

## Ordem de execução dos scripts que estão na pasta ETL_Pipeline:

1 - ETL_Pipeline\1_Extract\Extract_Paises.py 
## Esse Script é reponsável por realizar a primeira requisição do Desafio.

2 - ETL_Pipeline\2_Transform\Transform_Paises.py 
## Esse Script é reponsável por realizar a tranformação dos dados coletados da API (Paises).

3 - ETL_Pipeline\3_Load\Load_Paises.py
## Esse Script é responsável por carregar os dados prontos para consumo na camada Consume_Zone(paises_final.parquet).

4 - ETL_Pipeline\1_Extract\Extract_Valores.py 
## Esse Script é reponsável por realizar a segunda requisição do Desafio, com base nos valores da primeira requisição.

5 - ETL_Pipeline\2_Transform\Transform_Valores.py 
## Esse Script é reponsável por realizar a tranformação dos dados coletados da API (Valores).

6 - ETL_Pipeline\3_Load\Load_Valores.py 
## Esse Script é responsável por carregar os dados prontos para consumo na camada Consume_Zone(valores_final.parquet).


