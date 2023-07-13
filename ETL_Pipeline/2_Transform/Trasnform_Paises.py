# Importando as bibliotecas necessárias
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Lendo o arquivo parquet e armazenando os dados em um DataFrame
df_pais = pd.read_parquet('../SOFTPLAN/Data/Raw_Zone/Paises/paises_raw.parquet')

# Definindo a função para tratar o DataFrame
def tratamento_base(data):
    # Removendo colunas específicas do DataFrame
    pais_df = data.drop(columns = ['region', 'adminregion', 'incomeLevel','lendingType']) 
    # Aplicando uma função lambda para extrair o valor de 'value' em colunas específicas e armazenando em uma lista
    a = [df_pais[i].apply(lambda x: x['value']) for i in df_pais[['region', 'adminregion', 'incomeLevel','lendingType']]]   
    # Concatenando os DataFrames na lista ao longo do eixo das colunas
    info_df = pd.concat(a, axis = 1)
    df = pd.concat([pais_df, info_df], axis = 1)
    
    return df

df = tratamento_base(df_pais)

df_staging = df[['id', 'name', 'longitude', 'latitude', 'region']]

# Renomeando as colunas do DataFrame
df_staging.columns = ['ID_PAIS','NAME','LONGITUDE','LATITUDE','REGION']

#Removendo Países que tem a região 'Aggregates'
df_staging = df_staging.loc[df_staging['REGION'] != 'Aggregates']


# Definindo a função para salvar o DataFrame em um arquivo parquet
def staging_saved_file_paises(data):
    # Convertendo o DataFrame do pandas em uma tabela PyArrow
    tb_consumezone_indicador = pa.Table.from_pandas(data)
    
    # Escrevendo a tabela em um arquivo parquet
    return pq.write_table(tb_consumezone_indicador, '../SOFTPLAN/Data/Staging_Zone/Paises/paises_transformed.parquet')

# Salvando o DataFrame em um arquivo parquet
df_paisess = staging_saved_file_paises(df_staging)

print("DataFrame salvo com sucesso em formato parquet.")
