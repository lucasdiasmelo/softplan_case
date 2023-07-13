import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq



# Carregar o arquivo Parquet no DataFrame
df_pq_valores = pd.read_parquet('../SOFTPLAN/Data/Raw_Zone/Valores/valores_raw.parquet')

# Aplicar a função pivot_table para transformar os indicadores em colunas
def tratamento_valores(data):
    df_pivot = df_pq_valores.pivot_table(index=['Country', 'Date'], columns='Indicator', values='Value', aggfunc='mean').reset_index()
    df_pivot.columns = ['ID_PAIS', 'ANO', 'GDP', 'GINI'] # Renomear as colunas de acordo com o formato desejado
    return df_pivot

df = tratamento_valores(df_pq_valores)

# Salvando o DataFrame em um arquivo parquet na camada staging
def sataging_saved_file_valores(data):
    tb_consumezone_valores = pa.Table.from_pandas(data)
    return pq.write_table(tb_consumezone_valores, '../SOFTPLAN/Data/Staging_Zone/Valores/valores_transformed.parquet')

df_valores = sataging_saved_file_valores(df)
print("DataFrame salvo com sucesso em formato parquet.")


