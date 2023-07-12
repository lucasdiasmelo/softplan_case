import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq



# Carregar o arquivo Parquet no DataFrame
df_find_paises = pd.read_parquet('../SOFTPLAN/DataSets_Tratamentos/Raw_Zone/Valores/valores.parquet')

# Aplicar a função pivot_table para transformar os indicadores em colunas
def tratamento_base_fIndPaises(data):
    df_pivot = df_find_paises.pivot_table(index=['Country', 'Date'], columns='Indicator', values='Value', aggfunc='mean').reset_index()
    df_pivot.columns = ['ID_PAIS', 'ANO', 'PIB', 'GINI'] # Renomear as colunas de acordo com o formato desejado
    return df_pivot

df = tratamento_base_fIndPaises(df_find_paises)

def consumezone_saved_file_find_Paises(data):
    tb_consumezone_valores = pa.Table.from_pandas(data)
    return pq.write_table(tb_consumezone_valores, '../SOFTPLAN/DataSets_Tratamentos/Consume_Zone/Valores/valores.parquet')

df_find_pais = consumezone_saved_file_find_Paises(df)
print("DataFrame salvo com sucesso em formato parquet.")


