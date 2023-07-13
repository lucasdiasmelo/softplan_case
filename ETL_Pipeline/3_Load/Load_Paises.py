import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Função que salva um arquivo parquet na camada Consume_Zone
def load_consume_zone(data):
    tb_consumezone_indicador = pa.Table.from_pandas(data)
    return pq.write_table(tb_consumezone_indicador, '../SOFTPLAN/Data/Consume_Zone/Paises/paises_final.parquet')

# Carregar o arquivo parquet transformado
df_pais_transformed = pd.read_parquet('../SOFTPLAN/Data/Staging_Zone/Paises/paises_transformed.parquet')

#Chama a função e salva o arquivo .parquet na camada de consumo
load_consume_zone(df_pais_transformed)
print("DataFrame salvo com sucesso em formato parquet.")
