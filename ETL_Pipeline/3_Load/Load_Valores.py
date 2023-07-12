import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


df_valores = pd.read_parquet('../SOFTPLAN/Data/Staging_Zone/Valores/valores_transformed.parquet')

def consumezone_saved_file_find_Paises(data):
    tb_consumezone_valores = pa.Table.from_pandas(data)
    return pq.write_table(tb_consumezone_valores, '../SOFTPLAN/Data/Consume_Zone/Valores/valores_final.parquet')

df_save_valores = consumezone_saved_file_find_Paises(df_valores)
print("DataFrame salvo com sucesso em formato parquet.")