import os
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd 

def consumezone_saved_file_indicadores(data):
    tb_consumezone_indicador = pa.Table.from_pandas(data)
    return pq.write_table(tb_consumezone_indicador, '../SOFTPLAN/DataSets_Tratamentos/Consume_Zone/Indicadores/indicadores.parquet')


file_indicadores_rz = '../SOFTPLAN/DataSets_Tratamentos/Raw_Zone/Indicadores/indicadores.parquet'

if os.path.isfile(file_indicadores_rz):
    df_indicadores_rz = pd.read_parquet(file_indicadores_rz)
    df_indicadores = consumezone_saved_file_indicadores(df_indicadores_rz)
    print("DataFrame salvo com sucesso em formato parquet.")
else:
    print(f"File {file_indicadores_rz} not found!")


