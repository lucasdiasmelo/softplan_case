import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

df_pais = pd.read_parquet('../SOFTPLAN/DataSets_Tratamentos/Raw_Zone/Paises/paises.parquet')

def tratamento_base(data):
    pais_df = data.drop(columns = ['region', 'adminregion', 'incomeLevel','lendingType']) 
    a = [df_pais[i].apply(lambda x: x['value']) for i in df_pais[['region', 'adminregion', 'incomeLevel','lendingType']]]   
    info_df = pd.concat(a, axis = 1)
    df = pd.concat([pais_df, info_df], axis = 1)
    return df

df = tratamento_base(df_pais)

def consumezone_saved_file_pais(data):
    tb_consumezone_indicador = pa.Table.from_pandas(data)
    return pq.write_table(tb_consumezone_indicador, '../SOFTPLAN/DataSets_Tratamentos/Consume_Zone/Paises/paises.parquet')

df_paises = consumezone_saved_file_pais(df)

