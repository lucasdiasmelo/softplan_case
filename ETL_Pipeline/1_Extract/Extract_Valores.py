# Importando bibliotecas necessárias
import requests
import pandas as pd 
import concurrent.futures
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

# Definição de função para coletar dados de indicadores de uma API
def transform_valores(pais_id, indicator_code):
    url = f"https://api.worldbank.org/v2/country/{pais_id}/indicator/{indicator_code}?format=json"
    all_data = []

    page = 1
    while True:
        # Fazendo requisição à API
        response = requests.get(url, params={"page": page})
        response.raise_for_status()
        data = response.json()


        if not data or len(data) < 2 or not data[1]:
            break

        # Adicionando dados à lista
        all_data.extend(data[1])

        # Passando para a próxima página de dados
        page += 1

    return all_data

# Função para coletar dados de valores para vários países
def dados_indicadores_geral(pais_id, indicator_codes):
    all_data = []

    # Usando multithreading para fazer requisições simultâneas
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(transform_valores, country, indicator)
            for country in pais_id
            for indicator in indicator_codes
        ]

        for future in concurrent.futures.as_completed(futures):
            try:
                data = future.result()
                if data:
                    all_data.extend(data)
            except Exception as e:
                print(f"Aconteceu um erro enquanto : {str(e)}")

    return all_data


def main():
    # Lendo arquivo parquet e armazenando os dados em um DataFrame
    df_paises = pd.read_parquet('../SOFTPLAN/Data/Consume_Zone/Paises/paises_final.parquet')
    # Removendo linhas onde 'REGION' é 'Agregates'
    df_paises = df_paises[df_paises['REGION'] != 'Agregates']
    # Convertendo a coluna 'ID_PAIS' para uma lista
    pais_id = df_paises['ID_PAIS'].tolist()

    # Lista de códigos de indicadores que serão coletados
    indicator_codes = ["NY.GDP.MKTP.CD", "SI.POV.GINI"]

    # Coletando dados de indicadores
    data = dados_indicadores_geral(pais_id, indicator_codes)

    # Verificando se existem dados
    if not data:
        print("Nenhum dado encontrado.")
        return

    extracted_data = []
    for country in pais_id:
        for indicator in indicator_codes:
            # Filtrando os dados pelo país e indicador atual
            indicator_data = [entry for entry in data if entry.get("countryiso3code") == country and entry.get("indicator", {}).get("id") == indicator]
            if indicator_data:
                for entry in indicator_data:
                    date = entry.get("date", "")
                    value = entry.get("value", "")

                    if date and value:
                        # Adicionando os dados coletados à lista
                        extracted_data.append({
                            "Country": country,
                            "Indicator": indicator,
                            "Date": date,
                            "Value": value
                        })
            else:
                # Se não existem dados, adiciona uma entrada vazia à lista
                extracted_data.append({
                    "Country": country,
                    "Indicator": indicator,
                    "Date": "",
                    "Value": ""
                })

    # Convertendo a lista de dados em um DataFrame
    df = pd.DataFrame(extracted_data)
    
    # Substituir valores vazios por NaN
    df.replace('', np.nan, inplace=True)
    
    # Salvando o DataFrame em um arquivo parquet na camada raw
    df.to_parquet('../SOFTPLAN/Data/Raw_Zone/Valores/valores_raw.parquet') #editar a pasta para consume_zone
    print("DataFrame salvo com sucesso em formato parquet.")



if __name__ == "__main__":
    main()
