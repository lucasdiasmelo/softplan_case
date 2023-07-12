import requests
import pandas as pd 
import concurrent.futures
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


def dados_indicadores(pais_id, indicator_code):
    url = f"https://api.worldbank.org/v2/country/{pais_id}/indicator/{indicator_code}?format=json"
    all_data = []

    page = 1
    while True:
        response = requests.get(url, params={"page": page})
        response.raise_for_status()
        data = response.json()

        if not data or len(data) < 2 or not data[1]:
            break

        all_data.extend(data[1])

        page += 1

    return all_data


def dados_indicadores_geral(pais_id, indicator_codes):
    all_data = []

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(dados_indicadores, country, indicator)
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
    df_paises = pd.read_parquet('../SOFTPLAN/Data/Consume_Zone/Paises/paises_final.parquet')
    df_paises = df_paises[df_paises['REGION'] != 'Agregates']
    pais_id = df_paises['ID_PAIS'].tolist()

    indicator_codes = ["NY.GDP.MKTP.CD", "SI.POV.GINI"]

    data = dados_indicadores_geral(pais_id, indicator_codes)

    if not data:
        print("Nenhum dado encontrado.")
        return

    extracted_data = []
    for country in pais_id:
        for indicator in indicator_codes:
            indicator_data = [entry for entry in data if entry.get("countryiso3code") == country and entry.get("indicator", {}).get("id") == indicator]
            if indicator_data:
                for entry in indicator_data:
                    date = entry.get("date", "")
                    value = entry.get("value", "")

                    if date and value:
                        extracted_data.append({
                            "Country": country,
                            "Indicator": indicator,
                            "Date": date,
                            "Value": value
                        })
            else:
                extracted_data.append({
                    "Country": country,
                    "Indicator": indicator,
                    "Date": "",
                    "Value": ""
                })

    df = pd.DataFrame(extracted_data)
    
    # Substituir valores vazios por NaN
    df.replace('', np.nan, inplace=True)
    
    # Salvar o DataFrame em formato parquet
    df.to_parquet('../SOFTPLAN/Data/Raw_Zone/Valores/valores_raw.parquet') #editar a pasta para consume_zone
    print("DataFrame salvo com sucesso em formato parquet.")



if __name__ == "__main__":
    main()
