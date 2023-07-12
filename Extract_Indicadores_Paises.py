import requests
import pandas as pd
import concurrent.futures
import pyarrow as pa

def buscando_indicadores(page):
    try:
        url = f"https://api.worldbank.org/v2/indicator?format=json&per_page=1000&page={page}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data[1]

    except requests.exceptions.RequestException as e:
        print(f"Enquanto havia a busca dos metadados dos indicadores, ocorreu um erro: {str(e)}")
        return []

def buscando_todos_os_indicadores():
    try:
        response = requests.get("https://api.worldbank.org/v2/indicator?format=json")
        response.raise_for_status()
        data = response.json()
        total_pages = data[0]["pages"]

        indicator_metadata = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for page in range(1, total_pages + 1):
                futures.append(executor.submit(buscando_indicadores, page))

            for future in concurrent.futures.as_completed(futures):
                indicator_metadata += future.result()

        return indicator_metadata

    except requests.exceptions.RequestException as e:
        print(f"Ocorreu um erro ao obter os metadados dos indicadores: {str(e)}")
        return []

def main():
    indicator_metadata = buscando_todos_os_indicadores()

    if indicator_metadata:
        indicator_codes = [indicator["id"] for indicator in indicator_metadata]
        indicator_names = [indicator["name"] for indicator in indicator_metadata]

        df = pd.DataFrame({"ID": indicator_codes, "Nome": indicator_names})
        return df

if __name__ == "__main__":
    result_df = main()
    result_df.to_parquet('../SOFTPLAN/DataSets_Tratamentos/Raw_Zone/Indicadores/indicadores.parquet')
    print("DataFrame salvo com sucesso em formato parquet.")

