# Importando as bibliotecas necessárias
import requests
import pandas as pd
import concurrent.futures

# Função para extrair dados da URL fornecida. Se first_page for True, retorna o número total de páginas.
def extracao_dados(url, first_page=False):
    try:
        # Fazendo uma requisição GET para a URL
        response = requests.get(url)
        # Se a resposta contém um código de status de erro, raise_for_status() lançará uma exceção
        response.raise_for_status()
        # Convertendo a resposta em JSON para um objeto Python
        data = response.json()
        # Se estamos na primeira página, retorne o número total de páginas
        if first_page:
            return data[0]['pages']
        else:
            return data
    except requests.exceptions.RequestException as e:
        print(f"Enquanto havia a busca dos dados, aconteceu um erro: {str(e)}")
        return None

# Função para converter dados JSON em um dataframe do Pandas
def convercao_json_to_dataframe(data):
    if not data:
        return None

    try:
        # Extraindo os dados dos países do JSON
        paises = data[1]
        # Convertendo os dados dos países em um dataframe
        df = pd.DataFrame(paises)
        return df
    except (KeyError, IndexError) as e:
        print(f"Enquanto acontecia a conversão JSON para Dataframe, aconteceu um erro: {str(e)}")
        return None

# Função principal para extrair dados e salvar em um arquivo Parquet
def main():
    base_url = "https://api.worldbank.org/v2/country?format=json&page="
    all_data = []

    # Primeira requisição para obter o número total de páginas
    total_pages = extracao_dados(base_url + '1', first_page=True)
    if total_pages is None:
        print("Não foi possível obter o número total de páginas.")
        return None

    # Fazendo requisições em paralelo
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Fazendo as requisições para todas as páginas
        futures = [executor.submit(extracao_dados, base_url + str(page)) for page in range(1, total_pages + 1)]

        # Para cada resposta que chega, converte os dados em um dataframe e adiciona à lista all_data
        for future in concurrent.futures.as_completed(futures):
            data = future.result()
            if data is None:
                continue

            df = convercao_json_to_dataframe(data)
            if df is not None:
                all_data.append(df)

    # Se obtivemos algum dado, concatene todos os dataframes em um único dataframe
    if all_data:
        df = pd.concat(all_data, ignore_index=True)
        return df

    



# Se este script for o módulo principal, execute a função main e salve os resultados em um arquivo Parquet na camada raw.
if __name__ == "__main__":
    result_df = main()
    # Salvando o dataframe resultante em um arquivo Parquet
    result_df.to_parquet('../SOFTPLAN/Data/Raw_Zone/Paises/paises_raw.parquet')
    print("DataFrame salvo com sucesso em formato parquet.")
    

