"""
Módulo responsável pela extração de dados da cotação do dólar (USD-BRL) via API.

Este módulo contém funções para conectar com APIs externas de cotação de moedas
e extrair dados atualizados do dólar em relação ao real brasileiro.
"""

import requests

from src.config.config import TOKEN_AWESOMEAPI


def extract_data(logger):
    """Extrai dados da API AwesomeAPI para obter a cotação do dólar (USD-BRL).

    Faz uma requisição HTTP para a API AwesomeAPI para obter dados atualizados
    da cotação do dólar em relação ao real brasileiro.

    Parameters
    ----------
    logger : logging.Logger
        Logger para registrar logs do processo de extração.

    Returns
    -------
    dict or None
        Dicionário contendo os dados extraídos da API, ou None se houver erro.

    Examples
    --------
    >>> data = extract_data(logger)
    >>> if data:
    ...     print(f"USD-BRL: {data['USDBRL']['bid']}")
    """
    url = (
        f"https://economia.awesomeapi.com.br/json/last/USD-BRL?token={TOKEN_AWESOMEAPI}"
    )
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        logger.error(f"Erro ao acessar a API: {response.status_code} - {response.text}")
        return None
