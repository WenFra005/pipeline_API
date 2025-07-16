"""
Módulo de definição do modelo de dados e ORM para a tabela dolar_data no banco PostgreSQL.

Este módulo define a estrutura da tabela que armazena os dados de cotação do dólar,
incluindo campos para moedas, valores e timestamps.
"""

from sqlalchemy import Column, DateTime, Float, Integer, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class DolarData(Base):
    """Classe que representa a tabela dolar_data no banco de dados.

    Esta classe define o modelo ORM para armazenar dados de cotação do dólar,
    incluindo informações sobre moedas, valores de compra e timestamps.

    Attributes
    ----------
    id : int
        Chave primária auto-incrementada.
    moeda_origem : str
        Código da moeda de origem (ex: USD).
    moeda_destino : str
        Código da moeda de destino (ex: BRL).
    valor_de_compra : float
        Valor de compra da moeda de origem em relação à de destino.
    timestamp_moeda : datetime
        Data e hora da cotação (com timezone).
    timestamp_criacao : datetime
        Data e hora de criação do registro no sistema (com timezone).
    """

    __tablename__ = "dolar_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    moeda_origem = Column(String(3), nullable=False)
    moeda_destino = Column(String(3), nullable=False)
    valor_de_compra = Column(Float, nullable=False)
    timestamp_moeda = Column(DateTime(timezone=True), nullable=False)
    timestamp_criacao = Column(DateTime(timezone=True), nullable=False)
