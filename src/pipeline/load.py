"""
Módulo responsável pelo carregamento dos dados transformados no banco de dados PostgreSQL.

Este módulo contém funções para persistir os dados processados no banco de dados,
utilizando SQLAlchemy ORM para operações de inserção.
"""

from src.database.database import DolarData


def save_data_postgres(Session, data, logger):
    """Salva os dados transformados no banco de dados PostgreSQL.

    Utiliza uma sessão do SQLAlchemy para adicionar um novo registro na tabela DolarData.
    Se ocorrer um erro, a transação é revertida automaticamente.

    Parameters
    ----------
    Session : sqlalchemy.orm.session.Session
        Classe de sessão do SQLAlchemy para interagir com o banco.
    data : dict
        Dicionário contendo os dados transformados com as chaves:
        - moeda_origem: Código da moeda de origem
        - moeda_destino: Código da moeda de destino
        - valor_de_compra: Valor de compra
        - timestamp_moeda: Timestamp da cotação
        - timestamp_criacao: Timestamp de criação
    logger : logging.Logger
        Logger para registrar logs do processo de salvamento.

    Examples
    --------
    >>> data = {
    ...     "moeda_origem": "USD",
    ...     "moeda_destino": "BRL",
    ...     "valor_de_compra": 5.12,
    ...     "timestamp_moeda": datetime.now(),
    ...     "timestamp_criacao": datetime.now()
    ... }
    >>> save_data_postgres(Session, data, logger)
    """

    session = Session()
    try:
        novo_registro = DolarData(**data)
        session.add(novo_registro)
        session.commit()
        logger.info(
            f"[{data['timestamp_criacao'].strftime('%d/%m/%y %H:%M:%S')}] "
            f"Dados salvos com sucesso no banco de dados PostgreSQL."
        )
    except Exception as e:
        logger.error(f"Erro ao salvar dados no PostgreSQL: {e}")
        session.rollback()
    finally:
        session.close()
