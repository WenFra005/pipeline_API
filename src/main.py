"""
Módulo principal para execução do pipeline ETL de cotação do dólar (USD-BRL).

Este script inicializa o ambiente, cria as tabelas e executa o pipeline de extração,
transformação e carga de dados de cotação do dólar.

O pipeline executa automaticamente em horários específicos (08:00-19:00, dias úteis)
e pode ser interrompido via SIGTERM ou Ctrl+C.
"""

import datetime
import signal
import threading
import time
from zoneinfo import ZoneInfo

import logfire

from src.config.config import configure_ambient_logging, configure_database
from src.database.database import Base
from src.pipeline.extract import extract_data
from src.pipeline.load import save_data_postgres
from src.pipeline.transform import transform_data

stop_event = threading.Event()


def handle_sigterm(_signum, _frame):
    """Gerencia o sinal de término (SIGTERM) para encerrar o pipeline.

    Parameters
    ----------
    _signum : int
        O número do sinal recebido.
    _frame : frame
        O frame atual do programa.
    """
    logger.info("Recebido sinal de término (SIGTERM). Encerrando o pipeline...")
    stop_event.set()


def is_within_allowed_time():
    """Verifica se o horário atual está dentro do intervalo permitido para execução.

    O intervalo permitido é de segunda a sexta-feira, das 08:00 às 19:00
    (horário de São Paulo).

    Returns
    -------
    bool
        True se o horário atual estiver dentro do intervalo permitido,
        False caso contrário.
    """
    now = datetime.datetime.now(ZoneInfo("America/Sao_Paulo"))
    # Verifica se é fim de semana (5 = sábado, 6 = domingo)
    if now.weekday() >= 5:
        return False
    start = now.replace(hour=8, minute=0, second=0, microsecond=0)
    end = now.replace(hour=19, minute=0, second=0, microsecond=0)
    return start <= now <= end


def time_until_next_start():
    """Calcula o tempo restante até o próximo início permitido do pipeline.

    Considera que o pipeline só pode ser executado de segunda a sexta-feira,
    das 08:00 às 19:00.

    Returns
    -------
    datetime.timedelta
        Tempo restante até o próximo início permitido.
    """
    now = datetime.datetime.now(ZoneInfo("America/Sao_Paulo"))
    next_start = now.replace(hour=8, minute=0, second=0, microsecond=0)

    # Se já passou das 8h hoje, vai para o próximo dia
    if now >= next_start:
        next_start = next_start + datetime.timedelta(days=1)

    # Pula sábados e domingos
    while next_start.weekday() >= 5:  # 5 = sábado, 6 = domingo
        next_start = next_start + datetime.timedelta(days=1)

    return next_start - now


def create_tables(engine, logger):
    """Cria as tabelas no banco de dados PostgreSQL usando SQLAlchemy.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        Objeto engine do SQLAlchemy configurado para PostgreSQL.
    logger : logging.Logger
        Logger para registrar logs do processo.
    """
    Base.metadata.create_all(engine)
    logger.info("Tabelas criadas/verificadas com sucesso.")


def pipeline(Session, logger):
    """Executa o pipeline completo de dados (extract, transform, load).

    Parameters
    ----------
    Session : sqlalchemy.orm.session.Session
        Classe de sessão do SQLAlchemy para interagir com o banco.
    logger : logging.Logger
        Logger para registrar logs do pipeline.
    """
    with logfire.span("Extraindo dados"):
        data = extract_data(logger)

    if not data:
        logger.error("Nenhum dado foi extraído. Encerrando o pipeline.")
        return

    with logfire.span("Transformando dados"):
        transformed_data = transform_data(data)

    with logfire.span("Salvando dados no PostgreSQL"):
        save_data_postgres(Session, transformed_data, logger)

    logger.info("Pipeline de dados concluído com sucesso.")


def loop_pipeline(Session, logger):
    """Executa o pipeline em loop contínuo com controle de horário.

    O pipeline executa apenas dentro do horário permitido (08:00-19:00, dias úteis).
    Fora do horário, aguarda e faz logs informativos.

    Parameters
    ----------
    Session : sqlalchemy.orm.session.Session
        Classe de sessão do SQLAlchemy para interagir com o banco.
    logger : logging.Logger
        Logger para registrar logs do pipeline.
    """
    while not stop_event.is_set():
        if is_within_allowed_time():
            with logfire.span("Executando o pipeline"):
                try:
                    pipeline(Session, logger)
                    logger.info("Aguardando 30 segundos para a próxima execução...")
                    stop_event.wait(30)
                except Exception as e:
                    logger.error(f"Ocorreu um erro inesperado: {e}")
                    time.sleep(30)
            logger.info("Pipeline finalizado.")
        else:
            time_remaining = time_until_next_start()
            minutes, seconds = divmod(time_remaining.seconds, 60)
            hours, minutes = divmod(minutes, 60)
            logger.info(
                f"Fora do horário permitido (08:00-19:00). "
                f"Tempo restante até o próximo início: {hours:02d}:{minutes:02d}:{seconds:02d}. "
                f"Checando novamente em 10 minutos..."
            )
            stop_event.wait(600)  # 10 minutos
    logger.info("Execução encerrada.")


if __name__ == "__main__":
    logger = configure_ambient_logging()
    signal.signal(signal.SIGTERM, handle_sigterm)
    engine, Session = configure_database()
    create_tables(engine, logger)
    logger.info("Iniciando...")
    try:
        loop_pipeline(Session, logger)
    except KeyboardInterrupt:
        logger.info("Interrupção do teclado recebida. Encerrando o pipeline...")
        stop_event.set()
