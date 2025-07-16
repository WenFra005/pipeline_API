"""
Este módulo contém funções para configurar o ambiente de logging e a conexão com o banco de dados
PostgreSQL. Ele carrega as variáveis de ambiente necessárias e define as configurações
para o logger e a conexão com o banco de dados.
"""

import logging
import os
from logging import basicConfig, getLogger

import logfire
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

load_dotenv()

TOKEN_AWESOMEAPI = os.getenv("TOKEN_AWESOMEAPI")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")


def configure_ambient_logging():
    """
    Configura o ambiente de logging usando logfire e logging padrão do Python.
    Carrega as variáveis de ambiente necessárias e define o logger para registrar logs no console e
    no logfire.

    Returns
    -------
    logger : logging.Logger
        Um objeto logger configurado para registrar logs no console e no logfire.
    """
    logfire.configure()
    basicConfig(handlers=[logfire.LogfireLoggingHandler()])
    logger = getLogger(__name__)
    logger.setLevel(logging.INFO)
    logfire.instrument_requests()
    logfire.instrument_sqlalchemy()

    return logger


def configure_database():
    """
    Configura a conexão com o banco de dados PostgreSQL usando SQLAlchemy. Carrega as variáveis de
    ambiente necessárias para a conexão e cria um objeto engine e uma sessão para interagir
    com o banco de dados.

    Returns
    -------
    engine : sqlalchemy.engine.Engine
        Um objeto engine do SQLAlchemy configurado para se conectar ao banco de dados PostgreSQL.

    Session : sqlalchemy.orm.session.Session
        Uma classe de sessão do SQLAlchemy para interagir com o banco de dados.
    """

    DATABASE_URL = (
        f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
        f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )

    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)

    return engine, Session
