from fastapi import APIRouter

from src.config.config import configure_database
from src.database.database import DolarData

router = APIRouter()


@router.get("/cotacoes")
def listar_cotacoes():
    engine, SessionLocal = configure_database()
    session = SessionLocal()
    try:
        cotacoes = session.query(DolarData).all()
        resultado = [
            {
                "id": c.id,
                "moeda_origem": c.moeda_origem,
                "moeda_destino": c.moeda_destino,
                "valor_de_compra": c.valor_de_compra,
                "timestamp_moeda": c.timestamp_moeda.isoformat(),
                "timestamp_criacao": c.timestamp_criacao.isoformat(),
            }
            for c in cotacoes
        ]
        return resultado
    finally:
        session.close()
