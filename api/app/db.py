import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv()

def get_database_url():
    return (
        f"postgresql+psycopg2://"
        f"{os.getenv('POSTGRES_USER_API')}:"
        f"{os.getenv('POSTGRES_PASSWORD_API')}@"
        f"{os.getenv('POSTGRES_HOST_API', 'postgres_api')}:"
        f"{os.getenv('POSTGRES_PORT_API', '5432')}/"
        f"{os.getenv('POSTGRES_DB_API')}"
    )

def get_engine():
    return create_engine(get_database_url())

engine = None
SessionLocal = None

def init_db():
    global engine, SessionLocal
    engine = get_engine()
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    if SessionLocal is None:
        init_db()
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()