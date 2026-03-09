import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = (
    f"postgresql+psycopg2://"
    f"{os.getenv('POSTGRES_USER_API')}:"
    f"{os.getenv('POSTGRES_PASSWORD_API')}@"
    f"{os.getenv('POSTGRES_HOST_API', 'postgres_api')}:"
    f"{os.getenv('POSTGRES_PORT_API', '5432')}/"
    f"{os.getenv('POSTGRES_DB_API')}"
)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()