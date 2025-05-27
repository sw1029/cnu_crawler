# cnu_crawler/storage/models.py
from datetime import datetime
from pathlib import Path
from sqlalchemy import create_engine, Integer, String, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session

from src.cnu_crawler.config import DATA_DIR

DB_PATH = Path(DATA_DIR) / "notices.sqlite3"
ENGINE = create_engine(f"sqlite:///{DB_PATH}", echo=False, future=True)

class Base(DeclarativeBase): ...

class College(Base):
    __tablename__ = "colleges"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    code: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String, nullable=False)
    url:  Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

class Department(Base):
    __tablename__ = "departments"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    college_id: Mapped[int] = mapped_column(ForeignKey("colleges.id"), nullable=False)
    code: Mapped[str] = mapped_column(String, nullable=False)
    name: Mapped[str] = mapped_column(String, nullable=False)
    url:  Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    __table_args__ = (UniqueConstraint("college_id", "code", name="uix_dept"),)

class Notice(Base):
    __tablename__ = "notices"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    dept_id: Mapped[int] = mapped_column(ForeignKey("departments.id"), nullable=False)
    board:   Mapped[str] = mapped_column(String)  # undergrad | grad
    post_id: Mapped[str] = mapped_column(String)  # 원 게시판 글 번호
    title:   Mapped[str] = mapped_column(String)
    url:     Mapped[str] = mapped_column(String)
    posted_at: Mapped[datetime] = mapped_column(DateTime)
    crawled_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    __table_args__ = (UniqueConstraint("dept_id", "board", "post_id", name="uix_unique_notice"),)

def init_db():
    Base.metadata.create_all(ENGINE)

def get_session() -> Session:
    return Session(ENGINE, expire_on_commit=False)
