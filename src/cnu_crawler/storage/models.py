# src/cnu_crawler/storage/models.py
from datetime import datetime
from pathlib import Path
from sqlalchemy import create_engine, Integer, String, DateTime, ForeignKey, UniqueConstraint, Boolean # Boolean 추가
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session

from src.cnu_crawler.config import DATA_DIR

DB_PATH = Path(DATA_DIR) / "notices.sqlite3"
ENGINE = create_engine(f"sqlite:///{DB_PATH}", echo=False, future=True)

class Base(DeclarativeBase): ...

class College(Base):
    __tablename__ = "colleges"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    code: Mapped[str] = mapped_column(String, unique=True, nullable=False) # 대학 코드 (고유해야 함)
    name: Mapped[str] = mapped_column(String, nullable=False) # 대학명 (예: 공과대학, 일반대학원)
    url:  Mapped[str] = mapped_column(String, nullable=False) # 대표 URL
    # 'plus_general_grad' (plus.cnu.ac.kr 일반대학원), 'plus_special_grad' (plus.cnu.ac.kr 전문/특수대학원),
    # 'normal_college' (일반 단과대학), 'grad_page_college' (grad.cnu.ac.kr 에서 가져온 대학 단위)
    college_type: Mapped[str] = mapped_column(String, default="normal_college", nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

class Department(Base):
    __tablename__ = "departments"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    college_id: Mapped[int] = mapped_column(ForeignKey("colleges.id"), nullable=False)
    code: Mapped[str] = mapped_column(String, nullable=False) # 학과 코드 (college_id 내에서 고유)
    name: Mapped[str] = mapped_column(String, nullable=False) # 학과명 (예: 기계공학부, 인공지능학과)
    url:  Mapped[str] = mapped_column(String, nullable=False) # 학과 대표 URL
    # 'normal_dept', 'grad_school_dept' (grad.cnu.ac.kr에서 파싱), 'ai_hardcoded'
    dept_type: Mapped[str] = mapped_column(String, default="normal_dept", nullable=False)
    # "학사공지" URL 템플릿 (페이지 번호는 {} 등으로 표시)
    academic_notice_url_template: Mapped[str] = mapped_column(String, nullable=True)
    # "일반공지" (학부) URL 템플릿
    undergrad_notice_url_template: Mapped[str] = mapped_column(String, nullable=True)
    # "일반공지" (대학원) URL 템플릿
    grad_notice_url_template: Mapped[str] = mapped_column(String, nullable=True)
    # "대학원" 키워드 관련 공지사항 페이지 URL (발견 시)
    specific_grad_keyword_notice_url: Mapped[str] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    __table_args__ = (UniqueConstraint("college_id", "code", name="uix_dept"),)

class Notice(Base):
    __tablename__ = "notices"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    dept_id: Mapped[int] = mapped_column(ForeignKey("departments.id"), nullable=False)
    # 'undergrad', 'grad', 'academic' (학사공지), 'grad_keyword_found' (대학원 키워드 공지)
    board: Mapped[str] = mapped_column(String)
    post_id: Mapped[str] = mapped_column(String)
    title:   Mapped[str] = mapped_column(String)
    url:     Mapped[str] = mapped_column(String)
    posted_at: Mapped[datetime] = mapped_column(DateTime)
    crawled_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    # "학과명 + 대학원" 과 같은 출처 표시 이름
    source_display_name: Mapped[str] = mapped_column(String, nullable=True)
    __table_args__ = (UniqueConstraint("dept_id", "board", "post_id", name="uix_unique_notice"),)

def init_db():
    Base.metadata.create_all(ENGINE)

def get_session() -> Session:
    return Session(ENGINE, expire_on_commit=False)