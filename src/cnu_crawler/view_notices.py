# check_notices.py (예시)
import asyncio
from sqlalchemy import select, desc  # SQLAlchemy 2.0 스타일로 변경
from cnu_crawler.storage.models import Notice, Department, College, get_session, init_db
from cnu_crawler.config import DATA_DIR  # DB 경로를 위해 추가


async def view_collected_notices():
    # 데이터베이스 파일이 DATA_DIR에 있는지 확인하고, 없으면 초기화 시도
    # 실제로는 DB 파일이 크롤러 실행 시 생성되므로, 이 스크립트 실행 전에 크롤러가 최소 한 번은 실행되어야 함
    db_path = DATA_DIR / "notices.sqlite3"
    if not db_path.exists():
        print(f"데이터베이스 파일({db_path})을 찾을 수 없습니다. 크롤러를 먼저 실행해주세요.")
        # 필요시 init_db()를 호출하여 빈 DB라도 생성할 수 있으나, 데이터가 없을 것임
        # print("DB 파일을 생성합니다...")
        # init_db()
        return

    print("수집된 공지사항 확인 시작...")
    with get_session() as session:
        # 최근 공지사항부터 50개만 가져오는 예시 (필요에 따라 조절)
        # 모든 공지사항을 보려면 .limit(50) 제거
        # SQLAlchemy 2.0 스타일 쿼리
        stmt = (
            select(Notice, Department.name, College.name)
            .join(Department, Notice.dept_id == Department.id)
            .join(College, Department.college_id == College.id)
            .order_by(desc(Notice.crawled_at))  # 최신 크롤링 순
            .limit(50)
        )

        results = session.execute(stmt).all()  # [(Notice, dept_name, college_name), ...]

        if not results:
            print("수집된 공지사항이 없습니다.")
            return

        print(f"\n--- 최근 수집된 공지사항 (최대 50개) ---")
        for notice_obj, dept_name, college_name in results:
            print(f"\n[대학]: {college_name}")
            print(f"[학과/출처]: {dept_name}")
            if notice_obj.source_display_name:  # "학과명 + 대학원" 같은 출처 표시
                print(f"[상세출처]: {notice_obj.source_display_name}")
            print(f"[게시판유형]: {notice_obj.board}")
            print(f"[제목]: {notice_obj.title}")
            print(f"[게시일]: {notice_obj.posted_at.strftime('%Y-%m-%d %H:%M') if notice_obj.posted_at else 'N/A'}")
            print(f"[URL]: {notice_obj.url}")
            print(f"[수집일]: {notice_obj.crawled_at.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"[Post ID]: {notice_obj.post_id}")
            print("-" * 20)

        # 특정 학과의 공지사항만 보고 싶다면 filter_by 추가
        # 예: 특정 학과 이름으로 필터링 (Department 테이블과 join 필요)
        # target_dept_name = "컴퓨터융합학부" # 예시 학과명
        # stmt_specific_dept = (
        #     select(Notice)
        #     .join(Department, Notice.dept_id == Department.id)
        #     .filter(Department.name == target_dept_name)
        #     .order_by(desc(Notice.posted_at))
        #     .limit(10)
        # )
        # results_specific = session.scalars(stmt_specific_dept).all()
        # print(f"\n--- {target_dept_name} 공지사항 (최근 10개) ---")
        # for notice_obj in results_specific:
        #     print(f"제목: {notice_obj.title}, 게시일: {notice_obj.posted_at}, URL: {notice_obj.url}")


if __name__ == "__main__":
    # 이 스크립트는 프로젝트 루트에서 python -m path.to.check_notices 와 같이 실행하거나,
    # PYTHONPATH를 설정하고 직접 실행해야 합니다.
    # 또는, cnu_crawler 모듈이 설치된 환경에서 실행합니다.
    # 가장 간단한 방법은 프로젝트 루트에 이 파일을 두고,
    # from src.cnu_crawler.storage.models ... 와 같이 경로를 수정하는 것입니다.
    # 여기서는 현재 cnu_crawler 프로젝트 구조에 맞게 asyncio.run을 사용합니다.
    asyncio.run(view_collected_notices())