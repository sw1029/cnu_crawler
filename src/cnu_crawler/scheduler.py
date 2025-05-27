# src/cnu_crawler/scheduler.py
import argparse
import asyncio
from loguru import logger
# apscheduler 타입 힌트 문제를 무시하거나, 정확한 타입 명시 필요
from apscheduler.schedulers.asyncio import AsyncIOScheduler  # type: ignore

from cnu_crawler.config import ROOT_URL, SCHEDULE_MINUTES
# config 파일에서 지연 시간 설정을 가져옵니다.
from cnu_crawler.config import (
    # REQUEST_DELAY_COLLEGE_SECONDS, # 현재 파이프라인에서는 직접 사용하지 않음
    REQUEST_DELAY_DEPARTMENT_SECONDS,  # 각 College의 학과 처리 후 또는 각 Department 공지 처리 전 지연
    # REQUEST_DELAY_NOTICE_PAGE_SECONDS # notices.py에서 사용
)
# DB 모델 및 세션 함수 임포트
from cnu_crawler.storage.models import init_db, get_session, College, Department, Notice
# 스파이더 함수 임포트 (spiders/__init__.py를 통해 노출된 함수 사용)
from cnu_crawler.spiders import (  #
    discover_all_colleges_entrypoint,  # 수정된 College 수집 함수
    crawl_departments,
    crawl_department_notices
)
from cnu_crawler.storage.csv_sink import dump_daily_csv  #
from cnu_crawler.core.fetcher import close_global_fetcher_client  #


async def add_hardcoded_ai_department():
    """인공지능학과 정보를 DB에 하드코딩하여 추가/업데이트합니다."""
    ai_dept_info = {
        "code": "dept_cnu_ai_hardcoded",  # 고유한 학과 코드
        "name": "인공지능학과",
        "url": "https://ai.cnu.ac.kr/ai/index.do",
        "dept_type": "ai_hardcoded",  # 모델에 정의된 타입
        # FIXME: 아래 URL 템플릿은 실제 인공지능학과 웹사이트를 분석하여 정확히 설정해야 합니다.
        "undergrad_notice_url_template": "https://ai.cnu.ac.kr/ai/community/notice.do?mode=list&page={}",  # 학부 공지 (예시)
        "academic_notice_url_template": "https://ai.cnu.ac.kr/ai/community/undergraduate_course_notice.do?mode=list&page={}",
        # 학사 공지 (예시)
        "grad_notice_url_template": "https://ai.cnu.ac.kr/ai/community/graduate_notice.do?mode=list&page={}"
        # 대학원 공지 (예시)
    }
    # 인공지능학과를 소속시킬 College가 필요.
    # 1. '기타' 또는 '미분류' College를 만들고 그 ID를 사용
    # 2. 특정 단과대학(예: 자연과학대학)에 임시로 소속 (이 경우 해당 College ID 필요)
    # 여기서는 '미분류' College를 가정하고, 해당 College가 DB에 없으면 생성합니다.
    misc_college_code = "college_misc_for_ai"
    misc_college_name = "미분류 학과 소속"
    misc_college_url = "#"  # 실제 URL 없음

    with get_session() as sess:
        misc_college = sess.query(College).filter_by(code=misc_college_code).one_or_none()
        if not misc_college:
            misc_college = College(
                code=misc_college_code,
                name=misc_college_name,
                url=misc_college_url,
                college_type="placeholder"  # 임시 타입
            )
            sess.add(misc_college)
            try:
                sess.commit()  # misc_college에 ID가 할당되도록 commit
                logger.info(f"'{misc_college_name}' College를 생성했습니다.")
            except Exception as e_coll_db:
                logger.error(f"'{misc_college_name}' College 생성 중 DB 오류: {e_coll_db}")
                sess.rollback()
                return  # College 생성 실패 시 학과 추가 불가

        if not misc_college.id:  # 커밋 후에도 ID가 없다면 문제
            logger.error(f"'{misc_college_name}' College ID를 가져올 수 없습니다.")
            return

        ai_dept_db_data = ai_dept_info.copy()
        ai_dept_db_data["college_id"] = misc_college.id

        existing_dept = sess.query(Department).filter_by(
            college_id=ai_dept_db_data["college_id"],
            code=ai_dept_db_data["code"]
        ).one_or_none()

        if existing_dept:
            changed = False
            for key, value in ai_dept_db_data.items():
                if hasattr(existing_dept, key) and getattr(existing_dept, key) != value:
                    setattr(existing_dept, key, value)
                    changed = True
            if changed:
                logger.info(f"하드코딩된 '{ai_dept_db_data['name']}' 학과 정보를 업데이트합니다.")
        else:
            existing_dept = Department(**ai_dept_db_data)
            sess.add(existing_dept)
            logger.info(f"하드코딩된 '{ai_dept_db_data['name']}' 학과 정보를 새로 추가합니다.")

        try:
            sess.commit()
        except Exception as e_dept_db:
            logger.error(f"'{ai_dept_db_data['name']}' 학과 정보 저장 중 DB 오류: {e_dept_db}")
            sess.rollback()


async def run_pipeline():
    logger.info("⚙️ 파이프라인 실행 시작")

    # 0. 인공지능학과 정보 추가/업데이트 (하드코딩)
    # 이 작업은 College 정보 수집 전 또는 후에 수행 가능
    # College 정보가 먼저 있어야 college_id를 할당할 수 있으므로, 적절한 위치에 배치.
    # 여기서는 College 수집 후, Department 수집 전에 수행 (미분류 College 생성 후)
    # 또는, discover_all_colleges_entrypoint에서 AI용 College를 만들도록 수정할 수도 있음.

    # 1) 모든 종류의 대학/대학원 단위 정보 수집
    await discover_all_colleges_entrypoint()  # 이 함수는 내부적으로 DB에 College 정보를 저장/업데이트합니다.

    # 1.5) 인공지능학과 정보 DB에 추가/업데이트
    await add_hardcoded_ai_department()

    # 2) 각 College에 대해 학과/학부 정보 수집
    with get_session() as sess:  # DB에서 College 목록 다시 로드
        all_colleges_from_db = sess.query(College).all()

    if not all_colleges_from_db:
        logger.warning("DB에서 College 정보를 찾을 수 없습니다. 학과 정보 수집을 건너뜁니다.")
    else:
        logger.info(f"총 {len(all_colleges_from_db)}개의 College 단위에 대해 학과/학부 정보 수집 시작.")
        for college_obj in all_colleges_from_db:
            await crawl_departments(college_obj)  #
            # 각 College의 학과 목록 크롤링 후 대기
            if REQUEST_DELAY_DEPARTMENT_SECONDS > 0:  # config.py에 정의된 값 사용
                logger.debug(f"'{college_obj.name}' 학과 목록 크롤링 후 {REQUEST_DELAY_DEPARTMENT_SECONDS:.1f}초 대기...")
                await asyncio.sleep(REQUEST_DELAY_DEPARTMENT_SECONDS)

    # 3) 각 Department에 대해 공지사항 수집
    with get_session() as sess:  # DB에서 Department 목록 다시 로드
        all_departments_from_db = sess.query(Department).all()

    if not all_departments_from_db:
        logger.warning("DB에서 Department 정보를 찾을 수 없습니다. 공지사항 수집을 건너뜁니다.")
    else:
        logger.info(f"총 {len(all_departments_from_db)}개 학과/학부의 공지사항 수집을 시작합니다.")
        # 병렬 처리 유지 (crawl_department_notices 함수 내부에서 시작 전 딜레이 적용됨)
        # 각 crawl_department_notices는 첫 페이지만 가져오도록 수정됨
        await asyncio.gather(*(crawl_department_notices(dept_obj) for dept_obj in all_departments_from_db))  #

        # # 순차 실행 및 각 학과 처리 후 딜레이를 원할 경우 (디버깅 또는 부하 매우 민감 시):
        # for dept_obj in all_departments_from_db:
        #     await crawl_department_notices(dept_obj)
        #     # crawl_department_notices 함수 시작 시 이미 REQUEST_DELAY_DEPARTMENT_SECONDS 만큼 대기함.
        #     # 추가적인 짧은 딜레이가 필요하다면 여기에 추가.
        #     # await asyncio.sleep(0.1)

    dump_daily_csv()
    logger.info("✅ 파이프라인 실행 완료")


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--init", action="store_true",
                        help="DB 초기화만 수행 후 종료")
    parser.add_argument("--run", action="store_true",
                        help="단일 실행(스케줄러 없이)")
    args = parser.parse_args()

    if args.init:
        init_db()  # DB 테이블 생성
        logger.info("데이터베이스가 초기화되었습니다.")
        return

    # --run 플래그가 없거나, 스케줄러 모드일 때 DB 초기화
    # init_db()는 create_all(ENGINE)을 호출하므로 여러 번 호출해도 안전합니다.
    init_db()

    try:
        if args.run:
            logger.info("단일 실행 모드로 파이프라인을 실행합니다.")
            await run_pipeline()
            # 단일 실행 후 정상 종료 시 아래 finally 블록이 호출됩니다.
            return

            # 스케줄러 모드
        logger.info(f"스케줄러를 시작합니다. 매 {SCHEDULE_MINUTES}분마다 실행됩니다.")  # SCHEDULE_MINUTES from config
        sched = AsyncIOScheduler(timezone="Asia/Seoul")
        sched.add_job(run_pipeline, "interval", minutes=SCHEDULE_MINUTES, next_run_time=None)
        sched.start()
        await asyncio.Event().wait()  # 스케줄러 실행 중 대기

    except KeyboardInterrupt:
        logger.info("사용자에 의해 프로그램이 중단되었습니다. 종료 절차를 진행합니다...")
    except Exception as e:
        logger.opt(exception=True).error(f"main 실행 중 예측하지 못한 오류 발생: {e}")
    finally:
        logger.info("main의 finally 블록 실행: Fetcher 클라이언트 정리 시도...")
        await close_global_fetcher_client()  #
        logger.info("Fetcher 클라이언트 정리 완료. main의 finally 블록이 완료되었습니다.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # asyncio.run() 내부에서 KeyboardInterrupt 발생 시, main 함수의 finally가 먼저 실행됨.
        # 이 부분은 거의 호출되지 않거나, 매우 예외적인 경우 (예: asyncio.run 직전 Ctrl+C)
        logger.info("애플리케이션 프로세스가 최상위 레벨에서 KeyboardInterrupt를 수신했습니다.")
    except Exception as e:
        logger.opt(exception=True).critical(f"애플리케이션 실행 중 최상위 레벨에서 처리되지 않은 예외 발생: {e}")
    finally:
        logger.info("애플리케이션이 종료됩니다.")