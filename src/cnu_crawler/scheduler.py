# src/cnu_crawler/scheduler.py
import argparse
import asyncio
from loguru import logger
from apscheduler.schedulers.asyncio import AsyncIOScheduler  # type: ignore

from cnu_crawler.config import ROOT_URL, SCHEDULE_MINUTES
from cnu_crawler.config import REQUEST_DELAY_DEPARTMENT_SECONDS
from cnu_crawler.storage.models import init_db, get_session, College, Department, Notice
from cnu_crawler.spiders import (
    discover_all_colleges_entrypoint,
    # crawl_departments, # 제거
    crawl_department_notices,
    process_manual_links_file  # 변경 (TXT 처리 함수)
)
from cnu_crawler.storage.csv_sink import dump_daily_csv
from cnu_crawler.core.fetcher import close_global_fetcher_client


# add_hardcoded_ai_department 함수는 이제 필요 없으므로 제거 (TXT 파일에 AI 학과 정보 포함)

async def run_pipeline():
    logger.info("⚙️ 파이프라인 실행 시작")

    # 1) 모든 종류의 대학/대학원 단위 정보 수집 (DB에 저장/업데이트)
    await discover_all_colleges_entrypoint()

    # 2) TXT 파일에서 Department 정보 생성 및 공지사항 URL 템플릿 업데이트
    #    (이 단계에서 College 정보가 DB에 이미 있어야 함)
    await process_manual_links_file()
    # 이 함수 실행 후 Department 테이블이 TXT 내용으로 채워지거나 업데이트됨.

    # (선택적) Department 정보 생성 후 짧은 대기
    if REQUEST_DELAY_DEPARTMENT_SECONDS > 0:
        logger.debug(f"TXT 파일 처리 후 {REQUEST_DELAY_DEPARTMENT_SECONDS:.1f}초 대기...")
        await asyncio.sleep(REQUEST_DELAY_DEPARTMENT_SECONDS)

    # 3) 각 Department에 대해 공지사항 수집 (DB에 저장/업데이트)
    with get_session() as sess:
        all_departments_from_db = sess.query(Department).all()

    if not all_departments_from_db:
        logger.warning("DB에서 Department 정보를 찾을 수 없습니다 (TXT 파일 처리 결과 확인 필요). 공지사항 수집을 건너뜁니다.")
    else:
        logger.info(f"총 {len(all_departments_from_db)}개 학과/학부의 공지사항 수집을 시작합니다 (URL 템플릿 기반).")
        await asyncio.gather(*(crawl_department_notices(dept_obj) for dept_obj in all_departments_from_db))

    dump_daily_csv()
    logger.info("✅ 파이프라인 실행 완료")


# main 함수는 이전과 동일하게 유지
async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--init", action="store_true",
                        help="DB 초기화만 수행 후 종료")
    parser.add_argument("--run", action="store_true",
                        help="단일 실행(스케줄러 없이)")
    args = parser.parse_args()

    init_db()

    if args.init:
        logger.info("데이터베이스가 초기화(또는 이미 준비)되었습니다.")
        return

    try:
        if args.run:
            logger.info("단일 실행 모드로 파이프라인을 실행합니다.")
            await run_pipeline()
            return

        logger.info(f"스케줄러를 시작합니다. 매 {SCHEDULE_MINUTES}분마다 실행됩니다.")
        sched = AsyncIOScheduler(timezone="Asia/Seoul")
        sched.add_job(run_pipeline, "interval", minutes=SCHEDULE_MINUTES, next_run_time=None)
        sched.start()
        await asyncio.Event().wait()

    except KeyboardInterrupt:
        logger.info("사용자에 의해 프로그램이 중단되었습니다. 종료 절차를 진행합니다...")
    except Exception as e:
        logger.opt(exception=True).error(f"main 실행 중 예측하지 못한 오류 발생: {e}")
    finally:
        logger.info("main의 finally 블록 실행: Fetcher 클라이언트 정리 시도...")
        await close_global_fetcher_client()
        logger.info("Fetcher 클라이언트 정리 완료. main의 finally 블록이 완료되었습니다.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("애플리케이션 프로세스가 최상위 레벨에서 KeyboardInterrupt를 수신했습니다.")
    except Exception as e:
        logger.opt(exception=True).critical(f"애플리케이션 실행 중 최상위 레벨에서 처리되지 않은 예외 발생: {e}")
    finally:
        logger.info("애플리케이션이 종료됩니다.")