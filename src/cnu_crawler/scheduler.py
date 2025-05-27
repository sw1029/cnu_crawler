# src/cnu_crawler/scheduler.py
import argparse
import asyncio
from loguru import logger
from apscheduler.schedulers.asyncio import AsyncIOScheduler # type: ignore

from cnu_crawler.config import ROOT_URL, SCHEDULE_MINUTES
from cnu_crawler.storage.models import init_db, get_session, College, Department
from cnu_crawler.spiders.colleges import discover_colleges
from cnu_crawler.spiders.departments import crawl_departments
from cnu_crawler.spiders.notices import crawl_department_notices
from cnu_crawler.storage.csv_sink import dump_daily_csv
# 수정된 fetcher.py에서 close_global_fetcher_client 함수를 가져옵니다.
from cnu_crawler.core.fetcher import close_global_fetcher_client


async def run_pipeline():
    # 1) 대학 목록 (변동 적음) – 하루 1회만 탐색
    # discover_colleges 함수는 이제 HTML 직접 파싱 방식을 사용하므로,
    # ROOT_URL을 올바르게 전달해야 합니다.
    await discover_colleges(ROOT_URL) # ROOT_URL 전달
    # 2) 학과 목록 + 3) 공지사항 증분
    with get_session() as sess:
        colleges_db = sess.query(College).all()
    for college_obj in colleges_db: # 변수명 변경 (college -> college_obj)
        await crawl_departments(college_obj) # college_obj 전달
    with get_session() as sess:
        depts = sess.query(Department).all()
    # 병렬 크롤링
    await asyncio.gather(*(crawl_department_notices(d) for d in depts))
    dump_daily_csv()

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--init", action="store_true",
                        help="DB 초기화만 수행 후 종료")
    parser.add_argument("--run", action="store_true",
                        help="단일 실행(스케줄러 없이)")
    args = parser.parse_args()

    # DB 초기화는 필요한 경우 한 번만 수행하는 것이 좋습니다.
    # 예를 들어, --init 플래그가 있을 때만 또는 프로그램 시작 시점에 한 번.
    if args.init:
        init_db() # src/cnu_crawler/storage/models.py 정의된 함수
        logger.info("데이터베이스가 초기화되었습니다.")
        return # 초기화 후 종료

    # --run 플래그가 없을 때 (스케줄러 모드) DB 초기화
    if not args.run:
        init_db() # 스케줄러 시작 전에 DB 초기화

    try:
        if args.run:
            logger.info("단일 실행 모드로 파이프라인을 실행합니다.")
            await run_pipeline()
            logger.info("파이프라인 실행 완료 (단일 실행 모드).")
            # 단일 실행 후 정상 종료 시 finally 블록이 호출됩니다.
            return

        # 스케줄러 모드
        logger.info(f"스케줄러를 시작합니다. 매 {SCHEDULE_MINUTES}분마다 실행됩니다.")
        # AsyncIOScheduler 생성 시 timezone을 전달하는 것이 좋습니다.
        # from zoneinfo import ZoneInfo # Python 3.9+
        # sched = AsyncIOScheduler(timezone=ZoneInfo("Asia/Seoul"))
        sched = AsyncIOScheduler(timezone="Asia/Seoul") # 기존 방식 유지
        sched.add_job(run_pipeline, "interval", minutes=SCHEDULE_MINUTES, next_run_time=None)
        sched.start()
        # 스케줄러가 백그라운드에서 실행되도록 메인 코루틴을 유지합니다.
        # KeyboardInterrupt (Ctrl+C) 등으로 중단될 수 있습니다.
        await asyncio.Event().wait()

    except KeyboardInterrupt:
        logger.info("사용자에 의해 스케줄러가 중단되었습니다. 종료 절차를 진행합니다...")
    except Exception as e:
        logger.opt(exception=True).error(f"main 실행 중 오류 발생: {e}")
    finally:
        logger.info("main의 finally 블록 실행: Fetcher 클라이언트가 닫혔는지 확인합니다.")
        await close_global_fetcher_client() # 수정된 정리 함수 호출
        logger.info("main의 finally 블록이 완료되었습니다.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # asyncio.run() 내부에서 KeyboardInterrupt가 처리되지만,
        # 만약을 위해 최상위에도 로깅을 남길 수 있습니다.
        logger.info("애플리케이션 프로세스가 KeyboardInterrupt를 수신했습니다. 종료합니다.")
    except Exception as e:
        logger.opt(exception=True).critical(f"최상위 레벨에서 처리되지 않은 예외 발생: {e}")
    finally:
        logger.info("애플리케이션이 종료됩니다.")