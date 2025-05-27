# src/cnu_crawler/scheduler.py
import argparse
import asyncio  # asyncio 모듈 import
from loguru import logger
from apscheduler.schedulers.asyncio import AsyncIOScheduler  # type: ignore

from cnu_crawler.config import ROOT_URL, SCHEDULE_MINUTES
# config 파일에서 지연 시간 설정을 가져옵니다.
from cnu_crawler.config import (
    REQUEST_DELAY_COLLEGE_SECONDS,  # 필요시 사용 (현재 로직에는 직접 사용되지 않음)
    REQUEST_DELAY_DEPARTMENT_SECONDS,  # 각 학과 처리 전/후 지연 시간
    REQUEST_DELAY_NOTICE_PAGE_SECONDS  # 공지사항 페이지 간 지연 시간 (notices.py에서 사용)
)
from cnu_crawler.storage.models import init_db, get_session, College, Department
from cnu_crawler.spiders.colleges import discover_colleges
from cnu_crawler.spiders.departments import crawl_departments
from cnu_crawler.spiders.notices import crawl_department_notices
from cnu_crawler.storage.csv_sink import dump_daily_csv
from cnu_crawler.core.fetcher import close_global_fetcher_client


async def run_pipeline():
    logger.info("⚙️ 파이프라인 실행 시작")
    # 1) 대학 목록 (변동 적음)
    await discover_colleges(ROOT_URL)  #

    # 필요하다면 대학 목록 가져온 후 약간의 대기
    # await asyncio.sleep(REQUEST_DELAY_COLLEGE_SECONDS)

    # 2) 학과 목록 크롤링
    with get_session() as sess:
        colleges_db = sess.query(College).all()

    for college_obj in colleges_db:
        await crawl_departments(college_obj)  #
        # 각 대학의 학과 목록 크롤링 후 대기
        if REQUEST_DELAY_DEPARTMENT_SECONDS > 0:  # 설정된 지연 시간이 있을 경우에만 대기
            logger.debug(f"'{college_obj.name}' 학과 목록 크롤링 후 {REQUEST_DELAY_DEPARTMENT_SECONDS:.1f}초 대기...")
            await asyncio.sleep(REQUEST_DELAY_DEPARTMENT_SECONDS)

    # 3) 학과별 공지사항 증분 크롤링
    with get_session() as sess:
        depts_to_crawl = sess.query(Department).all()

    # 학과별 공지사항 크롤링 시, 각 학과 처리 전에 딜레이를 두는 것은
    # spiders/notices.py의 crawl_department_notices 함수 시작 부분에서 이미 처리하고 있습니다.
    # from cnu_crawler.config import REQUEST_DELAY_DEPARTMENT_SECONDS
    # await asyncio.sleep(REQUEST_DELAY_DEPARTMENT_SECONDS) 부분이 그 역할입니다.
    # 만약 여기서 추가적인 딜레이를 원하거나, 병렬 처리 방식을 변경한다면 수정 가능합니다.

    if depts_to_crawl:
        logger.info(f"총 {len(depts_to_crawl)}개 학과의 공지사항 수집을 시작합니다.")
        # 현재 asyncio.gather를 사용하여 모든 학과 공지사항을 병렬로 수집합니다.
        # 만약 서버 부하가 매우 우려된다면, 한 번에 실행되는 코루틴 수를 제한하거나 (예: asyncio.Semaphore 사용)
        # 아래와 같이 순차적으로 실행하며 각 작업 사이에 sleep을 넣을 수 있습니다.
        #
        # 순차 실행 예시:
        for dept_obj in depts_to_crawl:
            await crawl_department_notices(dept_obj)
            if REQUEST_DELAY_DEPARTMENT_SECONDS > 0: # 각 학과 공지사항 수집 후 추가 대기
                logger.debug(f"'{dept_obj.name}' 공지사항 수집 후 {REQUEST_DELAY_DEPARTMENT_SECONDS:.1f}초 대기...")
                await asyncio.sleep(REQUEST_DELAY_DEPARTMENT_SECONDS)
        #
        # 현재 병렬 방식 유지 (각 crawl_department_notices 함수 내에서 시작 시 딜레이가 이미 있음)
        #await asyncio.gather(*(crawl_department_notices(d) for d in depts_to_crawl))  #
    else:
        logger.info("공지사항을 수집할 학과 정보가 없습니다.")

    dump_daily_csv()  #
    logger.info("✅ 파이프라인 실행 완료")


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--init", action="store_true",
                        help="DB 초기화만 수행 후 종료")
    parser.add_argument("--run", action="store_true",
                        help="단일 실행(스케줄러 없이)")
    args = parser.parse_args()

    if args.init:
        init_db()  #
        logger.info("데이터베이스가 초기화되었습니다.")
        return

    if not args.run:  # 스케줄러 모드일 때만 DB 초기화 (단일 실행 시에는 run_pipeline에서 처리될 수 있음)
        init_db()

    try:
        if args.run:
            logger.info("단일 실행 모드로 파이프라인을 실행합니다.")
            await run_pipeline()
            return  # 단일 실행 후 종료 시 finally 블록 실행됨

        logger.info(f"스케줄러를 시작합니다. 매 {SCHEDULE_MINUTES}분마다 실행됩니다.")  #
        sched = AsyncIOScheduler(timezone="Asia/Seoul")
        sched.add_job(run_pipeline, "interval", minutes=SCHEDULE_MINUTES, next_run_time=None)
        sched.start()
        await asyncio.Event().wait()

    except KeyboardInterrupt:
        logger.info("사용자에 의해 스케줄러가 중단되었습니다. 종료 절차를 진행합니다...")
    except Exception as e:
        logger.opt(exception=True).error(f"main 실행 중 오류 발생: {e}")
    finally:
        logger.info("main의 finally 블록 실행: Fetcher 클라이언트가 닫혔는지 확인합니다.")
        await close_global_fetcher_client()  #
        logger.info("main의 finally 블록이 완료되었습니다.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("애플리케이션 프로세스가 KeyboardInterrupt를 수신했습니다. 종료합니다.")
    except Exception as e:
        logger.opt(exception=True).critical(f"최상위 레벨에서 처리되지 않은 예외 발생: {e}")
    finally:
        logger.info("애플리케이션이 종료됩니다.")