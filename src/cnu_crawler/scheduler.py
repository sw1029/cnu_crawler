# cnu_crawler/scheduler.py
import argparse
import asyncio
from loguru import logger
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from cnu_crawler.config import ROOT_URL, SCHEDULE_MINUTES
from cnu_crawler.storage.models import init_db, get_session, College, Department
from cnu_crawler.spiders.colleges import discover_colleges
from cnu_crawler.spiders.departments import crawl_departments
from cnu_crawler.spiders.notices import crawl_department_notices
from cnu_crawler.storage.csv_sink import dump_daily_csv
from cnu_crawler.core.fetcher import Fetcher

async def run_pipeline():
    # 1) 대학 목록 (변동 적음) – 하루 1회만 탐색
    colleges = await discover_colleges(ROOT_URL)
    # 2) 학과 목록 + 3) 공지사항 증분
    with get_session() as sess:
        colleges_db = sess.query(College).all()
    for college in colleges_db:
        await crawl_departments(college)
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

    if args.init:
        init_db()
        print("DB initialized.")
        return

    if args.run:
        await run_pipeline()
        return

    # 스케줄러
    init_db()
    sched = AsyncIOScheduler(timezone="Asia/Seoul")
    sched.add_job(run_pipeline, "interval", minutes=SCHEDULE_MINUTES, next_run_time=None)
    sched.start()
    logger.info(f"🚀 Scheduler started. every {SCHEDULE_MINUTES} min")
    try:
        await asyncio.Event().wait()  # keep running
    finally:
        await Fetcher.instance().close()

if __name__ == "__main__":
    asyncio.run(main())
