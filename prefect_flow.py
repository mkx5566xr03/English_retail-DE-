from prefect import flow, task

from etl_pipeline import extract_data, transform_data, load_data, quality_check


@task
def t_extract():
    return extract_data()


@task
def t_transform(df):
    return transform_data(df)


@task
def t_load(df):
    load_data(df)


@task
def t_quality():
    quality_check()


@flow(name="daily_sales_etl")
def daily_sales_etl():
    raw = t_extract()
    cleaned = t_transform(raw)
    t_load(cleaned)
    t_quality()


if __name__ == "__main__":
    # 直接在本機服務方式部署
    # interval=86400 ，每天跑一次
    daily_sales_etl.serve(
        name="daily_sales_etl",
        interval=86400,           # 每 24 小時執行一次
        pause_on_shutdown=True,   # 關掉程式後不會掛掉
    )

