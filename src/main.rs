use polars::{
    export::chrono::{Datelike, TimeZone, Utc},
    lazy::dsl::GetOutput,
    prelude::*,
};

fn main() {
    let recept_path = "100knocks-preprocess/docker/work/data/receipt.csv";
    // let store_path = "100knocks-preprocess/docker/work/data/store.csv";
    // let customer_path = "100knocks-preprocess/docker/work/data/customer.csv";
    // let product_path = "100knocks-preprocess/docker/work/data/product.csv";
    // let category_path = "100knocks-preprocess/docker/work/data/category.csv";

    fn to_weekdays(s: &Series) -> Series {
        s.datetime()
            .unwrap()
            .into_iter()
            .map(|date| match date {
                Some(date) => Utc
                    .timestamp_millis_opt(date as i64)
                    .unwrap()
                    .weekday()
                    .num_days_from_monday(),
                None => 0u32,
            })
            .collect()
    }

    fn to_date_string(s: &Series) -> Series {
        s.i64()
            .unwrap()
            .into_iter()
            .map(|date| match date {
                Some(date) => Utc
                    .timestamp_millis_opt(date as i64)
                    .unwrap()
                    .date_naive()
                    .format("%Y-%m-%d")
                    .to_string(),
                None => "".to_string(),
            })
            .collect()
    }

    let recept_df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .select([(col("sales_ymd").cast(DataType::Utf8))
            .str()
            .strptime(StrpTimeOptions {
                fmt: Some("%Y%m%d".to_string()),
                date_dtype: DataType::Date,
                ..Default::default()
            })])
        .with_column(
            col("sales_ymd")
                .cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                .map(|s| Ok(to_weekdays(&s)), GetOutput::default())
                .alias("weekdays"),
        )
        .with_column((col("weekdays") * lit(86400000)).alias("weekdays_millis"))
        .with_column(
            (col("sales_ymd").cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                - col("weekdays_millis"))
            .map(|s| Ok(to_date_string(&s)), GetOutput::default())
            .alias("monday"),
        )
        .select([col("*").exclude(["weekdays_millis"])])
        .collect()
        .unwrap();

    println!("{:?}", recept_df);
}
