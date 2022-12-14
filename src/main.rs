use polars::{lazy::dsl::GetOutput, prelude::*};

fn main() {
    let recept_path = "100knocks-preprocess/docker/work/data/receipt.csv";
    // let store_path = "100knocks-preprocess/docker/work/data/store.csv";
    let customer_path = "100knocks-preprocess/docker/work/data/customer.csv";
    // let product_path = "100knocks-preprocess/docker/work/data/product.csv";
    // let category_path = "100knocks-preprocess/docker/work/data/category.csv";

    fn to_epoch(s: &Series) -> Series {
        s.duration()
            .unwrap()
            .into_iter()
            .map(|date| match date {
                Some(date) => date / 1000,
                None => 0,
            })
            .collect()
    }

    let recept_df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .select([col("customer_id"), col("sales_ymd")]);

    let customer_df = LazyCsvReader::new(customer_path)
        .has_header(true)
        .finish()
        .unwrap()
        .select([col("customer_id"), col("application_date")]);

    let joined = recept_df
        .inner_join(customer_df, "customer_id", "customer_id")
        .unique(None, UniqueKeepStrategy::First)
        .select([
            col("customer_id"),
            (col("sales_ymd").cast(DataType::Utf8))
                .str()
                .strptime(StrpTimeOptions {
                    fmt: Some("%Y%m%d".to_string()),
                    date_dtype: DataType::Date,
                    ..Default::default()
                }),
            (col("application_date").cast(DataType::Utf8))
                .str()
                .strptime(StrpTimeOptions {
                    fmt: Some("%Y%m%d".to_string()),
                    date_dtype: DataType::Date,
                    ..Default::default()
                }),
        ])
        .with_column(
            ((col("sales_ymd") - col("application_date"))
                .map(|s| Ok(to_epoch(&s)), GetOutput::default()))
            .alias("diff"),
        )
        .filter(col("customer_id").str().contains("CS006214000001")) //PythonのAnswerと同じ結果を出すために便宜的に追加。題意には含まれない。
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", joined);
}
