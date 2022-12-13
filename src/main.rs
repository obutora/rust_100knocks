use polars::{prelude::*, lazy::dsl::GetOutput, export::chrono::{Utc, TimeZone}};

fn main() {
    let recept_path = "100knocks-preprocess/docker/work/data/receipt.csv";
    // let store_path = "100knocks-preprocess/docker/work/data/store.csv";
    // let customer_path = "100knocks-preprocess/docker/work/data/customer.csv";
    // let product_path = "100knocks-preprocess/docker/work/data/product.csv";
    // let category_path = "100knocks-preprocess/docker/work/data/category.csv";

    fn to_str_series(date: &Series) -> Series{
        date.i64()
        .unwrap()
        .into_iter()
        .map(|date| match date {
            // Some(date) => date.to_string(),
            Some(date) => Utc.timestamp_opt(date, 0).unwrap().to_string(),
            None => "".to_string(),
        })
        .collect()
    }

    let customer_df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .select([
            col("receipt_no"),
            col("receipt_sub_no"),
            // col("sales_epoch")
            col("sales_epoch").map(|s| Ok(to_str_series(&s)), GetOutput::default())
        ])
        .select([
            col("receipt_no"),
            col("receipt_sub_no"),
            col("sales_epoch").str().strptime(StrpTimeOptions {
                    fmt: Some("%Y-%m-%d".to_string()),
                    date_dtype: DataType::Date,
                    ..Default::default()
                })
        ])
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", customer_df);
}
