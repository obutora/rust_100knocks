use polars::prelude::*;

fn main() {
    let recept_path = "100knocks-preprocess/docker/work/data/receipt.csv";
    // let store_path = "100knocks-preprocess/docker/work/data/store.csv";
    // let customer_path = "100knocks-preprocess/docker/work/data/customer.csv";

    let df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .groupby([col("customer_id")])
        .agg([
            col("sales_ymd").min().alias("sales_ymd_min"),
            col("sales_ymd").max().alias("sales_ymd_max"),
        ])
        .filter(col("sales_ymd_min").neq(col("sales_ymd_max")))
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", df);
}
