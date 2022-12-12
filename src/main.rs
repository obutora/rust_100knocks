use polars::prelude::*;

fn main() {
    let recept_path = "100knocks-preprocess/docker/work/data/receipt.csv";
    // let store_path = "100knocks-preprocess/docker/work/data/store.csv";
    // let customer_path = "100knocks-preprocess/docker/work/data/customer.csv";
    // let product_path = "100knocks-preprocess/docker/work/data/product.csv";
    // let category_path = "100knocks-preprocess/docker/work/data/category.csv";

    let recept_df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .groupby([col("sales_ymd")])
        .agg([col("amount").sum().alias("today")])
        .sort(
            "sales_ymd",
            SortOptions {
                descending: (false),
                nulls_last: (true),
            },
        )
        .select([
            col("sales_ymd"),
            col("today"),
            col("today").shift(1).alias("past1"),
            col("today").shift(2).alias("past2"),
            col("today").shift(3).alias("past3"),
        ])
        .collect()
        .unwrap()
        .head(Some(10));


    println!("{:?}", recept_df);
}
