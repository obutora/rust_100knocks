use polars::prelude::*;

fn main() {
    let recept_path = "100knocks-preprocess/docker/work/data/receipt.csv";
    // let store_path = "100knocks-preprocess/docker/work/data/store.csv";
    // let customer_path = "100knocks-preprocess/docker/work/data/customer.csv";

    let df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .groupby([col("store_cd")])
        .agg([
            col("amount").mean().alias("avg"),
        ])
        .sort(
            "avg",
            SortOptions {
                descending: (true), 
                nulls_last: (true),
            },
        )
        .collect()
        .unwrap()
        .head(Some(5));

    println!("{:?}", df);
}
