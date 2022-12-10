use polars::prelude::*;

fn main() {
    let recept_path = "100knocks-preprocess/docker/work/data/receipt.csv";
    // let store_path = "100knocks-preprocess/docker/work/data/store.csv";
    // let customer_path = "100knocks-preprocess/docker/work/data/customer.csv";

    let df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .select([
            col("store_cd"),
            col("product_cd")
        ])
        .groupby([col("store_cd"), col("product_cd")])
        .agg([
            col("product_cd").count().alias("count"),
        ])
        .sort(
            "count",
            SortOptions {
                descending: (true), 
                nulls_last: (true),
            },
        )
        .sort(
            "store_cd",
            SortOptions {
                descending: (false), 
                nulls_last: (true),
            },
        )
        .filter(col("store_cd"))
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", df);
}
