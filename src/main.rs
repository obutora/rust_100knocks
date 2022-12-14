use polars::prelude::*;

fn main() {
    let recept_path = "100knocks-preprocess/docker/work/data/receipt.csv";
    // let store_path = "100knocks-preprocess/docker/work/data/store.csv";
    // let customer_path = "100knocks-preprocess/docker/work/data/customer.csv";
    // let product_path = "100knocks-preprocess/docker/work/data/product.csv";
    // let category_path = "100knocks-preprocess/docker/work/data/category.csv";

    fn to_log(val: &Series) -> Series {
        val.i64()
            .unwrap()
            .into_iter()
            .map(|val| match val {
                Some(val) => (val as f64).ln(),
                None => 0f64,
            })
            .collect()
    }

    let recept_df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .filter(col("customer_id").str().contains("^[A-Y]"))
        .groupby([col("customer_id")])
        .agg([
            col("amount").sum().alias("amount"),
        ])
        .with_column(
            col("amount")
            .map(|s| Ok(to_log(&s)), GetOutput::default())
                .alias("log"),
        )
        .sort(
            "customer_id",
            SortOptions {
                descending: (false),
                nulls_last: (true),
            },
        )
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", recept_df);
}
