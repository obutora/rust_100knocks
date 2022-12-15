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
                Some(val) => (val as f64).log10(),
                None => 0f64,
            })
            .collect()
    }

    let recept_df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .groupby([col("customer_id")])
        .agg([col("amount")
            .sum()
            .map(|s| Ok(to_log(&s)), GetOutput::default())
            .alias("amount_log")])
        .with_columns([
            col("amount_log").mean().alias("mean"),
            col("amount_log").std(0).alias("std"),
        ])
        .with_columns([
            (col("mean") - (col("std") * lit(3))).alias("minus_3_sigma"),
            (col("mean") + (col("std") * lit(3))).alias("up_3_sigma"),
        ])
        .with_columns([
            col("amount_log")
                .lt(col("minus_3_sigma"))
                .alias("minus_flag"),
            col("amount_log").gt(col("up_3_sigma")).alias("up_flag"),
        ])
        .filter(
            col("minus_flag")
                .eq(lit(true))
                .or(col("up_flag").eq(lit(true))),
        )
        .collect()
        .unwrap()
        .head(Some(10));

    //
    println!("{:?}", recept_df);
}
