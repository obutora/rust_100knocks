use polars::prelude::*;

fn main() {
    let recept_path = "100knocks-preprocess/docker/work/data/receipt.csv";
    // let store_path = "100knocks-preprocess/docker/work/data/store.csv";
    // let customer_path = "100knocks-preprocess/docker/work/data/customer.csv";
    // let product_path = "100knocks-preprocess/docker/work/data/product.csv";
    // let category_path = "100knocks-preprocess/docker/work/data/category.csv";

    // col("amount")
    //             .quantile(0.25f64, QuantileInterpolOptions::Nearest)
    //             .alias("q1"),

    let recept_df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .filter(col("customer_id").str().contains("^[A-Y]"))
        .groupby([col("customer_id")])
        .agg([col("amount").sum().alias("amount")])
        .with_columns([
            col("amount")
                .quantile(0.25f64, QuantileInterpolOptions::Nearest)
                .alias("q1"),
            col("amount")
                .quantile(0.75f64, QuantileInterpolOptions::Nearest)
                .alias("q3"),
        ])
        .with_column((col("q3") - col("q1")).alias("iqr"))
        .with_columns([
            (col("q1") - (col("iqr") * lit(1.5))).alias("lower"),
            (col("q3") + (col("iqr") * lit(1.5))).alias("upper"),
        ])
        .with_columns([
            col("amount").lt(col("lower")).alias("lower_flag"),
            col("amount").gt(col("upper")).alias("upeer_flag"),
        ])
        .filter(
            col("lower_flag")
                .eq(lit(true))
                .or(col("upeer_flag").eq(lit(true))),
        )
        .sort(
            "customer_id",
            SortOptions {
                descending: (false), //高齢順にソート とは、誕生日を昇順にソートすること
                nulls_last: (true),
            },
        )
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", recept_df);
}
