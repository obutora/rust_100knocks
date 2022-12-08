use polars::prelude::*;

fn main() {
    let recept_path = "100knocks-preprocess/docker/work/data/receipt.csv";

    let df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .select([
            col("sales_ymd"),
            col("customer_id"),
            col("product_cd"),
            col("amount"),
        ])
        .filter(col("customer_id").str().contains("CS018205000001"))
        .filter(col("amount").gt(lit(1000)))
        .collect()
        .unwrap();
    // .head(Some(10));

    println!("{:?}", df);
}
