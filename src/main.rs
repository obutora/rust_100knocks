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
            col("quantity"),
            col("amount"),
        ])
        .filter(col("customer_id").str().contains("CS018205000001"))
        .filter(col("product_cd").str().contains("[^(P071401019)]"))
        .collect()
        .unwrap();

    println!("{:?}", df);
}
