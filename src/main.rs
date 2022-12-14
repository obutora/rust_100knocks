use polars::prelude::*;

fn main() {
    // let recept_path = "100knocks-preprocess/docker/work/data/receipt.csv";
    // let store_path = "100knocks-preprocess/docker/work/data/store.csv";
    // let customer_path = "100knocks-preprocess/docker/work/data/customer.csv";
    let product_path = "100knocks-preprocess/docker/work/data/product.csv";
    // let category_path = "100knocks-preprocess/docker/work/data/category.csv";


    let product_df = LazyCsvReader::new(product_path)
        .has_header(true)
        .finish()
        .unwrap()
        .select([
            col("*"),
            (col("unit_price") - col("unit_cost")).alias("unit_profit")
        ])
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", product_df);
}
