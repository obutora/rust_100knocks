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
        .with_columns([col("*").is_null().alias("flag")])
        .collect()
        .unwrap()
        .fill_null(FillNullStrategy::Mean)
        .unwrap()
        .lazy()
        .filter(col("flag").eq(lit(true)))
        .collect()
        .unwrap();

    println!("{:?}", product_df);
}
