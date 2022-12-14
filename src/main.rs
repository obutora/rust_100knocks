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
        .filter(col("*").is_not_null())
        .select([
            ((col("unit_price") - col("unit_cost")).cast(DataType::Float32) / col("unit_price")).alias("rate")
        ])
        .mean()
        .collect()
        .unwrap();

    println!("{}", product_df.get_columns()[0].get(0));
}
