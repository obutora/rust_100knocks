use polars::prelude::*;

fn main() {
    // let recept_path = "100knocks-preprocess/docker/work/data/receipt.csv";
    // let store_path = "100knocks-preprocess/docker/work/data/store.csv";
    let customer_path = "100knocks-preprocess/docker/work/data/customer.csv";
    // let product_path = "100knocks-preprocess/docker/work/data/product.csv";
    // let category_path = "100knocks-preprocess/docker/work/data/category.csv";
    let geocode_path = "100knocks-preprocess/docker/work/data/geocode.csv";

    let customer_df = LazyCsvReader::new(customer_path)
        .has_header(true)
        .finish()
        .unwrap();
    
    let geocode_df = LazyCsvReader::new(geocode_path)
        .has_header(true)
        .finish()
        .unwrap()
        .groupby([col("postal_cd")])
        .agg([
            col("longitude").mean().alias("m_lng"),
            col("latitude").mean().alias("m_lat")
            ]);

    let joined = customer_df.inner_join(geocode_df, col("postal_cd"), col("postal_cd"))
        .collect()
        .unwrap()
        .head(Some(10));
    
    println!("{:?}", joined);
}
