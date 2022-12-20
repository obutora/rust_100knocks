use polars::prelude::*;

fn main() {
    // let recept_path = "100knocks-preprocess/docker/work/data/receipt.csv";
    // let store_path = "100knocks-preprocess/docker/work/data/store.csv";
    let customer_path = "100knocks-preprocess/docker/work/data/customer.csv";
    // let product_path = "100knocks-preprocess/docker/work/data/product.csv";
    // let category_path = "100knocks-preprocess/docker/work/data/category.csv";
    // let geocode_path = "100knocks-preprocess/docker/work/data/geocode.csv";

    let customer_df = LazyCsvReader::new(customer_path)
        .has_header(true)
        .finish()
        .unwrap()
        .select([col("gender_cd"), col("gender")])
        // .unique(Some(["gender_cd", "gender"]), UniqueKeepStrategy::First)
        .collect()
        .unwrap();

    println!("{:?}", customer_df);
}
