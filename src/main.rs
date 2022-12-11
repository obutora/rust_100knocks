use polars::prelude::*;

fn main() {
    // let recept_path = "100knocks-preprocess/docker/work/data/receipt.csv";
    let store_path = "100knocks-preprocess/docker/work/data/store.csv";
    // let customer_path = "100knocks-preprocess/docker/work/data/customer.csv";
    let product_path = "100knocks-preprocess/docker/work/data/product.csv";
    // let category_path = "100knocks-preprocess/docker/work/data/category.csv";

    let store_df = LazyCsvReader::new(store_path)
        .has_header(true)
        .finish()
        .unwrap();

    let product_df = LazyCsvReader::new(product_path)
        .has_header(true)
        .finish()
        .unwrap();

    let joined = store_df
        .join(product_df, vec![], vec![], JoinType::Cross)
        .collect()
        .unwrap();

    println!("{:?}", joined);
}
