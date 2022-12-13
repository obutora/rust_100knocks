use polars::prelude::*;
fn main() {
    // let recept_path = "100knocks-preprocess/docker/work/data/receipt.csv";
    // let store_path = "100knocks-preprocess/docker/work/data/store.csv";
    let customer_path = "100knocks-preprocess/docker/work/data/customer.csv";
    // let product_path = "100knocks-preprocess/docker/work/data/product.csv";
    // let category_path = "100knocks-preprocess/docker/work/data/category.csv";

    fn format_birth(birth : &Series) -> Series {
        birth.utf8()
        .unwrap()
        .into_iter()
        .map(|birth| match birth {
            Some(birth) => birth.replace("-", ""),
            None => "".to_string(),
        })
        .collect()
    }

    let customer_df = LazyCsvReader::new(customer_path)
        .has_header(true)
        .finish()
        .unwrap()
        .select([
            col("customer_id"),
            col("birth_day")
        ])
        .collect()
        .unwrap()
        .apply("birth_day",format_birth)
        .unwrap()
        .head(Some(10));

    println!("{:?}", customer_df);
}
