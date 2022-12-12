use polars::prelude::*;
fn main() {
    let recept_path = "100knocks-preprocess/docker/work/data/receipt.csv";
    // let store_path = "100knocks-preprocess/docker/work/data/store.csv";
    let customer_path = "100knocks-preprocess/docker/work/data/customer.csv";
    // let product_path = "100knocks-preprocess/docker/work/data/product.csv";
    // let category_path = "100knocks-preprocess/docker/work/data/category.csv";

    let recept_df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap();

    fn calc_era(age: &Series) -> Series {
        age.i64()
            .unwrap()
            .into_iter()
            .map(|age| match age {
                Some(age) => (age as f64 / 10.0).floor() * 10.0,
                None => 0f64,
            })
            .collect()
    }

    let mut customer_df = LazyCsvReader::new(customer_path)
        .has_header(true)
        .finish()
        .unwrap()
        .select([
            col("customer_id"),
            col("gender_cd"),
            col("age").alias("era"),
        ])
        .collect()
        .unwrap();

    customer_df.apply("era", calc_era).unwrap(); //era列を追加

    let joined = customer_df
        .lazy()
        .left_join(recept_df, col("customer_id"), col("customer_id"))
        .groupby([col("gender_cd"), col("era")])
        .agg([col("amount").sum().alias("sum")]);

    let male = joined
        .clone()
        .filter(col("gender_cd").eq(0))
        .select([col("era"), col("sum").alias("male")]);

    let female = joined
        .clone()
        .filter(col("gender_cd").eq(1))
        .select([col("era"), col("sum").alias("female")]);

    let unknown = joined
        .clone()
        .filter(col("gender_cd").eq(9))
        .select([col("era"), col("sum").alias("unknown")]);

    let merged = male
        .left_join(female, col("era"), col("era"))
        .left_join(unknown, col("era"), col("era"))
        .sort(
            "era",
            SortOptions {
                descending: (false),
                nulls_last: (true),
            },
        )
        .collect()
        .unwrap();

    println!("{:?}", merged);
}
