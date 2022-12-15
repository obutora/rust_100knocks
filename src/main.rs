use polars::prelude::*;

fn main() {
    // let recept_path = "100knocks-preprocess/docker/work/data/receipt.csv";
    // let store_path = "100knocks-preprocess/docker/work/data/store.csv";
    let customer_path = "100knocks-preprocess/docker/work/data/customer.csv";
    // let product_path = "100knocks-preprocess/docker/work/data/product.csv";
    // let category_path = "100knocks-preprocess/docker/work/data/category.csv";

    let customer_df = LazyCsvReader::new(customer_path)
        .has_header(true)
        .finish()
        .unwrap();

    let male_df = customer_df.clone().filter(col("gender_cd").eq(0));
    let female_df = customer_df.clone().filter(col("gender_cd").eq(1));
    let unknown_df = customer_df.clone().filter(col("gender_cd").eq(9));

    let male_count = male_df.clone().collect().unwrap().height() / 10;
    let female_count = female_df.clone().collect().unwrap().height() / 10;
    let unknown_count = unknown_df.clone().collect().unwrap().height() / 10;

    let male_sample = male_df
        .collect()
        .unwrap()
        .sample_n(male_count, true, true, None)
        .unwrap()
        .lazy();

    let female_sample = female_df
        .collect()
        .unwrap()
        .sample_n(female_count, true, true, None)
        .unwrap()
        .lazy();

    let unknown_sample = unknown_df
        .collect()
        .unwrap()
        .sample_n(unknown_count, true, true, None)
        .unwrap()
        .lazy();

    let sample = concat([male_sample, female_sample, unknown_sample], true, true)
        .unwrap()
        .groupby([col("gender_cd")])
        .agg([col("gender_cd").count().alias("count")])
        .collect()
        .unwrap();
    //
    println!("{:?}", sample);
}
