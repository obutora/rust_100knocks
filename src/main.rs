use polars::prelude::*;

fn main() {
    let recept_path = "100knocks-preprocess/docker/work/data/receipt.csv";
    // let store_path = "100knocks-preprocess/docker/work/data/store.csv";
    // let customer_path = "100knocks-preprocess/docker/work/data/customer.csv";
    let product_path = "100knocks-preprocess/docker/work/data/product.csv";
    // let category_path = "100knocks-preprocess/docker/work/data/category.csv";

    let recept_df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap();

    let product_df = LazyCsvReader::new(product_path)
        .has_header(true)
        .finish()
        .unwrap();

    let joined = recept_df
        .inner_join(product_df, "product_cd", "product_cd")
        .select([col("customer_id"), col("category_major_cd"), col("amount")])
        .with_column(
            when(col("category_major_cd").eq(7))
                .then(col("amount"))
                .otherwise(0)
                .alias("7_amount"),
        )
        .groupby([col("customer_id")])
        .agg([
            col("amount").sum().alias("total_amount"),
            col("7_amount").sum().alias("7_amount"),
        ])
        .select([
            col("*"),
            (col("7_amount").cast(DataType::Float64) / col("total_amount")).alias("sales_rate"),
        ])
        .sort(
            "customer_id",
            SortOptions {
                descending: (false),
                nulls_last: (true),
            },
        )
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", joined);
}
