use polars::prelude::*;

fn main() {
    let recept_path = "100knocks-preprocess/docker/work/data/receipt.csv";
    // let store_path = "100knocks-preprocess/docker/work/data/store.csv";
    let customer_path = "100knocks-preprocess/docker/work/data/customer.csv";
    // let product_path = "100knocks-preprocess/docker/work/data/product.csv";
    // let category_path = "100knocks-preprocess/docker/work/data/category.csv";

    let customer_df = LazyCsvReader::new(customer_path)
        .has_header(true)
        .finish()
        .unwrap()
        .with_column(
            when(
                col("postal_cd")
                    .str()
                    .contains("^[1][0-9][0-9]|^[2][0][0-9]"),
            )
            .then(1)
            .otherwise(0)
            .alias("flag"),
        )
        .select([col("customer_id"), col("postal_cd"), col("flag")])
        .sort(
            "customer_id",
            SortOptions {
                descending: (false),
                nulls_last: (true),
            },
        );

    let recept_df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap();

    let joined = customer_df
        .inner_join(recept_df, col("customer_id"), col("customer_id"))
        .select([col("customer_id"), col("flag")])
        .unique(None, UniqueKeepStrategy::First) //customer_idで重複しているものがあるので削除
        .groupby([col("flag")])
        .agg([col("flag").count().alias("flag_count")])
        .collect()
        .unwrap();

    println!("{}", joined);
}
