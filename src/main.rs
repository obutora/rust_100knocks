use polars::{prelude::*, lazy::dsl::when};

fn main() {
    let recept_path = "100knocks-preprocess/docker/work/data/receipt.csv";
    // let store_path = "100knocks-preprocess/docker/work/data/store.csv";
    let customer_path = "100knocks-preprocess/docker/work/data/customer.csv";
    // let product_path = "100knocks-preprocess/docker/work/data/product.csv";
    // let category_path = "100knocks-preprocess/docker/work/data/category.csv";

    let customer_df = LazyCsvReader::new(customer_path)
        .has_header(true)
        .finish()
        .unwrap();
    
    let recept_df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap();
    
    
    let joined = customer_df.left_join(recept_df, col("customer_id"), col("customer_id"))
        .fill_null(lit(0))
        .with_column(
            when(col("sales_ymd")
                .gt(lit(20190000))
                .and(col("sales_ymd").lt(lit(20199999)))
            ).then(col("amount")).otherwise(lit(0)).alias("2019_sales")
        )
        .groupby([col("customer_id")])
        .agg([
            col("amount").sum().alias("total_sum"),
            col("2019_sales").sum().alias("2019_sum")
            ]);
    
    let zero_df = joined.clone()
        .filter(col("2019_sum").eq(lit(0)))
        .with_column(
            lit(0f64).alias("2019_rate")
        );

    let non_zero_df = joined.clone()
        .filter(col("2019_sum").neq(lit(0)))
        .with_column(
            (col("2019_sum").cast(DataType::Float64) / col("total_sum")).alias("2019_rate")
        );

    let result = concat([zero_df, non_zero_df],true, true)
    .unwrap();

    let null_df = result.clone()
        .filter(col("*").is_null());

    println!("{:?}", result.collect().unwrap());
    println!("{}", null_df.collect().unwrap());
}
