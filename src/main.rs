use polars::{prelude::*, lazy::dsl::when};

fn main() {
    // let recept_path = "100knocks-preprocess/docker/work/data/receipt.csv";
    // let store_path = "100knocks-preprocess/docker/work/data/store.csv";
    // let customer_path = "100knocks-preprocess/docker/work/data/customer.csv";
    let product_path = "100knocks-preprocess/docker/work/data/product.csv";
    // let category_path = "100knocks-preprocess/docker/work/data/category.csv";

    let std_df = LazyCsvReader::new(product_path)
        .has_header(true)
        .finish()
        .unwrap()
        .groupby([col("category_small_cd")])
        .agg([
            col("unit_price").std(0).alias("std_price"),
            col("unit_cost").std(0).alias("std_cost")
        ]);

    let product_df = LazyCsvReader::new(product_path)
        .has_header(true)
        .finish()
        .unwrap()
        .with_columns([
            col("unit_price").is_null().alias("price_flag"),
            col("unit_cost").is_null().alias("cost_flag"),
        ]);

    let joined = product_df.inner_join(std_df, col("category_small_cd"), col("category_small_cd"))
        .with_columns([
            when(col("price_flag").eq(lit(true))).then(col("std_price")).otherwise(col("unit_price")).alias("unit_price"),
            when(col("cost_flag").eq(lit(true))).then(col("std_cost")).otherwise(col("unit_cost")).alias("unit_cost"),
        ])
        .select([
            col("product_cd"),
            col("category_major_cd"),
            col("unit_price"),
            col("price_flag"),
            col("std_price"),
        ])
        // .filter(col("price_flag").eq(lit(true))) //フラグが立っているところが中央値になっているか確認用
        .collect()
        .unwrap();
    
    println!("{:?}", joined);
}
