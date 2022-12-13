use polars::{lazy::dsl::GetOutput, prelude::*};

fn main() {
    let recept_path = "100knocks-preprocess/docker/work/data/receipt.csv";
    // let store_path = "100knocks-preprocess/docker/work/data/store.csv";
    // let customer_path = "100knocks-preprocess/docker/work/data/customer.csv";
    // let product_path = "100knocks-preprocess/docker/work/data/product.csv";
    // let category_path = "100knocks-preprocess/docker/work/data/category.csv";

    fn define_quantile(amount: &Series) -> Series {
        let q1 = amount
            .quantile_as_series(0.25f64, QuantileInterpolOptions::Nearest)
            .unwrap()
            .f64()
            .unwrap()
            .get(0)
            .unwrap();

        let q2 = amount
            .quantile_as_series(0.5f64, QuantileInterpolOptions::Nearest)
            .unwrap()
            .f64()
            .unwrap()
            .get(0)
            .unwrap();

        let q3 = amount
            .quantile_as_series(0.75f64, QuantileInterpolOptions::Nearest)
            .unwrap()
            .f64()
            .unwrap()
            .get(0)
            .unwrap();

        amount
            .i64()
            .unwrap()
            .into_iter()
            .map(|amount| match amount {
                Some(amount) => {
                    if amount as f64 <= q1 {
                        1
                    } else if amount as f64 <= q2 {
                        2
                    } else if amount as f64 <= q3 {
                        3
                    } else {
                        4
                    }
                }
                None => 0,
            })
            .collect()
    }

    let recept_df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .groupby([col("customer_id")])
        .agg([col("amount").sum().alias("total_amount")])
        .with_column(
            col("total_amount")
                .map(|s| Ok(define_quantile(&s)), GetOutput::default())
                .alias("quantile"),
        )
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

    println!("{}", recept_df);
}
