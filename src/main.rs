use polars::{lazy::dsl::GetOutput, prelude::*};

fn main() {
//     let recept_path = "100knocks-preprocess/docker/work/data/receipt.csv";
    // let store_path = "100knocks-preprocess/docker/work/data/store.csv";
    let customer_path = "100knocks-preprocess/docker/work/data/customer.csv";
    // let product_path = "100knocks-preprocess/docker/work/data/product.csv";
    // let category_path = "100knocks-preprocess/docker/work/data/category.csv";

    fn calc_era(age: &Series) -> Series {
        age.i64()
            .unwrap()
            .into_iter()
            .map(|age| match age {
                Some(age) => {
                    let era = ((age as f64 / 10.0).floor()) * 10.0;

                    if age > 60 {
                        return 60f64
                    } else {
                        return era
                    }
                },
                None => 0f64,
            })
            .collect()
    }


    let customer_df = LazyCsvReader::new(customer_path)
        .has_header(true)
        .finish()
        .unwrap()
        .with_column(col("age")
        .map(|s| Ok(calc_era(&s)), GetOutput::default())
                .alias("era"),
            )
        .select([
            col("customer_id"),
            col("gender_cd"),//本来指定なかったが、検証に便利なので入れている
            col("birth_day"),
            col("age"), //本来指定なかったが、検証に便利なので入れている
            col("era"),
            fold_exprs(lit(0), |a, b| Ok(&a + &b), [
                col("gender_cd"),
                col("era")
            ]).alias("gender_era")
        ])
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", customer_df);
}
