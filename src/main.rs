use polars::{lazy::dsl::GetOutput, prelude::*, export::num::ToPrimitive};

fn main() {
//     let recept_path = "100knocks-preprocess/docker/work/data/receipt.csv";
    // let store_path = "100knocks-preprocess/docker/work/data/store.csv";
    let customer_path = "100knocks-preprocess/docker/work/data/customer.csv";
    // let product_path = "100knocks-preprocess/docker/work/data/product.csv";
    // let category_path = "100knocks-preprocess/docker/work/data/category.csv";

    fn to_dummy(val: &Series, code:i8) -> Series {
        val.i64()
            .unwrap()
            .into_iter()
            .map(|val| match val {
                Some(val) => {
                    if val == code.to_i64().unwrap() {
                        return 1i64
                    } else {
                        return 0i64
                    }
                },
                None => 0i64,
            })
            .collect()
    }


    let customer_df = LazyCsvReader::new(customer_path)
        .has_header(true)
        .finish()
        .unwrap()
        .with_columns([
            col("gender_cd").map(|s| Ok(to_dummy(&s, 0)), GetOutput::default()).alias("gender_0"),
            col("gender_cd").map(|s| Ok(to_dummy(&s, 1)), GetOutput::default()).alias("gender_1"),
            col("gender_cd").map(|s| Ok(to_dummy(&s, 9)), GetOutput::default()).alias("gender_9"),
        ])
        .select([
            col("customer_id"),
            col("gender_cd"),//本来指定なかったが、検証に便利なので入れている
            col("gender_0"),
            col("gender_1"),
            col("gender_9"),
        ])
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", customer_df);
}
