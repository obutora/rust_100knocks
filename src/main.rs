use polars::prelude::*;

fn main() {
    let recept_path = "100knocks-preprocess/docker/work/data/receipt.csv";
    // let store_path = "100knocks-preprocess/docker/work/data/store.csv";
    // let customer_path = "100knocks-preprocess/docker/work/data/customer.csv";
    // let product_path = "100knocks-preprocess/docker/work/data/product.csv";
    // let category_path = "100knocks-preprocess/docker/work/data/category.csv";
    // let geocode_path = "100knocks-preprocess/docker/work/data/geocode.csv";

    fn ymd_to_ym(ymd: &Series) -> Series {
        ymd.i64()
            .unwrap()
            .into_iter()
            .map(|ymd| match ymd {
                Some(ymd) => (ymd as f64 / 100.0).floor() as i32,
                None => 0i32,
            })
            .collect()
    }


    let recept_df = LazyCsvReader::new(recept_path)
    .has_header(true)
    .finish()
    .unwrap()
    .select([
        all(),
        col("sales_ymd").map(|s| Ok(ymd_to_ym(&s)), GetOutput::default()).alias("sales_ym")
    ])
    .groupby([col("sales_ym")])
    .agg([col("amount").sum().alias("amount")])
    .sort(
        "sales_ym",
        SortOptions {
            descending: (false),
            nulls_last: (true),
        },
    )
    .collect()
    .unwrap();

    let train1 = recept_df.clone().slice(0, 12);
    let val1 = recept_df.clone().slice(12, 6);

    let train2 = recept_df.slice(6, 12);
    let val2 = recept_df.slice(18, 6);

    let train3 = recept_df.slice(12, 12);
    let val3 = recept_df.slice(24, 6);

    println!("{:?}", train3);
    println!("{:?}", val3);
    
}


//NOTE: A89
// let customer_df = LazyCsvReader::new(customer_path)
//         .has_header(true)
//         .finish()
//         .unwrap();

//     let recept_df = LazyCsvReader::new(recept_path)
//     .has_header(true)
//     .finish()
//     .unwrap()
//     .groupby([col("customer_id")])
//     .agg([col("amount").sum().alias("amount")])
//     .filter(col("amount").gt(0));
    

//     let joined = customer_df.inner_join(recept_df, col("customer_id"), col("customer_id"))
//         .collect()
//         .unwrap()
//         .sample_frac(1.0, false, true, None)
//         .unwrap();

//     let shape = joined.shape();
//     println!("origin: {:?}", shape);
    
//     let train = joined.slice(0, ((shape.0 as f32) * 0.8).floor() as usize);
//     let test = joined.slice(((shape.0 as f32) * 0.8).floor() as i64, ((shape.0 as f32) * 0.2).round() as usize);

//     println!("{:?}", train.shape());
//     println!("{:?}", test.shape());

//     println!("{:?}", train.tail(Some(10)));
//     println!("{:?}", test.head(Some(10)));


//NOTE: A90
// fn ymd_to_ym(ymd: &Series) -> Series {
//     ymd.i64()
//         .unwrap()
//         .into_iter()
//         .map(|ymd| match ymd {
//             Some(ymd) => (ymd as f64 / 100.0).floor() as i32,
//             None => 0i32,
//         })
//         .collect()
// }


// let recept_df = LazyCsvReader::new(recept_path)
// .has_header(true)
// .finish()
// .unwrap()
// .select([
//     all(),
//     col("sales_ymd").map(|s| Ok(ymd_to_ym(&s)), GetOutput::default()).alias("sales_ym")
// ])
// .groupby([col("sales_ym")])
// .agg([col("amount").sum().alias("amount")])
// .sort(
//     "sales_ym",
//     SortOptions {
//         descending: (false),
//         nulls_last: (true),
//     },
// )
// .collect()
// .unwrap();

// let train1 = recept_df.clone().slice(0, 12);
// let val1 = recept_df.clone().slice(12, 6);

// let train2 = recept_df.slice(6, 12);
// let val2 = recept_df.slice(18, 6);

// let train3 = recept_df.slice(12, 12);
// let val3 = recept_df.slice(24, 6);

// println!("{:?}", train3);
// println!("{:?}", val3);