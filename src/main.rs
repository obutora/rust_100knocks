use polars::prelude::*;

fn main() {
    // let recept_path = "100knocks-preprocess/docker/work/data/receipt.csv";
    // let store_path = "100knocks-preprocess/docker/work/data/store.csv";
    let customer_path = "100knocks-preprocess/docker/work/data/customer.csv";
    // let product_path = "100knocks-preprocess/docker/work/data/product.csv";
    // let category_path = "100knocks-preprocess/docker/work/data/category.csv";

    // fn format_birth(birth : &Series) -> Series {
    //     birth.utf8()
    //     .unwrap()
    //     .into_iter()
    //     .map(|birth| match birth {
    //         Some(birth) => birth.replace("-", ""),
    //         None => "".to_string(),
    //     })
    //     .collect()
    // }

    fn define_prefecture(address: &Series) -> Series {
        address
            .utf8()
            .unwrap()
            .into_iter()
            .map(|address| match address {
                Some(address) => {
                    let pref = &address[0..9]; //漢字は3バイト
                    match pref {
                        "埼玉県" => 11,
                        "千葉県" => 12,
                        "東京都" => 13,
                        "神奈川" => 14,
                        &_ => 0,
                    }
                }
                None => 0,
            })
            .collect()
    }

    let customer_df = LazyCsvReader::new(customer_path)
        .has_header(true)
        .finish()
        .unwrap()
        .select([col("customer_id"), col("address")])
        .with_column(
            col("address")
                .map(|s| Ok(define_prefecture(&s)), GetOutput::default())
                .alias("pref"),
        )
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{}", customer_df);
}
