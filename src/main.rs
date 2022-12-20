use std::fs::File;

use polars::prelude::*;

fn main() {
    let recept_path = "100knocks-preprocess/docker/work/data/receipt.csv";
    // let store_path = "100knocks-preprocess/docker/work/data/store.csv";
    // let customer_path = "100knocks-preprocess/docker/work/data/customer.csv";
    // let product_path = "100knocks-preprocess/docker/work/data/product.csv";
    // let category_path = "100knocks-preprocess/docker/work/data/category.csv";
    // let geocode_path = "100knocks-preprocess/docker/work/data/geocode.csv";

    let mut df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .select([
            all(),
            col("amount")
                .rank(RankOptions {
                    method: RankMethod::Ordinal,
                    descending: true,
                })
                .alias("rank"),
        ])
        .sort(
            "rank",
            SortOptions {
                descending: (false),
                nulls_last: (true),
            },
        )
        .collect()
        .unwrap();

    println!("{:?}", df);

    let path = std::path::Path::new("./data/output.csv");
    let mut file = File::create(path).expect("could not create file");

    CsvWriter::new(&mut file)
        .has_header(true)
        .finish(&mut df)
        .unwrap();
}
