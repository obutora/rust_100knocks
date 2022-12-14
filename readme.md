# rust でデータ分析 100 本ノックをやる

## 準備

python でデータ分析を行う場合、pandas を使うのが一般的です。  
rust の場合、データ分析を行う場合のライブラリはいくつかありますが、ドキュメントが一番充実している polars を選択しました。

今回は、polars の仲でもさらに`Lazy`API を使って問題を解いていきます。  
`Lazy`を使うと、`LazyFrame.collect()`もしくは`LazyFrame.fetch()`を呼び出すまで実際の計算を実行しないようになります。これにより、Polars がクエリの最適化を行い、最速のアルゴリズムが選択されるようになるとのこと。実行用ファイルのサイズも上昇するので諸刃の剣ではありますが、今回は Polars のパフォーマンスも確認していきたいので、`Lazy`API を使って問題を解いていきます。

また、いくつかのクエリを実行するために読み込まなければいけないオプションがあるので、`Cargo.toml`に以下のように追記します。

```toml
[dependencies]
polars = {version = "0.25.1", features=["describe", "lazy", "strings", "rank"]}
```

csv の読み込みは以下の通り。

```rust
let recept_path = "csvへのパス";

let df: LazyFrame = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap();

println!("{:?}", df.collect().unwrap());
```

## memo

## 比較演算子

```
not equal : neq
gt_eq : >=
lt_eq : <=
```

## 未解決問題

- P19,P20 : Rank を算出する方法が分からない
- P59 : 売上金額合計を平均 0、標準偏差 1 に標準化が出来ていないので解けていない
- P60 : normalization を見つけられず、1 から実装するのも趣旨と異なる気がしたので一旦保留

### P-001: レシート明細データ（df_receipt）から全項目の先頭 10 件を表示し、どのようなデータを保有しているか目視で確認せよ。

```rust
let df = LazyCsvReader::new(recept_path)
    .has_header(true)
    .finish()
    .unwrap()
    .collect()
    .unwrap()
    .head(Some(10));

println!("{:?}", df);
```

### P-002: レシート明細データ（df_receipt）から売上年月日（sales_ymd）、顧客 ID（customer_id）、商品コード（product_cd）、売上金額（amount）の順に列を指定し、10 件表示せよ。

```rust
let df = LazyCsvReader::new(recept_path)
    .has_header(true)
    .finish()
    .unwrap()
    .select([
        col("sales_ymd"),
        col("customer_id"),
        col("product_cd"),
        col("amount"),
    ])
    .collect()
    .unwrap()
    .head(Some(10));

println!("{:?}", df);
```

### P-003: レシート明細データ（df_receipt）から売上年月日（sales_ymd）、顧客 ID（customer_id）、商品コード（product_cd）、売上金額（amount）の順に列を指定し、10 件表示せよ。ただし、sales_ymd->sales_date に項目名を変更しながら抽出すること。

```rust
let df = LazyCsvReader::new(recept_path)
    .has_header(true)
    .finish()
    .unwrap()
    .select([
        col("sales_ymd").alias("sales_date"), // .alias() で項目名を変更できる。pandasより直感的かも。
        col("customer_id"),
        col("product_cd"),
        col("amount"),
    ])
    .collect()
    .unwrap()
    .head(Some(10));

println!("{:?}", df);

```

### P-004: レシート明細データ（df_receipt）から売上日（sales_ymd）、顧客 ID（customer_id）、商品コード（product_cd）、売上金額（amount）の順に列を指定し、以下の条件を満たすデータを抽出せよ。

> 顧客 ID（customer_id）が"CS018205000001"

```rust
let df = LazyCsvReader::new(recept_path)
    .has_header(true)
    .finish()
    .unwrap()
    .select([
        col("sales_ymd"),
        col("customer_id"),
        col("product_cd"),
        col("amount"),
    ])
    .filter(col("customer_id").str().contains("CS018205000001"))
    .collect()
    .unwrap();

println!("{:?}", df);

```

### P-005: レシート明細データ（df_receipt）から売上日（sales_ymd）、顧客 ID（customer_id）、商品コード（product_cd）、売上金額（amount）の順に列を指定し、以下の全ての条件を満たすデータを抽出せよ。

> 顧客 ID（customer_id）が"CS018205000001"
> 売上金額（amount）が 1,000 以上

```rust
let df = LazyCsvReader::new(recept_path)
    .has_header(true)
    .finish()
    .unwrap()
    .select([
        col("sales_ymd"),
        col("customer_id"),
        col("product_cd"),
        col("amount"),
    ])
    .filter(col("customer_id").str().contains("CS018205000001"))
    .filter(col("amount").gt(lit(1000)))
    .collect()
    .unwrap();

println!("{:?}", df);
```

### P-006: レシート明細データ（df_receipt）から売上日（sales_ymd）、顧客 ID（customer_id）、商品コード（product_cd）、売上数量（quantity）、売上金額（amount）の順に列を指定し、以下の全ての条件を満たすデータを抽出せよ。

> 顧客 ID（customer_id）が"CS018205000001"
> 売上金額（amount）が 1,000 以上または売上数量（quantity）が 5 以上

```rust
let df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .select([
            col("sales_ymd"),
            col("customer_id"),
            col("product_cd"),
            col("quantity"),
            col("amount"),
        ])
        .filter(col("customer_id").str().contains("CS018205000001"))
        .filter(col("amount").gt(1000).or(col("quantity").gt_eq(5)))
        .collect()
        .unwrap();

    println!("{:?}", df);
```

### P-007: レシート明細データ（df_receipt）から売上日（sales_ymd）、顧客 ID（customer_id）、商品コード（product_cd）、売上金額（amount）の順に列を指定し、以下の全ての条件を満たすデータを抽出せよ。

> 顧客 ID（customer_id）が"CS018205000001"
> 売上金額（amount）が 1,000 以上 2,000 以下

```rust
let df = LazyCsvReader::new(recept_path)
    .has_header(true)
    .finish()
    .unwrap()
    .select([
        col("sales_ymd"),
        col("customer_id"),
        col("product_cd"),
        col("amount"),
    ])
    .filter(col("customer_id").str().contains("CS018205000001"))
    .filter(col("amount").gt_eq(1000))
    .filter(col("amount").lt_eq(2000))
    .collect()
    .unwrap()

println!("{:?}", df);

```

### P-008: レシート明細データ（df_receipt）から売上日（sales_ymd）、顧客 ID（customer_id）、商品コード（product_cd）、売上金額（amount）の順に列を指定し、以下の全ての条件を満たすデータを抽出せよ。

> 顧客 ID（customer_id）が"CS018205000001"
> 商品コード（product_cd）が"P071401019"以外

```rust
let df = LazyCsvReader::new(recept_path)
    .has_header(true)
    .finish()
    .unwrap()
    .select([
        col("sales_ymd"),
        col("customer_id"),
        col("product_cd"),
        col("quantity"),
        col("amount"),
    ])
    .filter(col("customer_id").str().contains("CS018205000001"))
    .with_column(when(col("product_cd").str().contains("P071401019")) //not containsを表現できずフラグを立てた
        .then(lit(1))
            .otherwise(0).alias("isExist"))
    .filter(col("isExist").eq(0)) // 作成したフラグでフィルター
    .select([
        col("sales_ymd"),
        col("customer_id"),
        col("product_cd"),
        col("quantity"),
        col("amount"),
    ]) //フラグが計算結果に出ないように再度Select
    .collect()
    .unwrap();

println!("{:?}", df);

```

### P-009: 以下の処理において、出力結果を変えずに OR を AND に書き換えよ。

> df_store.query('not(prefecture_cd == "13" | floor_area > 900)')

```rust
let df = LazyCsvReader::new(store_path)
    .has_header(true)
    .finish()
    .unwrap()
    .filter(col("prefecture_cd").neq(13))
    .filter(col("floor_area").lt_eq(900))
    .collect()
    .unwrap();

println!("{:?}", df);

```

### P-010: 店舗データ（df_store）から、店舗コード（store_cd）が"S14"で始まるものだけ全項目抽出し、10 件表示せよ。

```rust
let df = LazyCsvReader::new(store_path)
        .has_header(true)
        .finish()
        .unwrap()
        .filter(col("store_cd").str().starts_with("S14")) //starts_withを使える
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", df);
```

### P-011: 顧客データ（df_customer）から顧客 ID（customer_id）の末尾が 1 のものだけ全項目抽出し、10 件表示せよ。

```rust
let df = LazyCsvReader::new(customer_path)
        .has_header(true)
        .finish()
        .unwrap()
        .filter(col("customer_id").str().ends_with("1"))
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", df);
```

### P-012: 店舗データ（df_store）から、住所 (address) に"横浜市"が含まれるものだけ全項目表示せよ。

```rust
let df = LazyCsvReader::new(store_path)
        .has_header(true)
        .finish()
        .unwrap()
        .filter(col("address").str().contains("横浜市"))
        .collect()
        .unwrap();

    println!("{:?}", df);
```

### P-013: 顧客データ（df_customer）から、ステータスコード（status_cd）の先頭がアルファベットの A〜F で始まるデータを全項目抽出し、10 件表示せよ。

```rust
let df = LazyCsvReader::new(customer_path)
        .has_header(true)
        .finish()
        .unwrap()
        .filter(col("status_cd").str().contains(r"^[A-F]")) //正規表現で先頭がA-Fのものを抽出
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", df);
```

### P-014: 顧客データ（df_customer）から、ステータスコード（status_cd）の末尾が数字の 1〜9 で終わるデータを全項目抽出し、10 件表示せよ。

```rust
let df = LazyCsvReader::new(customer_path)
        .has_header(true)
        .finish()
        .unwrap()
        .filter(col("status_cd").str().contains(r"[1-9]$")) //正規表現で末尾が1-9のものを抽出
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", df);
```

### P-015: 顧客データ（df_customer）から、ステータスコード（status_cd）の先頭がアルファベットの A〜F で始まり、末尾が数字の 1〜9 で終わるデータを全項目抽出し、10 件表示せよ。

```rust
let df = LazyCsvReader::new(customer_path)
        .has_header(true)
        .finish()
        .unwrap()
        .filter(col("status_cd").str().contains(r"^[A-F]")) //チェインするだけでカンタンに記述できる
        .filter(col("status_cd").str().contains(r"[1-9]$"))
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", df);
```

### P-016: 店舗データ（df_store）から、電話番号（tel_no）が 3 桁-3 桁-4 桁のデータを全項目表示せよ。

```rust
let df = LazyCsvReader::new(store_path)
        .has_header(true)
        .finish()
        .unwrap()
        .filter(
            col("tel_no")
                .str()
                .contains(r"^[0-9]{3}-[0-9]{3}-[0-9]{4}$"),
        )
        .collect()
        .unwrap();

    println!("{:?}", df);
```

### P-017: 顧客データ（df_customer）を生年月日（birth_day）で高齢順にソートし、先頭から全項目を 10 件表示せよ。

```rust
let df = LazyCsvReader::new(customer_path)
        .has_header(true)
        .finish()
        .unwrap()
        .sort(
            "birth_day",
            SortOptions {
                descending: (false), //高齢順にソート とは、誕生日を昇順にソートすること
                nulls_last: (true),
            },
        )
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", df);
```

### P-018: 顧客データ（df_customer）を生年月日（birth_day）で若い順にソートし、先頭から全項目を 10 件表示せよ。

```rust
let df = LazyCsvReader::new(customer_path)
        .has_header(true)
        .finish()
        .unwrap()
        .sort(
            "birth_day",
            SortOptions {
                descending: (true), //若い順にソート とは、誕生日を降順にソートすること
                nulls_last: (true),
            },
        )
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", df);
```

### P-019: レシート明細データ（df_receipt）に対し、1 件あたりの売上金額（amount）が高い順にランクを付与し、先頭から 10 件表示せよ。項目は顧客 ID（customer_id）、売上金額（amount）、付与したランクを表示させること。なお、売上金額（amount）が等しい場合は同一順位を付与するものとする。

```rust
let mut df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .select([
            all(),
            col("amount")
                .rank(RankOptions {
                    method: RankMethod::Min,
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
```

### P-020: レシート明細データ（df_receipt）に対し、1 件あたりの売上金額（amount）が高い順にランクを付与し、先頭から 10 件表示せよ。項目は顧客 ID（customer_id）、売上金額（amount）、付与したランクを表示させること。なお、売上金額（amount）が等しい場合でも別順位を付与すること。

```rust
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
```

### P-021: レシート明細データ（df_receipt）に対し、件数をカウントせよ。

```rust
let df = LazyCsvReader::new(recept_path)
    .has_header(true)
    .finish()
    .unwrap()
    .select([col("customer_id").count().alias("count")])
    .collect()
    .unwrap();

println!("{}", df.get(0).unwrap()[0]);
```

### P-022: レシート明細データ（df_receipt）の顧客 ID（customer_id）に対し、ユニーク件数をカウントせよ。

```rust
let df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .select([col("customer_id").unique().count().alias("customer_count")])
        .collect()
        .unwrap();

println!("{}", df.get(0).unwrap()[0]);
```

### P-023: レシート明細データ（df_receipt）に対し、店舗コード（store_cd）ごとに売上金額（amount）と売上数量（quantity）を合計せよ。

```rust
let df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .groupby([col("store_cd")])
        .agg([
            col("amount").sum().alias("amount"), //aliasをとらないとエラーになるので注意
            col("quantity").sum().alias("quantity"),
        ])
        .collect()
        .unwrap();

println!("{:?}", df);
```

### P-024: レシート明細データ（df_receipt）に対し、顧客 ID（customer_id）ごとに最も新しい売上年月日（sales_ymd）を求め、10 件表示せよ。

```rust
let df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .groupby([col("customer_id")])
        .agg([col("sales_ymd").max().alias("sales_ymd")])
        // .filter(col("customer_id").str().contains("CS001114000005"))
        .collect()
        .unwrap()
        .head(Some(10));

println!("{:?}", df);

```

### P-025: レシート明細データ（df_receipt）に対し、顧客 ID（customer_id）ごとに最も古い売上年月日（sales_ymd）を求め、10 件表示せよ。

```rust
let df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .groupby([col("customer_id")])
        .agg([col("sales_ymd").min().alias("sales_ymd")])
        // .filter(col("customer_id").str().contains("CS001114000005"))
        .collect()
        .unwrap()
        .head(Some(10));

println!("{:?}", df);
```

### P-026: レシート明細データ（df_receipt）に対し、顧客 ID（customer_id）ごとに最も新しい売上年月日（sales_ymd）と古い売上年月日を求め、両者が異なるデータを 10 件表示せよ。

```rust
let df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .groupby([col("customer_id")])
        .agg([
            col("sales_ymd").min().alias("sales_ymd_min"),
            col("sales_ymd").max().alias("sales_ymd_max"),
        ])
        .filter(col("sales_ymd_min").neq(col("sales_ymd_max")))
        .collect()
        .unwrap()
        .head(Some(10));

println!("{:?}", df);
```

### P-027: レシート明細データ（df_receipt）に対し、店舗コード（store_cd）ごとに売上金額（amount）の平均を計算し、降順で TOP5 を表示せよ。

```rust
let df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .groupby([col("store_cd")])
        .agg([
            col("amount").mean().alias("avg"),
        ])
        .sort(
            "avg",
            SortOptions {
                descending: (true),
                nulls_last: (true),
            },
        )
        .collect()
        .unwrap()
        .head(Some(5));

    println!("{:?}", df);
```

### P-028: レシート明細データ（df_receipt）に対し、店舗コード（store_cd）ごとに売上金額（amount）の中央値を計算し、降順で TOP5 を表示せよ。

```rust
let df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .groupby([col("store_cd")])
        .agg([
            col("amount").median().alias("med"),
        ])
        .sort(
            "med",
            SortOptions {
                descending: (true),
                nulls_last: (true),
            },
        )
        .collect()
        .unwrap()
        .head(Some(5));

println!("{:?}", df);
```

### P-029: レシート明細データ（df_receipt）に対し、店舗コード（store_cd）ごとに商品コード（product_cd）の最頻値を求め、10 件表示させよ。

```rust
let df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .select([col("store_cd"), col("product_cd")])
        .groupby([col("store_cd"), col("product_cd")])
        .agg([col("product_cd").count().alias("count")]) //#1 最初にカウント列を作成
        .sort(                                           //#2 多い順に並び替え
            "count",
            SortOptions {
                descending: (true),
                nulls_last: (true),
            },
        )
        .groupby([col("store_cd")])
        .agg([col("product_cd").first().alias("product_cd")]) //#3 最初の値を取得
        .sort(
            "store_cd",
            SortOptions {
                descending: (false),
                nulls_last: (true),
            },
        )
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", df);
```

### P-030: レシート明細データ（df_receipt）に対し、店舗コード（store_cd）ごとに売上金額（amount）の分散を計算し、降順で 5 件表示せよ。

```rust
let df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .groupby([col("store_cd")])
        .agg([col("amount").var(0).alias("var")])
        .sort(
            "var",
            SortOptions {
                descending: (true),
                nulls_last: (true),
            },
        )
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", df);
```

### P-032: レシート明細データ（df_receipt）の売上金額（amount）について、25％刻みでパーセンタイル値を求めよ。

```rust
let df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .select([
            col("amount")
                .quantile(0.25f64, QuantileInterpolOptions::Nearest)
                .alias("q1"),
            col("amount")
                .quantile(0.5f64, QuantileInterpolOptions::Nearest)
                .alias("q2"),
            col("amount")
                .quantile(0.75f64, QuantileInterpolOptions::Nearest)
                .alias("q3"),
            col("amount")
                .quantile(1f64, QuantileInterpolOptions::Nearest)
                .alias("q4"),
        ])
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", df);
```

### P-033: レシート明細データ（df_receipt）に対し、店舗コード（store_cd）ごとに売上金額（amount）の平均を計算し、330 以上のものを抽出せよ。

```rust
let df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .groupby([col("store_cd")])
        .agg([col("amount").mean().alias("mean")])
        .filter(col("mean").gt_eq(330))
        .sort(
            "store_cd",
            SortOptions {
                descending: (false),
                nulls_last: (true),
            },
        )
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", df);
```

### P-034: レシート明細データ（df_receipt）に対し、顧客 ID（customer_id）ごとに売上金額（amount）を合計して全顧客の平均を求めよ。ただし、顧客 ID が"Z"から始まるものは非会員を表すため、除外して計算すること。

```rust
let df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .filter(col("customer_id").str().contains("^[A-Y]"))
        .groupby([col("customer_id")])
        .agg([col("amount").sum().alias("amount_sum")])
        .select([col("amount_sum").mean().alias("amount_mean")])
        .collect()
        .unwrap()
        .mean()
        .head(Some(10));

    println!("{:?}", df);
```

### P-035: レシート明細データ（df_receipt）に対し、顧客 ID（customer_id）ごとに売上金額（amount）を合計して全顧客の平均を求め、平均以上に買い物をしている顧客を抽出し、10 件表示せよ。ただし、顧客 ID が"Z"から始まるものは非会員を表すため、除外して計算すること。

```rust
let df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .filter(col("customer_id").str().contains("^[A-Y]"))
        .groupby([col("customer_id")])
        .agg([col("amount").sum().alias("amount_sum")])
        .with_column(col("amount_sum").mean().alias("amount_mean"))
        .filter(col("amount_sum").gt_eq(col("amount_mean")))
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

    println!("{:?}", df);
```

### P-036: レシート明細データ（df_receipt）と店舗データ（df_store）を内部結合し、レシート明細データの全項目と店舗データの店舗名（store_name）を 10 件表示せよ。

```rust
let recept_df = LazyCsvReader::new(recept_path)
    .has_header(true)
    .finish()
    .unwrap();

let store_df = LazyCsvReader::new(store_path)
    .has_header(true)
    .finish()
    .unwrap()
    .select([col("store_cd"), col("store_name")])
    .collect()
    .unwrap();

let joined = recept_df
    .inner_join(store_df.lazy(), col("store_cd"), col("store_cd"))
    .collect()
    .unwrap()
    .head(Some(10));

println!("{:?}", joined);
```

### P-037: 商品データ（df_product）とカテゴリデータ（df_category）を内部結合し、商品データの全項目とカテゴリデータのカテゴリ小区分名（category_small_name）を 10 件表示せよ。

```rust
let product_df = LazyCsvReader::new(product_path)
        .has_header(true)
        .finish()
        .unwrap();

let category_df = LazyCsvReader::new(category_path)
        .has_header(true)
        .finish()
        .unwrap()
        .select([col("category_small_cd"), col("category_small_name")]);

let joined = product_df
        .inner_join(
            category_df,
            col("category_small_cd"),
            col("category_small_cd"),
        )
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", joined);
```

### P-038: 顧客データ（df_customer）とレシート明細データ（df_receipt）から、顧客ごとの売上金額合計を求め、10 件表示せよ。ただし、売上実績がない顧客については売上金額を 0 として表示させること。また、顧客は性別コード（gender_cd）が女性（1）であるものを対象とし、非会員（顧客 ID が"Z"から始まるもの）は除外すること。

```rust
let customer_df = LazyCsvReader::new(customer_path)
        .has_header(true)
        .finish()
        .unwrap()
        .filter(col("gender_cd").eq(1))
        .filter(col("customer_id").str().contains("^[A-Y]"));

let recept_df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .groupby([col("customer_id")])
        .agg([col("amount").sum().alias("amount_sum")]);

let joined = customer_df
        .left_join(recept_df, col("customer_id"), col("customer_id"))
        .with_column(col("amount_sum").fill_null(lit(0)).alias("amount"))
        .select([col("customer_id"), col("amount")])
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", joined);
```

### P-039: レシート明細データ（df_receipt）から、売上日数の多い顧客の上位 20 件を抽出したデータと、売上金額合計の多い顧客の上位 20 件を抽出したデータをそれぞれ作成し、さらにその 2 つを完全外部結合せよ。ただし、非会員（顧客 ID が"Z"から始まるもの）は除外すること。

```rust
let recept_count = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .filter(col("customer_id").str().contains("^[A-Y]"))
        .groupby([col("customer_id")])
        .agg([col("sales_ymd").n_unique().alias("count")])
        .sort(
            "count",
            SortOptions {
                descending: (true),
                nulls_last: (true),
            },
        )
        .collect()
        .unwrap()
        .head(Some(20));

let recept_amount = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .filter(col("customer_id").str().contains("^[A-Y]"))
        .groupby([col("customer_id")])
        .agg([col("amount").sum().alias("sum")])
        .sort(
            "sum",
            SortOptions {
                descending: (true),
                nulls_last: (true),
            },
        )
        .collect()
        .unwrap()
        .head(Some(20));

let joined = recept_count
        .lazy()
        .outer_join(recept_amount.lazy(), col("customer_id"), col("customer_id"))
        .sort(
            "count",
            SortOptions {
                descending: (true),
                nulls_last: (true),
            },
        )
        .collect()
        .unwrap();

    println!("{:?}", joined)

```

### P-040: 全ての店舗と全ての商品を組み合わせたデータを作成したい。店舗データ（df_store）と商品データ（df_product）を直積し、件数を計算せよ。

> cross_join が上手く呼び出せず失敗してしまう。

```rust
    let store_df = LazyCsvReader::new(store_path)
        .has_header(true)
        .finish()
        .unwrap();

    let product_df = LazyCsvReader::new(product_path)
        .has_header(true)
        .finish()
        .unwrap();

    let joined = store_df
        .join(product_df, vec![], vec![], JoinType::Cross)
        .collect()
        .unwrap();

    println!("{:?}", joined);
```

### P-041: レシート明細データ（df_receipt）の売上金額（amount）を日付（sales_ymd）ごとに集計し、前回売上があった日からの売上金額増減を計算せよ。そして結果を 10 件表示せよ。

```rust
let recept_df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .groupby([col("sales_ymd")])
        .agg([col("amount").sum().alias("today")])
        .sort(
            "sales_ymd",
            SortOptions {
                descending: (false),
                nulls_last: (true),
            },
        )
        .select([
            col("sales_ymd"),
            col("today"),
            col("today").shift(1).alias("past"),
        ])
        .with_columns([
            (col("today") - col("past")).alias("diff")
        ])
        .collect()
        .unwrap()
        .head(Some(10));


    println!("{:?}", recept_df);
```

### P-042: レシート明細データ（df_receipt）の売上金額（amount）を日付（sales_ymd）ごとに集計し、各日付のデータに対し、前回、前々回、3 回前に売上があった日のデータを結合せよ。そして結果を 10 件表示せよ。

```rust
let recept_df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .groupby([col("sales_ymd")])
        .agg([col("amount").sum().alias("today")])
        .sort(
            "sales_ymd",
            SortOptions {
                descending: (false),
                nulls_last: (true),
            },
        )
        .select([
            col("sales_ymd"),
            col("today"),
            col("today").shift(1).alias("past1"),
            col("today").shift(2).alias("past2"),
            col("today").shift(3).alias("past3"),
        ])
        .collect()
        .unwrap()
        .head(Some(10));


    println!("{:?}", recept_df);
```

### P-043： レシート明細データ（df_receipt）と顧客データ（df_customer）を結合し、性別コード（gender_cd）と年代（age から計算）ごとに売上金額（amount）を合計した売上サマリデータを作成せよ。性別コードは 0 が男性、1 が女性、9 が不明を表すものとする。

> ただし、項目構成は年代、女性の売上金額、男性の売上金額、性別不明の売上金額の 4 項目とすること（縦に年代、横に性別のクロス集計）。
> また、年代は 10 歳ごとの階級とすること。

```rust
let recept_df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap();

    fn calc_era(age: &Series) -> Series {
        age.i64()
            .unwrap()
            .into_iter()
            .map(|age| match age {
                Some(age) => (age as f64 / 10.0).floor() * 10.0,
                None => 0f64,
            })
            .collect()
    }

    let mut customer_df = LazyCsvReader::new(customer_path)
        .has_header(true)
        .finish()
        .unwrap()
        .select([
            col("customer_id"),
            col("gender_cd"),
            col("age").alias("era"),
        ])
        .collect()
        .unwrap();

    customer_df.apply("era", calc_era).unwrap(); //era列を追加

    let joined = customer_df
        .lazy()
        .left_join(recept_df, col("customer_id"), col("customer_id"))
        .groupby([col("gender_cd"), col("era")])
        .agg([col("amount").sum().alias("sum")]);

    // どうやってもpivotが呼び出せないので、性別ごとに分けてmergeする戦略へ移行する。
    // lazyFrameを使えるので、遅延評価される。
    let male = joined
        .clone()
        .filter(col("gender_cd").eq(0))
        .select([col("era"), col("sum").alias("male")]);

    let female = joined
        .clone()
        .filter(col("gender_cd").eq(1))
        .select([col("era"), col("sum").alias("female")]);

    let unknown = joined
        .clone()
        .filter(col("gender_cd").eq(9))
        .select([col("era"), col("sum").alias("unknown")]);

    let merged = male
        .left_join(female, col("era"), col("era"))
        .left_join(unknown, col("era"), col("era"))
        .sort(
            "era",
            SortOptions {
                descending: (false),
                nulls_last: (true),
            },
        )
        .collect()
        .unwrap();

    println!("{:?}", merged);
```

### P-044： 043 で作成した売上サマリデータ（df_sales_summary）は性別の売上を横持ちさせたものであった。このデータから性別を縦持ちさせ、年代、性別コード、売上金額の 3 項目に変換せよ。ただし、性別コードは男性を"00"、女性を"01"、不明を"99"とする。

```rust
 let recept_df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap();

    fn calc_era(age: &Series) -> Series {
        age.i64()
            .unwrap()
            .into_iter()
            .map(|age| match age {
                Some(age) => (age as f64 / 10.0).floor() * 10.0,
                None => 0f64,
            })
            .collect()
    }

    fn replace_gender_code(code: &Series) -> Series {
        code.i64()
            .unwrap()
            .into_iter()
            .map(|code| match code {
                Some(code) => match code {
                    0 => "00",
                    1 => "01",
                    _ => "99",
                },
                None => "",
            })
            .collect()
    }

    let mut customer_df = LazyCsvReader::new(customer_path)
        .has_header(true)
        .finish()
        .unwrap()
        .select([
            col("customer_id"),
            col("gender_cd"),
            col("age").alias("era"),
        ])
        .collect()
        .unwrap();

    customer_df.apply("era", calc_era).unwrap(); //era列を追加

    let mut joined = customer_df
        .lazy()
        .inner_join(recept_df, col("customer_id"), col("customer_id"))
        .groupby([col("era"), col("gender_cd")])
        .agg([col("amount").sum().alias("sum")])
        .sort(
            "gender_cd",
            SortOptions {
                descending: (false),
                nulls_last: (true),
            },
        )
        .sort(
            "era",
            SortOptions {
                descending: (false),
                nulls_last: (true),
            },
        )
        .collect()
        .unwrap();

    joined.apply("gender_cd", replace_gender_code).unwrap();

    println!("{:?}", joined);
```

### P-045: 顧客データ（df_customer）の生年月日（birth_day）は日付型でデータを保有している。これを YYYYMMDD 形式の文字列に変換し、顧客 ID（customer_id）とともに 10 件表示せよ。

```rust
fn format_birth(birth : &Series) -> Series {
        birth.utf8()
        .unwrap()
        .into_iter()
        .map(|birth| match birth {
            Some(birth) => birth.replace("-", ""),
            None => "".to_string(),
        })
        .collect()
    }

    let customer_df = LazyCsvReader::new(customer_path)
        .has_header(true)
        .finish()
        .unwrap()
        .select([
            col("customer_id"),
            col("birth_day")
        ])
        .collect()
        .unwrap()
        .apply("birth_day",format_birth)
        .unwrap()
        .head(Some(10));

    println!("{:?}", customer_df);
```

### P-046: 顧客データ（df_customer）の申し込み日（application_date）は YYYYMMDD 形式の文字列型でデータを保有している。これを日付型に変換し、顧客 ID（customer_id）とともに 10 件表示せよ。

```rust
fn to_str_series(date: &Series) -> Series{
        date.i64()
        .unwrap()
        .into_iter()
        .map(|date| match date {
            Some(date) => date.to_string(),
            None => "".to_string(),
        })
        .collect()
    }

    let customer_df = LazyCsvReader::new(customer_path)
        .has_header(true)
        .finish()
        .unwrap()
        .select([
            col("customer_id"),
            col("application_date").map(|s| Ok(to_str_series(&s)), GetOutput::default())
        ])
        .select([
            col("customer_id"),
            col("application_date").str().strptime(StrpTimeOptions {
                    fmt: Some("%Y%m%d".to_string()),
                    date_dtype: DataType::Date,
                    ..Default::default()
                })
        ])
        .collect()
        .unwrap()
        // .apply("application_date", to_str_series)
        // .unwrap()
        .head(Some(10));

    println!("{:?}", customer_df);
```

### P-047: レシート明細データ（df_receipt）の売上日（sales_ymd）は YYYYMMDD 形式の数値型でデータを保有している。これを日付型に変換し、レシート番号（receipt_no）、レシートサブ番号（receipt_sub_no）とともに 10 件表示せよ。

```rust
fn to_str_series(date: &Series) -> Series{
        date.i64()
        .unwrap()
        .into_iter()
        .map(|date| match date {
            Some(date) => date.to_string(),
            None => "".to_string(),
        })
        .collect()
    }

    let customer_df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .select([
            col("receipt_no"),
            col("receipt_sub_no"),
            col("sales_ymd").map(|s| Ok(to_str_series(&s)), GetOutput::default())
        ])
        .select([
            col("receipt_no"),
            col("receipt_sub_no"),
            col("sales_ymd").str().strptime(StrpTimeOptions {
                    fmt: Some("%Y%m%d".to_string()),
                    date_dtype: DataType::Date,
                    ..Default::default()
                })
        ])
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", customer_df);
```

### P-048: レシート明細データ（df_receipt）の売上エポック秒（sales_epoch）は数値型の UNIX 秒でデータを保有している。これを日付型に変換し、レシート番号(receipt_no)、レシートサブ番号（receipt_sub_no）とともに 10 件表示せよ。

```rust
fn to_str_series(date: &Series) -> Series{
        date.i64()
        .unwrap()
        .into_iter()
        .map(|date| match date {
            // Some(date) => date.to_string(),
            Some(date) => Utc.timestamp_opt(date, 0).unwrap().to_string(),
            None => "".to_string(),
        })
        .collect()
    }

    let customer_df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .select([
            col("receipt_no"),
            col("receipt_sub_no"),
            // col("sales_epoch")
            col("sales_epoch").map(|s| Ok(to_str_series(&s)), GetOutput::default())
        ])
        .select([
            col("receipt_no"),
            col("receipt_sub_no"),
            col("sales_epoch").str().strptime(StrpTimeOptions {
                    fmt: Some("%Y-%m-%d".to_string()),
                    date_dtype: DataType::Date,
                    ..Default::default()
                })
        ])
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", customer_df);
```

### P-049: レシート明細データ（df_receipt）の売上エポック秒（sales_epoch）を日付型に変換し、「年」だけ取り出してレシート番号(receipt_no)、レシートサブ番号（receipt_sub_no）とともに 10 件表示せよ。

```rust
fn to_str_series(date: &Series) -> Series {
        date.i64()
            .unwrap()
            .into_iter()
            .map(|date| match date {
                Some(date) => Utc.timestamp_opt(date, 0).unwrap().format("%Y").to_string(),
                None => "".to_string(),
            })
            .collect()
    }

    let customer_df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .select([
            col("receipt_no"),
            col("receipt_sub_no"),
            col("sales_epoch").map(|s| Ok(to_str_series(&s)), GetOutput::default()),
        ])
        .select([col("receipt_no"), col("receipt_sub_no"), col("sales_epoch")])
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", customer_df);
```

### P-050: レシート明細データ（df_receipt）の売上エポック秒（sales_epoch）を日付型に変換し、「月」だけ取り出してレシート番号(receipt_no)、レシートサブ番号（receipt_sub_no）とともに 10 件表示せよ。なお、「月」は 0 埋め 2 桁で取り出すこと。

```rust
fn to_str_series(date: &Series) -> Series {
        date.i64()
            .unwrap()
            .into_iter()
            .map(|date| match date {
                Some(date) => Utc.timestamp_opt(date, 0).unwrap().format("%m").to_string(),
                None => "".to_string(),
            })
            .collect()
    }

    let customer_df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .select([
            col("receipt_no"),
            col("receipt_sub_no"),
            col("sales_epoch").map(|s| Ok(to_str_series(&s)), GetOutput::default()),
        ])
        .select([col("receipt_no"), col("receipt_sub_no"), col("sales_epoch")])
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", customer_df);
```

### P-051: レシート明細データ（df_receipt）の売上エポック秒を日付型に変換し、「日」だけ取り出してレシート番号(receipt_no)、レシートサブ番号（receipt_sub_no）とともに 10 件表示せよ。なお、「日」は 0 埋め 2 桁で取り出すこと。

```rust
fn to_str_series(date: &Series) -> Series {
        date.i64()
            .unwrap()
            .into_iter()
            .map(|date| match date {
                Some(date) => Utc.timestamp_opt(date, 0).unwrap().format("%d").to_string(),
                None => "".to_string(),
            })
            .collect()
    }

    let customer_df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .select([
            col("receipt_no"),
            col("receipt_sub_no"),
            col("sales_epoch").map(|s| Ok(to_str_series(&s)), GetOutput::default()),
        ])
        .select([col("receipt_no"), col("receipt_sub_no"), col("sales_epoch")])
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", customer_df);
```

### P-052: レシート明細データ（df_receipt）の売上金額（amount）を顧客 ID（customer_id）ごとに合計の上、売上金額合計に対して 2,000 円以下を 0、2,000 円より大きい金額を 1 に二値化し、顧客 ID、売上金額合計とともに 10 件表示せよ。ただし、顧客 ID が"Z"から始まるのものは非会員を表すため、除外して計算すること。

```rust
let recept_df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .filter(col("customer_id").str().contains("^[A-Y]"))
        .groupby([col("customer_id")])
        .agg([col("amount").sum().alias("amount_sum")])
        .with_column(
            when(col("amount_sum").gt_eq(2000))
                .then(1)
                .otherwise(0)
                .alias("amount_flg"),
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

    println!("{:?}", recept_df);
```

### P-053: 顧客データ（df_customer）の郵便番号（postal_cd）に対し、東京（先頭 3 桁が 100〜209 のもの）を 1、それ以外のものを 0 に二値化せよ。さらにレシート明細データ（df_receipt）と結合し、全期間において売上実績のある顧客数を、作成した二値ごとにカウントせよ。

```rust
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


    println!("{:?}", joined);

```

### P-054: 顧客データ（df_customer）の住所（address）は、埼玉県、千葉県、東京都、神奈川県のいずれかとなっている。都道府県毎にコード値を作成し、顧客 ID、住所とともに 10 件表示せよ。値は埼玉県を 11、千葉県を 12、東京都を 13、神奈川県を 14 とすること。

```rust
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

    println!("{:?}", customer_df);
```

### P-055: レシート明細（df_receipt）データの売上金額（amount）を顧客 ID（customer_id）ごとに合計し、その合計金額の四分位点を求めよ。その上で、顧客ごとの売上金額合計に対して以下の基準でカテゴリ値を作成し、顧客 ID、売上金額合計とともに 10 件表示せよ。カテゴリ値は順に 1〜4 とする。

> - 最小値以上第 1 四分位未満 ・・・ 1 を付与
> - 第 1 四分位以上第 2 四分位未満 ・・・ 2 を付与
> - 第 2 四分位以上第 3 四分位未満 ・・・ 3 を付与
> - 第 3 四分位以上 ・・・ 4 を付与

```rust
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

    println!("{:?}", recept_df);
```

### P-056: 顧客データ（df_customer）の年齢（age）をもとに 10 歳刻みで年代を算出し、顧客 ID（customer_id）、生年月日（birth_day）とともに 10 件表示せよ。ただし、60 歳以上は全て 60 歳代とすること。年代を表すカテゴリ名は任意とする。

```rust
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
            col("birth_day"),
            col("age"), //本来指定なかったが、検証に便利なので入れている
            col("era")
        ])
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", customer_df);
```

### P-057: 056 の抽出結果と性別コード（gender_cd）により、新たに性別 × 年代の組み合わせを表すカテゴリデータを作成し、10 件表示せよ。組み合わせを表すカテゴリの値は任意とする。

```rust
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
        .with_column(
            col("age")
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
```

### P-058: 顧客データ（df_customer）の性別コード（gender_cd）をダミー変数化し、顧客 ID（customer_id）とともに 10 件表示せよ。

```rust
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
```

### P-059: レシート明細データ（df_receipt）の売上金額（amount）を顧客 ID（customer_id）ごとに合計し、売上金額合計を平均 0、標準偏差 1 に標準化して顧客 ID、売上金額合計とともに 10 件表示せよ。標準化に使用する標準偏差は、分散の平方根、もしくは不偏分散の平方根のどちらでも良いものとする。ただし、顧客 ID が"Z"から始まるのものは非会員を表すため、除外して計算すること。

```rust
//TODO : 売上金額合計を平均0、標準偏差1に標準化が出来ていないので解けていません。
let recept_df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .filter(col("customer_id").str().contains("^[A-Y]"))
        .groupby([col("customer_id")])
        .agg([
            col("amount").sum().alias("amount"),
            col("amount").std(0).alias("std"),
        ])
        .sort(
            "customer_id",
            SortOptions {
                descending: (false),
                nulls_last: (true),
            },
        )
        .collect()
        .unwrap();

    println!("{:?}", recept_df);
```

### P-060: レシート明細データ（df_receipt）の売上金額（amount）を顧客 ID（customer_id）ごとに合計し、売上金額合計を最小値 0、最大値 1 に正規化して顧客 ID、売上金額合計とともに 10 件表示せよ。ただし、顧客 ID が"Z"から始まるのものは非会員を表すため、除外して計算すること。

//TODO: normalization を見つけられず、1 から実装するのも趣旨と異なるので今回は一旦保留

### P-061: レシート明細データ（df_receipt）の売上金額（amount）を顧客 ID（customer_id）ごとに合計し、売上金額合計を常用対数化（底 10）して顧客 ID、売上金額合計とともに 10 件表示せよ。ただし、顧客 ID が"Z"から始まるのものは非会員を表すため、除外して計算すること。

```rust
fn to_log(val: &Series) -> Series {
        val.i64()
            .unwrap()
            .into_iter()
            .map(|val| match val {
                Some(val) => (val as f64).log10(),
                None => 0f64,
            })
            .collect()
    }

    let recept_df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .filter(col("customer_id").str().contains("^[A-Y]"))
        .groupby([col("customer_id")])
        .agg([
            col("amount").sum().alias("amount"),
        ])
        .with_column(
            col("amount")
            .map(|s| Ok(to_log(&s)), GetOutput::default())
                .alias("log"),
        )
        .sort(
            "customer_id",
            SortOptions {
                descending: (false),
                nulls_last: (true),
            },
        )
        .collect()
        .unwrap();

    println!("{:?}", recept_df);
```

### P-062: レシート明細データ（df_receipt）の売上金額（amount）を顧客 ID（customer_id）ごとに合計し、売上金額合計を自然対数化（底 e）して顧客 ID、売上金額合計とともに 10 件表示せよ。ただし、顧客 ID が"Z"から始まるのものは非会員を表すため、除外して計算すること。

```rust
fn to_ln(val: &Series) -> Series {
        val.i64()
            .unwrap()
            .into_iter()
            .map(|val| match val {
                Some(val) => (val as f64).ln(),
                None => 0f64,
            })
            .collect()
    }

    let recept_df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .filter(col("customer_id").str().contains("^[A-Y]"))
        .groupby([col("customer_id")])
        .agg([
            col("amount").sum().alias("amount"),
        ])
        .with_column(
            col("amount")
            .map(|s| Ok(to_ln(&s)), GetOutput::default())
                .alias("log"),
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

    println!("{:?}", recept_df);
```

### P-063: 商品データ（df_product）の単価（unit_price）と原価（unit_cost）から各商品の利益額を算出し、結果を 10 件表示せよ。

```rust
let product_df = LazyCsvReader::new(product_path)
        .has_header(true)
        .finish()
        .unwrap()
        .select([
            col("*"),
            (col("unit_price") - col("unit_cost")).alias("unit_profit")
        ])
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", product_df);
```

### P-064: 商品データ（df_product）の単価（unit_price）と原価（unit_cost）から、各商品の利益率の全体平均を算出せよ。ただし、単価と原価には欠損が生じていることに注意せよ。

```rust
let product_df = LazyCsvReader::new(product_path)
        .has_header(true)
        .finish()
        .unwrap()
        .filter(col("*").is_not_null())
        .select([
            ((col("unit_price") - col("unit_cost")).cast(DataType::Float32) / col("unit_price")).alias("rate")
        ])
        .mean()
        .collect()
        .unwrap();

    println!("{}", product_df.get_columns()[0].get(0));
```

### P-065: 商品データ（df_product）の各商品について、利益率が 30%となる新たな単価を求めよ。ただし、1 円未満は切り捨てること。そして結果を 10 件表示させ、利益率がおよそ 30％付近であることを確認せよ。ただし、単価（unit_price）と原価（unit_cost）には欠損が生じていることに注意せよ。

```rust
let product_df = LazyCsvReader::new(product_path)
        .has_header(true)
        .finish()
        .unwrap()
        .filter(col("*").is_not_null())
        .select([
            col("*"),
            ((col("unit_cost") / lit(0.7))
                .cast(DataType::Float32)
                .floor())
            .alias("new_price"),
        ])
        .select([
            col("*"),
            ((col("new_price") - col("unit_cost")).cast(DataType::Float32) / col("new_price"))
                .alias("new_price_rate"),
        ])
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", product_df);
```

### P-066: 商品データ（df_product）の各商品について、利益率が 30%となる新たな単価を求めよ。今回は、1 円未満を丸めること（四捨五入または偶数への丸めで良い）。そして結果を 10 件表示させ、利益率がおよそ 30％付近であることを確認せよ。ただし、単価（unit_price）と原価（unit_cost）には欠損が生じていることに注意せよ。

```rust
let product_df = LazyCsvReader::new(product_path)
        .has_header(true)
        .finish()
        .unwrap()
        .filter(col("*").is_not_null())
        .select([
            col("*"),
            ((col("unit_cost") / lit(0.7))
                .cast(DataType::Float32)
                .round(0))
            .alias("new_price"),
        ])
        .select([
            col("*"),
            ((col("new_price") - col("unit_cost")).cast(DataType::Float32) / col("new_price"))
                .alias("new_price_rate"),
        ])
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", product_df);
```

### P-067: 商品データ（df_product）の各商品について、利益率が 30%となる新たな単価を求めよ。今回は、1 円未満を切り上げること。そして結果を 10 件表示させ、利益率がおよそ 30％付近であることを確認せよ。ただし、単価（unit_price）と原価（unit_cost）には欠損が生じていることに注意せよ。

```rust
let product_df = LazyCsvReader::new(product_path)
        .has_header(true)
        .finish()
        .unwrap()
        .filter(col("*").is_not_null())
        .select([
            col("*"),
            ((col("unit_cost") / lit(0.7)).cast(DataType::Float32).ceil()).alias("new_price"),
        ])
        .select([
            col("*"),
            ((col("new_price") - col("unit_cost")).cast(DataType::Float32) / col("new_price"))
                .alias("new_price_rate"),
        ])
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", product_df);
```

### P-068: 商品データ（df_product）の各商品について、消費税率 10％の税込み金額を求めよ。1 円未満の端数は切り捨てとし、結果を 10 件表示せよ。ただし、単価（unit_price）には欠損が生じていることに注意せよ。

```rust
let product_df = LazyCsvReader::new(product_path)
        .has_header(true)
        .finish()
        .unwrap()
        .filter(col("*").is_not_null())
        .select([
            col("*"),
            ((col("unit_price") * lit(1.1))
                .cast(DataType::Float32)
                .floor())
            .alias("tax_price"),
        ])
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", product_df);
```

### P-069: レシート明細データ（df_receipt）と商品データ（df_product）を結合し、顧客毎に全商品の売上金額合計と、カテゴリ大区分コード（category_major_cd）が"07"（瓶詰缶詰）の売上金額合計を計算の上、両者の比率を求めよ。抽出対象はカテゴリ大区分コード"07"（瓶詰缶詰）の売上実績がある顧客のみとし、結果を 10 件表示せよ。

```rust
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
```

### P-070: レシート明細データ（df_receipt）の売上日（sales_ymd）に対し、顧客データ（df_customer）の会員申込日（application_date）からの経過日数を計算し、顧客 ID（customer_id）、売上日、会員申込日とともに 10 件表示せよ（sales_ymd は数値、application_date は文字列でデータを保持している点に注意）。

```rust
let recept_df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .select([col("customer_id"), col("sales_ymd")]);

    let customer_df = LazyCsvReader::new(customer_path)
        .has_header(true)
        .finish()
        .unwrap()
        .select([col("customer_id"), col("application_date")]);

    let joined = recept_df
        .inner_join(customer_df, "customer_id", "customer_id")
        .select([
            col("customer_id"),
            (col("sales_ymd").cast(DataType::Utf8))
                .str()
                .strptime(StrpTimeOptions {
                    fmt: Some("%Y%m%d".to_string()),
                    date_dtype: DataType::Date,
                    ..Default::default()
                }),
            (col("application_date").cast(DataType::Utf8))
                .str()
                .strptime(StrpTimeOptions {
                    fmt: Some("%Y%m%d".to_string()),
                    date_dtype: DataType::Date,
                    ..Default::default()
                }),
        ])
        .with_column((col("sales_ymd") - col("application_date")).alias("diff"))
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", joined);
```

### P-071: レシート明細データ（df_receipt）の売上日（sales_ymd）に対し、顧客データ（df_customer）の会員申込日（application_date）からの経過月数を計算し、顧客 ID（customer_id）、売上日、会員申込日とともに 10 件表示せよ（sales_ymd は数値、application_date は文字列でデータを保持している点に注意）。1 ヶ月未満は切り捨てること。

```rust
fn to_month(s: &Series) -> Series {
        s.duration()
            .unwrap()
            .into_iter()
            .map(|date| match date {
                Some(date) => date / 2_629_746_000, // ms to month
                None => 0,
            })
            .collect()
    }

    let recept_df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .select([col("customer_id"), col("sales_ymd")]);

    let customer_df = LazyCsvReader::new(customer_path)
        .has_header(true)
        .finish()
        .unwrap()
        .select([col("customer_id"), col("application_date")]);

    let joined = recept_df
        .inner_join(customer_df, "customer_id", "customer_id")
        .unique(None, UniqueKeepStrategy::First)
        .select([
            col("customer_id"),
            (col("sales_ymd").cast(DataType::Utf8))
                .str()
                .strptime(StrpTimeOptions {
                    fmt: Some("%Y%m%d".to_string()),
                    date_dtype: DataType::Date,
                    ..Default::default()
                }),
            (col("application_date").cast(DataType::Utf8))
                .str()
                .strptime(StrpTimeOptions {
                    fmt: Some("%Y%m%d".to_string()),
                    date_dtype: DataType::Date,
                    ..Default::default()
                }),
        ])
        .with_column(
            ((col("sales_ymd") - col("application_date"))
                .map(|s| Ok(to_month(&s)), GetOutput::default()))
            .alias("diff"),
        )
        .filter(col("customer_id").str().contains("CS006214000001")) //PythonのAnswerと同じ結果を出すために便宜的に追加。題意には含まれない。
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", joined);
```

### P-073: レシート明細データ（df_receipt）の売上日（sales_ymd）に対し、顧客データ（df_customer）の会員申込日（application_date）からのエポック秒による経過時間を計算し、顧客 ID（customer_id）、売上日、会員申込日とともに 10 件表示せよ（なお、sales_ymd は数値、application_date は文字列でデータを保持している点に注意）。なお、時間情報は保有していないため各日付は 0 時 0 分 0 秒を表すものとする。

```rust
fn to_epoch(s: &Series) -> Series {
        s.duration()
            .unwrap()
            .into_iter()
            .map(|date| match date {
                Some(date) => date / 1000,
                None => 0,
            })
            .collect()
    }

    let recept_df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .select([col("customer_id"), col("sales_ymd")]);

    let customer_df = LazyCsvReader::new(customer_path)
        .has_header(true)
        .finish()
        .unwrap()
        .select([col("customer_id"), col("application_date")]);

    let joined = recept_df
        .inner_join(customer_df, "customer_id", "customer_id")
        .unique(None, UniqueKeepStrategy::First)
        .select([
            col("customer_id"),
            (col("sales_ymd").cast(DataType::Utf8))
                .str()
                .strptime(StrpTimeOptions {
                    fmt: Some("%Y%m%d".to_string()),
                    date_dtype: DataType::Date,
                    ..Default::default()
                }),
            (col("application_date").cast(DataType::Utf8))
                .str()
                .strptime(StrpTimeOptions {
                    fmt: Some("%Y%m%d".to_string()),
                    date_dtype: DataType::Date,
                    ..Default::default()
                }),
        ])
        .with_column(
            ((col("sales_ymd") - col("application_date"))
                .map(|s| Ok(to_epoch(&s)), GetOutput::default()))
            .alias("diff"),
        )
        .filter(col("customer_id").str().contains("CS006214000001")) //PythonのAnswerと同じ結果を出すために便宜的に追加。題意には含まれない。
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", joined);
```

### P-074: レシート明細データ（df_receipt）の売上日（sales_ymd）に対し、当該週の月曜日からの経過日数を計算し、売上日、直前の月曜日付とともに 10 件表示せよ（sales_ymd は数値でデータを保持している点に注意）。

```rust
fn to_weekdays(s: &Series) -> Series {
        s.datetime()
            .unwrap()
            .into_iter()
            .map(|date| match date {
                Some(date) => Utc
                    .timestamp_millis_opt(date as i64)
                    .unwrap()
                    .weekday()
                    .num_days_from_monday(),
                None => 0u32,
            })
            .collect()
    }

    fn to_date_string(s: &Series) -> Series {
        s.i64()
            .unwrap()
            .into_iter()
            .map(|date| match date {
                Some(date) => Utc
                    .timestamp_millis_opt(date as i64)
                    .unwrap()
                    .date_naive()
                    .format("%Y-%m-%d")
                    .to_string(),
                None => "".to_string(),
            })
            .collect()
    }

    let recept_df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .select([(col("sales_ymd").cast(DataType::Utf8))
            .str()
            .strptime(StrpTimeOptions {
                fmt: Some("%Y%m%d".to_string()),
                date_dtype: DataType::Date,
                ..Default::default()
            })])
        .with_column(
            col("sales_ymd")
                .cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                .map(|s| Ok(to_weekdays(&s)), GetOutput::default())
                .alias("weekdays"),
        )
        .with_column((col("weekdays") * lit(86400000)).alias("weekdays_millis"))
        .with_column(
            (col("sales_ymd").cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                - col("weekdays_millis"))
            .map(|s| Ok(to_date_string(&s)), GetOutput::default())
            .alias("monday"),
        )
        .select([col("*").exclude(["weekdays_millis"])])
        .collect()
        .unwrap();

    println!("{:?}", recept_df);
```

### P-075: 顧客データ（df_customer）からランダムに 1%のデータを抽出し、先頭から 10 件表示せよ。

```rust
 let customer_df = LazyCsvReader::new(customer_path)
        .has_header(true)
        .finish()
        .unwrap()
        .collect()
        .unwrap();

    let count = customer_df.height(); //count

    let sample = customer_df
        .sample_n((count / 100) as usize, true, true, None)
        .unwrap();

    println!("{:?}", sample);
```

### P-076: 顧客データ（df_customer）から性別コード（gender_cd）の割合に基づきランダムに 10%のデータを層化抽出し、性別コードごとに件数を集計せよ。

```rust
let customer_df = LazyCsvReader::new(customer_path)
        .has_header(true)
        .finish()
        .unwrap();

    let male_df = customer_df.clone().filter(col("gender_cd").eq(0));
    let female_df = customer_df.clone().filter(col("gender_cd").eq(1));
    let unknown_df = customer_df.clone().filter(col("gender_cd").eq(9));

    let male_count = male_df.clone().collect().unwrap().height() / 10;
    let female_count = female_df.clone().collect().unwrap().height() / 10;
    let unknown_count = unknown_df.clone().collect().unwrap().height() / 10;

    let male_sample = male_df
        .collect()
        .unwrap()
        .sample_n(male_count, true, true, None)
        .unwrap()
        .lazy();

    let female_sample = female_df
        .collect()
        .unwrap()
        .sample_n(female_count, true, true, None)
        .unwrap()
        .lazy();

    let unknown_sample = unknown_df
        .collect()
        .unwrap()
        .sample_n(unknown_count, true, true, None)
        .unwrap()
        .lazy();

    let sample = concat([male_sample, female_sample, unknown_sample], true, true)
        .unwrap()
        .groupby([col("gender_cd")])
        .agg([col("gender_cd").count().alias("count")])
        .collect()
        .unwrap();
    //
    println!("{:?}", sample);
```

### P-077: レシート明細データ（df_receipt）の売上金額を顧客単位に合計し、合計した売上金額の外れ値を抽出せよ。なお、外れ値は売上金額合計を対数化したうえで平均と標準偏差を計算し、その平均から 3σ を超えて離れたものとする（自然対数と常用対数のどちらでも可）。結果は 10 件表示せよ。

```rust
fn to_log(val: &Series) -> Series {
        val.i64()
            .unwrap()
            .into_iter()
            .map(|val| match val {
                Some(val) => (val as f64).log10(),
                None => 0f64,
            })
            .collect()
    }

    let recept_df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .groupby([col("customer_id")])
        .agg([col("amount")
            .sum()
            .map(|s| Ok(to_log(&s)), GetOutput::default())
            .alias("amount_log")])
        .with_columns([
            col("amount_log").mean().alias("mean"),
            col("amount_log").std(0).alias("std"),
        ])
        .with_columns([
            (col("mean") - (col("std") * lit(3))).alias("minus_3_sigma"),
            (col("mean") + (col("std") * lit(3))).alias("up_3_sigma"),
        ])
        .with_columns([
            col("amount_log")
                .lt(col("minus_3_sigma"))
                .alias("minus_flag"),
            col("amount_log").gt(col("up_3_sigma")).alias("up_flag"),
        ])
        .filter(
            col("minus_flag")
                .eq(lit(true))
                .or(col("up_flag").eq(lit(true))),
        )
        .collect()
        .unwrap()
        .head(Some(10));

    //
    println!("{:?}", recept_df);

```

### P-078: レシート明細データ（df_receipt）の売上金額（amount）を顧客単位に合計し、合計した売上金額の外れ値を抽出せよ。ただし、顧客 ID が"Z"から始まるのものは非会員を表すため、除外して計算すること。なお、ここでは外れ値を第 1 四分位と第 3 四分位の差である IQR を用いて、「第 1 四分位数-1.5×IQR」を下回るもの、または「第 3 四分位数+1.5×IQR」を超えるものとする。結果は 10 件表示せよ。

```rust
 let recept_df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .filter(col("customer_id").str().contains("^[A-Y]"))
        .groupby([col("customer_id")])
        .agg([col("amount").sum().alias("amount")])
        .with_columns([
            col("amount")
                .quantile(0.25f64, QuantileInterpolOptions::Nearest)
                .alias("q1"),
            col("amount")
                .quantile(0.75f64, QuantileInterpolOptions::Nearest)
                .alias("q3"),
        ])
        .with_column((col("q3") - col("q1")).alias("iqr"))
        .with_columns([
            (col("q1") - (col("iqr") * lit(1.5))).alias("lower"),
            (col("q3") + (col("iqr") * lit(1.5))).alias("upper"),
        ])
        .with_columns([
            col("amount").lt(col("lower")).alias("lower_flag"),
            col("amount").gt(col("upper")).alias("upeer_flag"),
        ])
        .filter(
            col("lower_flag")
                .eq(lit(true))
                .or(col("upeer_flag").eq(lit(true))),
        )
        .sort(
            "customer_id",
            SortOptions {
                descending: (false), //高齢順にソート とは、誕生日を昇順にソートすること
                nulls_last: (true),
            },
        )
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", recept_df);
```

### P-079: 商品データ（df_product）の各項目に対し、欠損数を確認せよ。

```rust
let product_df = LazyCsvReader::new(product_path)
        .has_header(true)
        .finish()
        .unwrap()
        .with_columns([col("*").is_null().alias("flag")])
        .filter(col("flag").eq(lit(true)))
        .sum()
        .collect()
        .unwrap();
```

### P-080: 商品データ（df_product）のいずれかの項目に欠損が発生しているレコードを全て削除した新たな商品データを作成せよ。なお、削除前後の件数を表示させ、079 で確認した件数だけ減少していることも確認すること。

```rust
let product_df = LazyCsvReader::new(product_path)
        .has_header(true)
        .finish()
        .unwrap()
        .with_columns([col("*").is_null().alias("flag")])
        .collect()
        .unwrap();

    let pre_count = &product_df.shape().0;

    let product_df = product_df.drop_nulls(None).unwrap();
    let pro_count = &product_df.shape().0;

    println!("削除前 : {}", pre_count);
    println!("削除後 : {}", pro_count);
```

### P-081: 単価（unit_price）と原価（unit_cost）の欠損値について、それぞれの平均値で補完した新たな商品データを作成せよ。なお、平均値については 1 円未満を丸めること（四捨五入または偶数への丸めで良い）。補完実施後、各項目について欠損が生じていないことも確認すること。

```rust
let product_df = LazyCsvReader::new(product_path)
        .has_header(true)
        .finish()
        .unwrap()
        .with_columns([col("*").is_null().alias("flag")])
        .collect()
        .unwrap()
        .fill_null(FillNullStrategy::Mean)
        .unwrap()
        .lazy()
        .filter(col("flag").eq(lit(true)))
        .collect()
        .unwrap();

    println!("{:?}", product_df);
```

### P-082: 単価（unit_price）と原価（unit_cost）の欠損値について、それぞれの中央値で補完した新たな商品データを作成せよ。なお、中央値については 1 円未満を丸めること（四捨五入または偶数への丸めで良い）。補完実施後、各項目について欠損が生じていないことも確認すること。

```rust
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
```

### P-084: 顧客データ（df_customer）の全顧客に対して全期間の売上金額に占める 2019 年売上金額の割合を計算し、新たなデータを作成せよ。ただし、売上実績がない場合は 0 として扱うこと。そして計算した割合が 0 超のものを抽出し、結果を 10 件表示せよ。また、作成したデータに欠損が存在しないことを確認せよ。

```rust
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
```

### P-085: 顧客データ（df_customer）の全顧客に対し、郵便番号（postal_cd）を用いてジオコードデータ（df_geocode）を紐付け、新たな顧客データを作成せよ。ただし、1 つの郵便番号（postal_cd）に複数の経度（longitude）、緯度（latitude）情報が紐づく場合は、経度（longitude）、緯度（latitude）の平均値を算出して使用すること。また、作成結果を確認するために結果を 10 件表示せよ。

```rust
let customer_df = LazyCsvReader::new(customer_path)
        .has_header(true)
        .finish()
        .unwrap();

    let geocode_df = LazyCsvReader::new(geocode_path)
        .has_header(true)
        .finish()
        .unwrap()
        .groupby([col("postal_cd")])
        .agg([
            col("longitude").mean().alias("m_lng"),
            col("latitude").mean().alias("m_lat")
            ]);

    let joined = customer_df.inner_join(geocode_df, col("postal_cd"), col("postal_cd"))
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", joined);
```

### P-086: 085 で作成した緯度経度つき顧客データに対し、会員申込店舗コード（application_store_cd）をキーに店舗データ（df_store）と結合せよ。そして申込み店舗の緯度（latitude）・経度情報（longitude)と顧客住所（address）の緯度・経度を用いて申込み店舗と顧客住所の距離（単位：km）を求め、顧客 ID（customer_id）、顧客住所（address）、店舗住所（address）とともに表示せよ。計算式は以下の簡易式で良いものとするが、その他精度の高い方式を利用したライブラリを利用してもかまわない。結果は 10 件表示せよ。

```rust
fn calc_distance(list: Vec<f64>) -> f64 {
        let distance = 6371f64
            * f64::acos(
                (f64::sin(list[0].to_radians()) * f64::sin(list[2].to_radians()))
                    + (f64::cos(list[0].to_radians())
                        * f64::cos(list[2].to_radians())
                        * f64::cos(list[1].to_radians() - list[3].to_radians())),
            );
        return distance;
    }

    let customer_df = LazyCsvReader::new(customer_path)
        .has_header(true)
        .finish()
        .unwrap();

    let geocode_df = LazyCsvReader::new(geocode_path)
        .has_header(true)
        .finish()
        .unwrap()
        .groupby([col("postal_cd")])
        .agg([
            col("longitude").mean().alias("m_lng"),
            col("latitude").mean().alias("m_lat"),
        ]);

    let store_df = LazyCsvReader::new(store_path)
        .has_header(true)
        .finish()
        .unwrap()
        .select([all(), col("address").alias("store_address")]);

    let joined = customer_df
        .inner_join(geocode_df, col("postal_cd"), col("postal_cd"))
        .inner_join(store_df, col("application_store_cd"), col("store_cd"))
        .select([
            col("customer_id"),
            col("address").alias("customer_address"),
            col("store_address"),
            as_struct(&[
                col("latitude"),
                col("longitude"),
                col("m_lat"),
                col("m_lng"),
            ])
            .apply(
                |s| {
                    let ca = s.struct_().unwrap();

                    let s_lat = &ca.fields()[0];
                    let s_lng = &ca.fields()[1];
                    let s_m_lat = &ca.fields()[2];
                    let s_m_lng = &ca.fields()[3];

                    let ca_lat = s_lat.f64().unwrap();
                    let ca_lng = s_lng.f64().unwrap();
                    let ca_m_lat = s_m_lat.f64().unwrap();
                    let ca_m_lng = s_m_lng.f64().unwrap();

                    let out: Float64Chunked = ca_lat
                        .into_iter()
                        .zip(ca_lng)
                        .zip(ca_m_lat)
                        .zip(ca_m_lng)
                        .map(|(((opt_a, opt_b), opt_c), opt_d)| {
                            match (((opt_a, opt_b), opt_c), opt_d) {
                                (((Some(a), Some(b)), Some(c)), Some(d)) => {
                                    Some(calc_distance(vec![a, b, c, d]))
                                }
                                _ => None,
                            }
                        })
                        .collect();

                    Ok(out.into_series())
                },
                GetOutput::default(),
            )
            .alias("distance"),
        ])
        .collect()
        .unwrap()
        .head(Some(10));

    println!("{:?}", joined);
```

### P-087: 顧客データ（df_customer）では、異なる店舗での申込みなどにより同一顧客が複数登録されている。名前（customer_name）と郵便番号（postal_cd）が同じ顧客は同一顧客とみなして 1 顧客 1 レコードとなるように名寄せした名寄顧客データを作成し、顧客データの件数、名寄顧客データの件数、重複数を算出せよ。ただし、同一顧客に対しては売上金額合計が最も高いものを残し、売上金額合計が同一もしくは売上実績がない顧客については顧客 ID（customer_id）の番号が小さいものを残すこととする。

```rust
let customer_df = LazyCsvReader::new(customer_path)
        .has_header(true)
        .finish()
        .unwrap();

    let recept_df = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap()
        .groupby([col("customer_id")])
        .agg([col("amount").sum()]);

    let joined = customer_df
        .clone()
        .left_join(recept_df, col("customer_id"), col("customer_id"))
        .fill_null(lit(0))
        .sort(
            "customer_id",
            SortOptions {
                descending: (false),
                nulls_last: (true),
            },
        )
        .sort(
            "amount",
            SortOptions {
                descending: (true),
                nulls_last: (true),
            },
        )
        .unique(
            Some(vec!["customer_name".to_string(), "postal_cd".to_string()]),
            UniqueKeepStrategy::First,
        )
        .collect()
        .unwrap();

    println!("{:?}", joined);

    // original の顧客データをShapeで確認
    let count = customer_df.collect().unwrap().shape();
    println!("origin size : {:?}", count);

    let unique_count = joined.shape();
    println!("unique size : {:?}", count);

    let diff = count.0 - unique_count.0;
    println!("diff : {:?}", diff);
```

### P-089: 売上実績がある顧客を、予測モデル構築のため学習用データとテスト用データに分割したい。それぞれ 8:2 の割合でランダムにデータを分割せよ。

```rust
    let customer_df = LazyCsvReader::new(customer_path)
        .has_header(true)
        .finish()
        .unwrap();

    let recept_df = LazyCsvReader::new(recept_path)
    .has_header(true)
    .finish()
    .unwrap()
    .groupby([col("customer_id")])
    .agg([col("amount").sum().alias("amount")])
    .filter(col("amount").gt(0));


    let joined = customer_df.inner_join(recept_df, col("customer_id"), col("customer_id"))
        .collect()
        .unwrap()
        .sample_frac(1.0, false, true, None)
        .unwrap();

    let shape = joined.shape();
    println!("origin: {:?}", shape);

    let train = joined.slice(0, ((shape.0 as f32) * 0.8).floor() as usize);
    let test = joined.slice(((shape.0 as f32) * 0.8).floor() as i64, ((shape.0 as f32) * 0.2).round() as usize);

    println!("{:?}", train.shape());
    println!("{:?}", test.shape());

    println!("{:?}", train.tail(Some(10)));
    println!("{:?}", test.head(Some(10)));
```

### P-090: レシート明細データ（df_receipt）は 2017 年 1 月 1 日〜2019 年 10 月 31 日までのデータを有している。売上金額（amount）を月次で集計し、学習用に 12 ヶ月、テスト用に 6 ヶ月の時系列モデル構築用データを 3 セット作成せよ。

```rust
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
```

### P-091: 顧客データ（df_customer）の各顧客に対し、売上実績がある顧客数と売上実績がない顧客数が 1:1 となるようにアンダーサンプリングで抽出せよ。

```rust
let customer_df = LazyCsvReader::new(customer_path)
    .has_header(true)
    .finish()
    .unwrap();

let recept_df = LazyCsvReader::new(recept_path)
    .has_header(true)
    .finish()
    .unwrap()
    .groupby([col("customer_id")])
    .agg([col("amount").sum().alias("amount")]);

let joined = customer_df.left_join(recept_df, col("customer_id"), col("customer_id"))
    .select([
        col("*").exclude(["amount"]),
        col("amount").fill_null(lit(0)).alias("amount")
    ]);

let non_zero_df = joined.clone().filter(col("amount").gt(0))
    .collect().unwrap();

let zero_df = joined.filter(col("amount").eq(0))
    .collect().unwrap();

println!("non-zero :{:?}", non_zero_df.shape()); //zero_dfよりすくないので、こっちに合わせる
println!("zero : {:?}", zero_df.shape());

let zero_df = zero_df
    .sample_n(non_zero_df.shape().0, false, true, None).unwrap();

println!("picked zero : {:?}", zero_df.shape());
```

### P-092: 顧客データ（df_customer）の性別について、第三正規形へと正規化せよ。

```rust
let customer_df = LazyCsvReader::new(customer_path)
        .has_header(true)
        .finish()
        .unwrap()
        .select([col("gender_cd"), col("gender")])
        .unique(
            Some(vec!["gender_cd".to_string(), "gender".to_string()]),
            UniqueKeepStrategy::First,
        )
        .collect()
        .unwrap();

    println!("{:?}", customer_df);
```

### P-093: 商品データ（df_product）では各カテゴリのコード値だけを保有し、カテゴリ名は保有していない。カテゴリデータ（df_category）と組み合わせて非正規化し、カテゴリ名を保有した新たな商品データを作成せよ。

```rust
    let product_df = LazyCsvReader::new(product_path)
        .has_header(true)
        .finish()
        .unwrap();

    let category_df = LazyCsvReader::new(category_path)
        .has_header(true)
        .finish()
        .unwrap()
        .select([
            col("category_small_cd"),
            col("category_major_name"),
            col("category_medium_name"),
            col("category_small_name"),
        ]);

    let joined = product_df
        .inner_join(
            category_df,
            col("category_small_cd"),
            col("category_small_cd"),
        )
        .collect()
        .unwrap();

    println!("{:?}", joined);
```

### P-094: 093 で作成したカテゴリ名付き商品データを以下の仕様でファイル出力せよ。

> CSV, header あり, UTF-8, ./data/output.csv

```rust
let path = std::path::Path::new("./data/output.csv");
    let mut file = File::create(path).expect("could not create file");

    CsvWriter::new(&mut file)
        .has_header(true)
        .finish(&mut joined)
        .unwrap();
```

### P-096: 093 で作成したカテゴリ名付き商品データを以下の仕様でファイル出力せよ。

> CSV, header なし, UTF-8, ./data/output.csv

```rust
let path = std::path::Path::new("./data/output.csv");
    let mut file = File::create(path).expect("could not create file");

    CsvWriter::new(&mut file)
        .has_header(false)
        .finish(&mut joined)
        .unwrap();
```

### P-099: 093 で作成したカテゴリ名付き商品データを以下の仕様でファイル出力せよ。

> TSV, header あり, UTF-8, ./data/output.csv

```rust
let path = std::path::Path::new("./data/output.tsv");
    let mut file = File::create(path).expect("could not create file");

    CsvWriter::new(&mut file)
        .has_header(true)
        .with_delimiter(b'\t')
        .finish(&mut joined)
        .unwrap();
```
