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

P19,P20 : Rank を算出する方法が分からない

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
//TODO filter で複数条件を指定する方法がわからないので、
// 各条件でフィルターかけてから結合する方法をとった。
// もっといい方法があれば教えてください。。。
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
    ]);

let df1 = df
    .clone()
    .filter(col("customer_id").str().contains("CS018205000001"))
    .filter(col("amount").gt(1000));

let df2 = df
    .clone()
    .filter(col("customer_id").str().contains("CS018205000001"))
    .filter(col("quantity").gt_eq(5));

let concat_df = concat([df1, df2], true, true).unwrap();

println!("{:?}", concat_df.collect().unwrap());
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

### P-020: レシート明細データ（df_receipt）に対し、1 件あたりの売上金額（amount）が高い順にランクを付与し、先頭から 10 件表示せよ。項目は顧客 ID（customer_id）、売上金額（amount）、付与したランクを表示させること。なお、売上金額（amount）が等しい場合でも別順位を付与すること。

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
