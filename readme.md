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
### P-056: 顧客データ（df_customer）の年齢（age）をもとに10歳刻みで年代を算出し、顧客ID（customer_id）、生年月日（birth_day）とともに10件表示せよ。ただし、60歳以上は全て60歳代とすること。年代を表すカテゴリ名は任意とする。
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