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
