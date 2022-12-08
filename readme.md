# rust でデータ分析 100 本ノックをやる

## 準備

python でデータ分析を行う場合、pandas を使うのが一般的です。  
rust の場合、データ分析を行う場合のライブラリはいくつかありますが、ドキュメントが一番充実している polars を選択しました。

今回は、polars の仲でもさらに`Lazy`API を使って問題を解いていきます。  
`Lazy`を使うと、`LazyFrame.collect()`もしくは`LazyFrame.fetch()`を呼び出すまで実際の計算を実行しないようになります。これにより、Polars がクエリの最適化を行い、最速のアルゴリズムが選択されるようになるとのこと。実行用ファイルのサイズも上昇するので諸刃の剣ではありますが、今回は Polars のパフォーマンスも確認していきたいので、`Lazy`API を使って問題を解いていきます。

また、いくつかのクエリを実行するために読み込まなければいけないオプションがあるので、`Cargo.toml`に以下のように追記します。

```toml
[dependencies]
polars = {version = "0.25.1", features=["describe", "lazy", "strings"]}
```

csv の読み込みは以下の通り。

```
let recept_path = "csvへのパス";

let df: LazyFrame = LazyCsvReader::new(recept_path)
        .has_header(true)
        .finish()
        .unwrap();

println!("{:?}", df.collect().unwrap());
```

### P-001: レシート明細データ（df_receipt）から全項目の先頭 10 件を表示し、どのようなデータを保有しているか目視で確認せよ。

```
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

```
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

```
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

```
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
