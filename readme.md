# rust でデータ分析 100 本ノックをやる

## 準備

python でデータ分析を行う場合、pandas を使うのが一般的です。  
rust の場合、データ分析を行う場合のライブラリはいくつかありますが、ドキュメントが一番充実している polars を選択しました。

今回は、polars の仲でもさらに`Lazy`API を使って問題を解いていきます。  
`Lazy`を使うと、`LazyFrame.collect()`もしくは`LazyFrame.fetch()`を呼び出すまで実際の計算を実行しないようになります。これにより、Polars がクエリの最適化を行い、最速のアルゴリズムが選択されるようになるとのこと。実行用ファイルのサイズも上昇するので諸刃の剣ではありますが、今回は Polars のパフォーマンスも確認していきたいので、`Lazy`API を使って問題を解いていきます。

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
