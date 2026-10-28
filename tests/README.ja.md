# テスト

TinyWG と同じく、[pytest-embedded](https://docs.espressif.com/projects/pytest-embedded/en/latest/) + Arduino CLI([pytest-embedded-arduino-cli](https://github.com/tanakamasayuki/pytest-embedded-arduino-cli))で動かします。既定のプロファイルは `lang-ship:host`(PC 上で動くスケッチ)です。

```sh
cd tests
uv run pytest smoke codec topic pool
```

## 各テスト

| ディレクトリ | 内容 | プロファイル |
|---|---|---|
| `smoke/` | ビルドとハーネスの確認 | host / esp32 / esp32s3 / ch32v307 / picow |
| `codec/` | 可変長整数、UTF-8 の検証、固定ヘッダ、全パケット種別のエンコード / デコード(3.1.1 と 5.0)を仕様書から組み立てたバイト列と照合。不正なパケットの拒否、バッファ不足 | 同上 |
| `topic/` | トピック名とフィルタの検証、照合(仕様書 §4.7 の例)、`$` で始まるトピック、共有購読 `$share/` の分解 | 同上 |
| `pool/` | メッセージプールの確保・解放・結合・枯渇・アライメント、2 万回のランダムな確保と解放、参照カウント付きメッセージ | 同上 |

## 実機で動かす

`.env.example` を `.env` にコピーし、シリアルポートと Wi-Fi を書きます。

```sh
uv run --env-file .env pytest smoke --profile esp32 -s
```
