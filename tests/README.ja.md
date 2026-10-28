# テスト

TinyWG と同じく、[pytest-embedded](https://docs.espressif.com/projects/pytest-embedded/en/latest/) + Arduino CLI([pytest-embedded-arduino-cli](https://github.com/tanakamasayuki/pytest-embedded-arduino-cli))で動かします。既定のプロファイルは `lang-ship:host`(PC 上で動くスケッチ)です。

```sh
cd tests
uv run pytest smoke
```

## 各テスト

| ディレクトリ | 内容 | プロファイル |
|---|---|---|
| `smoke/` | ビルドとハーネスの確認 | host / esp32 / esp32s3 / ch32v307 / picow |

## 実機で動かす

`.env.example` を `.env` にコピーし、シリアルポートと Wi-Fi を書きます。

```sh
uv run --env-file .env pytest smoke --profile esp32 -s
```
