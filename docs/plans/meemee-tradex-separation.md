# MeeMee / TRADEX Separation Plan

## Phase 1

- MeeMee v1 の境界を固定する。
- docs と contract test で confirmed / provisional / research-only を締める。
- ranking / detail / positions の回帰を先に止める。

## Phase 2

- TRADEX v1 の publish artifact / compare artifact / validation summary を固定する。
- MeeMee が読むのは publish 済み結果だけにする。
- 研究途中の artifact は TRADEX 内部に閉じる。

## Phase 3

- 画像 rerank は renderer / labels / leakage-safe split / eval / rerank の最小構成から進める。
- YOLO のような重い追加は後ろに回す。

