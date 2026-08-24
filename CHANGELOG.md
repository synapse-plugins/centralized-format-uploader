# Changelog

이 플러그인의 주요 변경을 기록한다.
포맷은 [Keep a Changelog](https://keepachangelog.com/), 버전은 SemVer 를 따른다.

## [Unreleased]

### Added
### Changed
### Fixed
### Removed

## [2.2.1] - 2026-08-24

### Changed
- SDK 핀 고정 — `requirements.txt` 의 `synapse-sdk[all]` → **`synapse-sdk[all]==2026.1.174`**.
  종전에는 버전이 없어 **agent env 를 빌드하는 시점의 최신 SDK** 가 깔렸다. 같은 플러그인
  릴리즈가 언제 배포됐는지에 따라 다른 SDK 로 돌았고, 그래서 재현이 안 됐다. 무엇이 깔릴지가
  이제 릴리즈에 적혀 있다.
  **플러그인 코드는 바뀌지 않았다.** 실제로 도는 SDK 가 달라지는 것도 아닐 수 있다 — 핀이
  없던 동안에도 최신이 깔렸다면 2026.1.174 였을 것이다. 이 릴리즈가 바꾸는 것은 **동작이
  아니라 재현성**이다.

### Fixed
- `config.yaml` 의 SDK 기본/샘플 action entrypoint 가 **해소되지 않는 경로**였다 —
  `.venv.lib.python3.12.site-packages.synapse_sdk.…`. 로컬 `.venv` 경로가 config 에 그대로
  커밋돼 있었다. 정상 모듈 경로(`synapse_sdk.…`)로 고친다.
- `add_task_data` action 이 **SDK 에서 사라진 모듈**(`synapse_sdk.plugins.actions.add_task_data`)
  을 가리켰다. 현행 이름 **`to_task`**(`synapse_sdk.plugins.actions.to_task.action.ToTaskAction`)
  로 바꾼다.
  본 플러그인 본체인 `upload`(`plugin.upload.UploadAction` + COCO 분할·보강 스텝 3종)
  는 영향받지 않았다 — 깨져 있던 것은 함께 선언된 SDK 기본/샘플 action 들이다.

## [2.2.0] - 2026-05-12

- `data_meta_1` 이 필수이거나 `image_1` 과 경로를 공유할 때의 중앙집중 COCO 업로드 수정.

## [2.1.0] - 2026-04-22

- 이 플러그인의 CHANGELOG 는 여기서 시작한다. 그 이전 이력(2026-04-21 최초 커밋, LIG·KAL
  납품 태그)은 git log 와 태그를 본다.
