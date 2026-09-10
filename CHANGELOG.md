# Changelog

이 플러그인의 주요 변경을 기록한다.
포맷은 [Keep a Changelog](https://keepachangelog.com/), 버전은 SemVer 를 따른다.

## [Unreleased]

## [2.2.4] - 2026-09-10

### Changed
- `synapse-sdk` 의존성 `2026.2.5` → `2026.2.6` 업그레이드. plugin 자체 동작 / 입출력 무변경.
  - SDK 2026.2.6: upload 액션이 스토리지 자격증명을 **세션 경계 안에서** 받는다 (SYN-7616). 종전에는 `with_configuration` 의 평문 설정을 써서 `credential_regime` 이 `none` 이 아닌 스토리지(SMB 공용/개인 인증 등)로는 업로드가 시작조차 되지 않았다 — 이제 그런 스토리지의 경로를 원본으로 쓸 수 있다. 원본 스토리지는 읽기 스코프(`purpose=read`)로 연다.
  - 부수: 원본·asset·Excel 메타데이터 경로를 열 때의 평문 설정 재조립이 사라졌고, `InitializeStep` 의 step 결과에서도 평문이 빠졌다.

## [2.2.3] - 2026-09-09

### Changed
- `synapse-sdk` 의존성 `2026.2.4` → `2026.2.5` 업그레이드. plugin 자체 동작 / 입출력 무변경.
  - SDK 2026.2.5: v1 `Storage.provider` 를 열린 계약(`str`)으로 바꿔 backend 가 새로 추가한 provider 코드(`smb` 등)를 응답 모델이 거부하지 않는다 (SYN-7613). 종전에는 `initialize` 단계에서 `1 validation error for Storage / provider` 로 즉시 실패했다. 더불어 `Storage.configuration` 미선언 키에 경고를 달아 유실 관측성을 확보했다 (SYN-7581).
  - upload 액션의 스토리지 획득 경로는 무변경이다 — `credential_regime` 이 `none` 이 아닌 스토리지(예: SMB 공용 인증)는 자격증명을 받지 못해 여전히 사용할 수 없다 (SYN-7614 후속 대상).

## [2.2.2] - 2026-09-09

### Changed
- `synapse-sdk` 의존성 `2026.1.174` → `2026.2.4` 업그레이드. plugin 자체 동작 / 입출력 무변경.
  - SDK 2026.2.4: Synology API error 119 재로그인에 1회 상한을 두고, 새 SID도 거부되면 `SynologySessionRefreshError`로 명시적으로 실패한다.

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
