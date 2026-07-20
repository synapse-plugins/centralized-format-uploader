# centralized-format-uploader

하나의 중앙 집중형 **COCO** 어노테이션 JSON을 이미지별 JSON으로 분할하여, 각 이미지를 개별 데이터 유닛으로 업로드하는 업로드 플러그인.

---

## 1. 플러그인 식별 정보

| 항목 | 값 |
| --- | --- |
| 폴더명 / GitHub 저장소 | `centralized-format-uploader` |
| 코드명 (`config.yaml` → `code`) | `centralized-format-uploader` |
| 플러그인 이름 (`config.yaml` → `name`) | `centralized-format-uploader-v2` |
| 패키지명 (`pyproject.toml` → `name`) | `centralized-format-uploader` |
| 버전 | `2.2.0` |
| 카테고리 | `upload` |
| 지원 데이터 타입 | `image` |
| upload 진입점 | `plugin.upload.UploadAction` |

---

## 2. 개요

COCO 데이터셋은 보통 하나의 큰 JSON에 모든 이미지의 어노테이션이 모여 있습니다(**중앙 집중형**). 이 플러그인은 그 단일 COCO JSON을 **이미지 파일명(stem) 기준으로 분할**하여, 이미지 1장 = 데이터 유닛 1개로 업로드합니다.

핵심 설계 목표는 **어노테이션 스펙(`data_meta_1`)이 데이터 컬렉션에서 선택/필수 어느 쪽이든, 그리고 이미지와 JSON이 같은 경로를 쓰든 다른 경로를 쓰든 동작**하도록 하는 것입니다.

### 분할 개념도

```mermaid
flowchart LR
    COCO["중앙 COCO JSON<br/>images[] + annotations[]"] -->|이미지별 분할| A["cat.json<br/>(cat.jpg의 annotation)"]
    COCO --> B["dog.json"]
    COCO --> C["... 이미지별"]
    A -.stem 매칭.-> IA["cat.jpg → 데이터 유닛"]
    B -.stem 매칭.-> IB["dog.jpg → 데이터 유닛"]
```

---

## 3. 파라미터 (UI 스키마)

| 이름 | 형태 | 설명 | 기본값 |
| --- | --- | --- | --- |
| `group_name` | text | 데이터 유닛에 부여할 묶음 이름 | (없음) |

### 입력 요구사항

- `data_meta_1` 스펙 디렉터리에 유효한 COCO JSON(최소 `images`, `annotations` 키) 1개
- 이미지 파일명이 COCO의 `images[].file_name`과 일치해야 stem 매칭됨

---

## 4. 전체 업로드 워크플로우

기본 8단계 위에 **3개의 커스텀 단계**를 삽입합니다.

```mermaid
flowchart TD
    A["1 initialize"] --> P["🔧 pre_split_coco_annotations<br/><b>커스텀</b> · weight 0.05"]
    P --> B["2 process_metadata"]
    B --> C["3 analyze_collection"]
    C --> D["4 organize_files<br/><i>이미지 ↔ 분할 JSON stem 매칭</i>"]
    D --> E2["🔧 enrich_coco_metadata<br/><b>커스텀</b> · weight 0.05"]
    E2 --> E["5 validate_files"]
    E --> F["6 upload_files"]
    F --> G["7 generate_data_units"]
    G --> Fin["🔧 finalize_coco_split<br/><b>커스텀</b> · weight 0.01"]
    Fin --> H["8 cleanup"]

    style P fill:#e7f5ff,stroke:#1971c2,stroke-width:2px
    style E2 fill:#e7f5ff,stroke:#1971c2,stroke-width:2px
    style Fin fill:#e7f5ff,stroke:#1971c2,stroke-width:2px
```

> 삽입 위치: `insert_before('organize_files', PreSplit…)`, `insert_after('organize_files', Enrich…)`, `insert_after('generate_data_units', Finalize…)`

---

## 5. 커스텀 단계 상세

### 5.1 `PreSplitCocoAnnotationsStep` — 분할 (organize_files 이전)

`organize_files`보다 **먼저** 실행되어야 이미지와 분할 JSON이 stem 기준으로 자연스럽게 짝지어집니다. `data_collection`이 없으면 스킵.

```mermaid
flowchart TD
    S(["execute()"]) --> DIR{"data_meta_1<br/>디렉터리 존재?"}
    DIR -- 아니오 --> SK["split=False 반환"]
    DIR -- 예 --> JSON{"*.json 존재?"}
    JSON -- 아니오 --> SK
    JSON -- 예 --> PARSE["첫 JSON 파싱"]
    PARSE --> VALID{"유효한 COCO?<br/>(images·annotations)"}
    VALID -- 아니오 --> FAIL["StepResult(success=False)"]
    VALID -- 예 --> TGT["_resolve_target_dir()<br/>쓸 위치 결정"]

    TGT --> SAME{"다중경로 &&<br/>image_1 경로 == data_meta_1 경로?"}
    SAME -- 예 --> RED["_synapse_split 하위폴더 생성<br/>data_meta_1 asset path 재지정<br/>(same-path 충돌 회피)"]
    SAME -- 아니오 --> KEEP["data_meta_1 디렉터리 그대로 사용"]

    RED --> SPLIT["이미지별 JSON 생성<br/>{stem}.json = 공유필드+해당 image+해당 annotations"]
    KEEP --> SPLIT
    SPLIT --> STORE["context.params에 저장<br/>생성파일 목록·이미지별 메타·재지정 상태"]
    STORE --> DONE(["success · rollback_data=생성파일/재지정"])

    style FAIL fill:#ffe3e3,stroke:#e03131
    style RED fill:#fff3bf,stroke:#f08c00
    style DONE fill:#d3f9d8,stroke:#2f9e44
```

- 원본 COCO 파일은 **절대 덮어쓰지 않음**(출력 경로가 원본과 같으면 skip).
- 공유 필드로 `categories`, (있으면) `info`, `licenses`를 각 분할 JSON에 복사.
- **롤백**: 생성한 분할 JSON 삭제 + asset path 원복 + `_synapse_split` 폴더 제거.

> **same-path 충돌이란?** 다중 경로 모드에서 `image_1`과 `data_meta_1`이 동일 디렉터리를 가리키면, SDK의 `FlatFileDiscoveryStrategy`가 두 스펙을 구분하지 못해 모든 파일이 한 스펙으로 몰립니다. 이를 피하려고 분할 JSON을 `_synapse_split` 하위 폴더로 내려 디렉터리 깊이로 구분되게 합니다.

### 5.2 `EnrichCocoMetadataStep` — 메타 부착 (organize_files 직후)

이미 이미지-JSON 짝이 맞춰진 상태에서 메타데이터를 붙이고, 원본 중앙 COCO 그룹을 제거합니다.

```mermaid
flowchart TD
    S(["execute()"]) --> LOOP{"organized_files 그룹 순회"}
    LOOP -->|그룹| SRC{"원본 중앙 COCO 그룹?"}
    SRC -- 예 --> DROP["드롭<br/>(별도 데이터 유닛 방지)"]
    SRC -- 아니오 --> IMG["대표 이미지 경로 탐색<br/>(data_meta_1 제외 우선)"]
    IMG --> META["저장된 COCO 메타 병합<br/>coco_image_id · annotation_count · width · height"]
    META --> GRP["(group_name 시) groups=[group_name]"]
    GRP --> KEEP["kept_files에 추가"]
    DROP --> LOOP
    KEEP --> LOOP
    LOOP -->|완료| DONE(["organized_files=kept_files"])

    style DROP fill:#fff3bf,stroke:#f08c00
    style DONE fill:#d3f9d8,stroke:#2f9e44
```

### 5.3 `FinalizeCocoSplitStep` — 정리 (generate_data_units 직후)

업로드·데이터 유닛 생성이 **성공적으로 끝난 뒤** 임시 산출물을 정리합니다. 생성 파일도 재지정도 없으면 스킵.

- 분할 JSON 삭제 → `data_meta_1` asset path 원복 → `_synapse_split` 폴더 제거 → 관련 `context.params` 키 제거.
- 정상 경로는 이 단계가, **실패 경로는 `PreSplitCocoAnnotationsStep.rollback`** 이 정리를 담당(역할 분담).

---

## 6. 생성되는 메타데이터

| 키 | 설명 |
| --- | --- |
| `coco_image_id` | COCO image id |
| `coco_annotation_count` | 해당 이미지의 어노테이션 개수 |
| `coco_image_width` / `coco_image_height` | COCO 기록 이미지 크기 |
| `groups` | `group_name` 지정 시 (선택) |

---

## 7. 의존성

- `synapse-sdk`

---

## 8. 설치 / 실행 / 배포

```bash
uv sync
synapse run upload
synapse plugin publish
```
