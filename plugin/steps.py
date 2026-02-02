"""Custom steps for centralized-format-uploader plugin."""

from __future__ import annotations

import json
import os
import shutil
from collections import defaultdict
from pathlib import Path
from typing import Any

from synapse_sdk.plugins.actions.upload.context import UploadContext
from synapse_sdk.plugins.steps import BaseStep, StepResult
from synapse_sdk.utils.storage import get_pathlib


class SplitCocoAnnotationsStep(BaseStep[UploadContext]):
    """Split a single COCO JSON annotation file into per-image JSONs.

    After organize_files groups images (without labels), this step:
    1. Finds the COCO annotation JSON file from data_meta_1 spec directory
    2. Parses it and builds image_filename → annotations mapping
    3. Creates per-image JSON files in a temp directory
    4. Adds annotation file paths to each organized_file entry

    Prerequisite: The annotation/label file spec must be **optional** in the
    data collection so that organize_files keeps image-only groups.

    Reads extra_params from context:
        - group_name (str | None): Group name to assign to all data units.
    """

    @property
    def name(self) -> str:
        return 'split_coco_annotations'

    @property
    def progress_weight(self) -> float:
        return 0.10

    def can_skip(self, context: UploadContext) -> bool:
        """Skip if there are no organized files to process."""
        return not context.organized_files

    def execute(self, context: UploadContext) -> StepResult:
        extra = context.params.get('extra_params') or {}
        group_name = extra.get('group_name')

        try:
            # 1. Find the COCO JSON file
            annotation_path = self._find_coco_json(context)
            if annotation_path is None:
                return StepResult(
                    success=False,
                    error='COCO JSON annotation file not found in data_meta_1 directory.',
                )

            context.log(
                'coco_annotation_found',
                {'path': str(annotation_path)},
            )

            # 2. Parse COCO JSON
            coco_data = json.loads(annotation_path.read_text(encoding='utf-8'))

            if not self._is_valid_coco(coco_data):
                return StepResult(
                    success=False,
                    error=f'Invalid COCO format: {annotation_path}. '
                    'Must contain "images" and "annotations" keys.',
                )

            # 3. Build image_filename → image mapping
            image_map: dict[str, dict[str, Any]] = {
                img['file_name']: img for img in coco_data['images']
            }

            # 4. Build image_id → [annotations] mapping
            ann_map: dict[int, list[dict[str, Any]]] = defaultdict(list)
            for ann in coco_data['annotations']:
                ann_map[ann['image_id']].append(ann)

            # 5. Shared data preserved in every per-image JSON
            shared: dict[str, Any] = {
                'categories': coco_data.get('categories', []),
            }
            if 'info' in coco_data:
                shared['info'] = coco_data['info']
            if 'licenses' in coco_data:
                shared['licenses'] = coco_data['licenses']

            # 6. Determine annotation spec name
            annotation_spec_name = self._find_annotation_spec_name(context)

            # 7. Create temp directory for per-image JSONs
            temp_dir = self._create_temp_directory(context)

            # 8. Generate per-image JSONs and update organized_files
            total_split = 0
            processed_files: list[dict[str, Any]] = []

            for file_group in context.organized_files:
                image_path = self._get_primary_image_path(file_group)
                if image_path is None:
                    processed_files.append(file_group)
                    continue

                image_filename = image_path.name
                coco_image = image_map.get(image_filename)
                if coco_image is None:
                    processed_files.append(file_group)
                    continue

                image_id = coco_image['id']
                annotations = ann_map.get(image_id, [])

                per_image_json: dict[str, Any] = {
                    **shared,
                    'images': [coco_image],
                    'annotations': annotations,
                }

                output_path = temp_dir / f'{image_path.stem}.json'
                output_path.write_text(
                    json.dumps(per_image_json, ensure_ascii=False, indent=2),
                    encoding='utf-8',
                )

                # Update organized_file entry
                file_group['files'][annotation_spec_name] = output_path

                meta = file_group.get('meta', {})
                meta.update({
                    'coco_image_id': image_id,
                    'coco_annotation_count': len(annotations),
                    'coco_image_width': coco_image.get('width'),
                    'coco_image_height': coco_image.get('height'),
                })
                file_group['meta'] = meta

                if group_name:
                    file_group['groups'] = [group_name]

                processed_files.append(file_group)
                total_split += 1

            context.organized_files = processed_files

            # 9. Register temp directory for cleanup
            context.params['cleanup_temp'] = True
            context.params['temp_path'] = str(temp_dir)

            context.log(
                'coco_annotation_split_complete',
                {
                    'total_images': len(image_map),
                    'total_annotations': len(coco_data['annotations']),
                    'total_split': total_split,
                    'annotation_spec': annotation_spec_name,
                },
            )

            return StepResult(
                success=True,
                data={
                    'annotations_split': total_split,
                    'total_coco_images': len(image_map),
                    'total_coco_annotations': len(coco_data['annotations']),
                },
                rollback_data={'temp_dir': str(temp_dir)},
            )

        except Exception as e:
            return StepResult(
                success=False,
                error=f'COCO annotation splitting failed: {e}',
            )

    def rollback(self, context: UploadContext, result: StepResult) -> None:
        temp_dir = result.rollback_data.get('temp_dir')
        if temp_dir and Path(temp_dir).exists():
            shutil.rmtree(temp_dir, ignore_errors=True)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _find_coco_json(self, context: UploadContext) -> Path | None:
        """Find the COCO JSON annotation file from data_meta_1 spec directory.

        Returns the first JSON file found in the data_meta_1 spec directory.
        """
        data_meta_1_dir = self._get_data_meta_1_directory(context)
        if data_meta_1_dir is None or not data_meta_1_dir.exists():
            return None

        # Get the first JSON file from data_meta_1 directory
        json_files = sorted(data_meta_1_dir.glob('*.json'))
        if json_files:
            first_json = json_files[0]
            context.log(
                'coco_json_found',
                {'spec': 'data_meta_1', 'file': first_json.name},
            )
            return first_json

        return None

    def _get_data_meta_1_directory(self, context: UploadContext) -> Path | None:
        """Get the data_meta_1 spec directory path."""
        if context.use_single_path:
            # Single-path mode: data_meta_1 directory under pathlib_cwd
            if context.pathlib_cwd:
                return context.pathlib_cwd / 'data_meta_1'
        else:
            # Multi-path mode: use assets configuration
            assets = context.params.get('assets', {})
            if 'data_meta_1' in assets:
                asset_config = assets['data_meta_1']
                asset_path_str = asset_config.get('path', '')
                if asset_path_str:
                    try:
                        storage_config = {
                            'provider': context.storage.provider,
                            'configuration': context.storage.configuration,
                        }
                        return get_pathlib(storage_config, asset_path_str)
                    except Exception:
                        pass
        return None

    def _is_valid_coco(self, data: Any) -> bool:
        """Check if the data has the minimum COCO format structure."""
        return (
            isinstance(data, dict)
            and 'images' in data
            and 'annotations' in data
            and isinstance(data['images'], list)
            and isinstance(data['annotations'], list)
        )

    def _find_annotation_spec_name(self, context: UploadContext) -> str:
        """Determine the annotation file spec name.

        Priority:
        1. Auto-detect from file specifications (spec allowing .json)
        2. Find spec not present in organized_files
        3. Default: 'label'
        """
        # 1. Auto-detect: find spec that allows .json
        file_specifications = self._get_file_specifications(context)
        for spec in file_specifications:
            extensions = spec.get('extensions', [])
            if '.json' in extensions:
                return spec.get('name', 'label')

        # 2. Find spec not present in organized_files
        present_specs = set()
        for file_group in context.organized_files:
            present_specs.update(file_group.get('files', {}).keys())

        for spec in file_specifications:
            name = spec.get('name', '')
            if name and name not in present_specs:
                return name

        return 'label'

    def _get_file_specifications(self, context: UploadContext) -> list[dict[str, Any]]:
        """Get file specifications from data collection."""
        if context.data_collection:
            return context.data_collection.get('file_specifications', [])
        return []

    def _get_primary_image_path(self, file_group: dict[str, Any]) -> Path | None:
        """Get the primary image file path from a file group.

        Returns the first file path found in the files dict.
        """
        files_dict = file_group.get('files', {})
        for file_path in files_dict.values():
            if isinstance(file_path, list):
                file_path = file_path[0] if file_path else None
            if file_path is not None:
                return Path(file_path) if not isinstance(file_path, Path) else file_path
        return None

    def _create_temp_directory(self, context: UploadContext) -> Path:
        """Create a temporary directory for per-image JSON files."""
        base = context.pathlib_cwd if context.pathlib_cwd else Path(os.getcwd())
        temp_dir = base / 'temp_coco_annotations'
        temp_dir.mkdir(parents=True, exist_ok=True)
        return temp_dir
