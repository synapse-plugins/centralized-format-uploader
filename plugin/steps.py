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


def _find_annotation_spec_name_from_specs(specs: list[dict[str, Any]]) -> str | None:
    """Find the annotation spec name from file specifications.

    Priority:
    1. Spec named 'data_meta_1' (plugin convention)
    2. Spec that allows .json extension (normalized comparison)
    """
    for spec in specs:
        if spec.get('name') == 'data_meta_1':
            return 'data_meta_1'

    for spec in specs:
        extensions = spec.get('extensions', [])
        normalized = [ext.lower().lstrip('.') for ext in extensions]
        if 'json' in normalized:
            return spec.get('name')

    return None


def _delete_files(file_paths: list[str]) -> None:
    """Best-effort deletion of a list of file paths."""
    for path_str in file_paths:
        try:
            p = Path(path_str)
            if p.exists() and p.is_file():
                p.unlink()
        except Exception:
            pass


def _get_data_meta_1_directory(context: UploadContext) -> Path | None:
    """Get the data_meta_1 spec directory path for both single- and multi-path modes."""
    if context.use_single_path:
        if context.pathlib_cwd:
            return context.pathlib_cwd / 'data_meta_1'
        return None

    assets = context.params.get('assets', {})
    if 'data_meta_1' not in assets:
        return None
    asset_path_str = assets['data_meta_1'].get('path', '')
    if not asset_path_str:
        return None
    try:
        storage_config = {
            'provider': context.storage.provider,
            'configuration': context.storage.configuration,
        }
        return get_pathlib(storage_config, asset_path_str)
    except Exception:
        return None


def _is_valid_coco(data: Any) -> bool:
    """Check if the data has the minimum COCO format structure."""
    return (
        isinstance(data, dict)
        and 'images' in data
        and 'annotations' in data
        and isinstance(data['images'], list)
        and isinstance(data['annotations'], list)
    )


SHARED_PATH_SPLIT_SUBDIR = '_synapse_split'


class PreSplitCocoAnnotationsStep(BaseStep[UploadContext]):
    """Split the centralized COCO JSON into per-image JSONs before organize_files.

    This step runs BEFORE organize_files so that organize_files can naturally pair
    each image with its corresponding per-image JSON (matched by file stem), even
    when the annotation spec (data_meta_1) is configured as required in the data
    collection.

    Behavior:
    1. Locates the COCO JSON file inside the data_meta_1 spec directory
    2. Parses it and validates the COCO structure
    3. Determines a write target:
       - Default: data_meta_1 directory itself
       - If multi-path mode AND image_1 path == data_meta_1 path: uses a
         '_synapse_split' subdirectory and redirects assets['data_meta_1']['path']
         to the subdirectory. This avoids the SDK's same-path ambiguity in
         FlatFileDiscoveryStrategy._find_matching_spec where two specs sharing the
         same directory cause all files to collapse onto one spec.
    4. Creates per-image JSON files at the target, named after each image's file
       stem (e.g. cat.jpg → <target>/cat.json)
    5. Stores per-image COCO metadata, created files, and any path redirection
       state in context.params for downstream steps (enrichment, cleanup, rollback)

    The original centralized COCO JSON is left untouched. Its dataset group (built
    from its stem) is dropped explicitly by EnrichCocoMetadataStep so it does not
    get uploaded as a data unit.
    """

    PARAM_CREATED_FILES = '_pre_split_created_files'
    PARAM_COCO_METADATA = '_coco_image_metadata'
    PARAM_SOURCE_COCO_PATH = '_pre_split_source_coco_path'
    PARAM_REDIRECT_ORIGINAL_PATH = '_pre_split_original_data_meta_1_path'
    PARAM_CREATED_SUBDIR = '_pre_split_created_subdir'

    @property
    def name(self) -> str:
        return 'pre_split_coco_annotations'

    @property
    def progress_weight(self) -> float:
        return 0.05

    def can_skip(self, context: UploadContext) -> bool:
        return context.data_collection is None

    def execute(self, context: UploadContext) -> StepResult:
        try:
            data_meta_1_dir = _get_data_meta_1_directory(context)
            if data_meta_1_dir is None or not data_meta_1_dir.exists():
                context.log(
                    'pre_split_skipped_no_directory',
                    {'reason': 'data_meta_1 directory not found'},
                )
                return StepResult(success=True, data={'split': False})

            json_files = sorted(data_meta_1_dir.glob('*.json'))
            if not json_files:
                context.log(
                    'pre_split_skipped_no_json',
                    {'dir': str(data_meta_1_dir)},
                )
                return StepResult(success=True, data={'split': False})

            coco_path = json_files[0]
            coco_data = json.loads(coco_path.read_text(encoding='utf-8'))

            if not _is_valid_coco(coco_data):
                return StepResult(
                    success=False,
                    error=(
                        f'Invalid COCO format: {coco_path}. '
                        'Must contain "images" and "annotations" keys.'
                    ),
                )

            # Decide where to write the per-image JSONs and whether to redirect
            # the data_meta_1 asset path (multi-path same-path workaround).
            target_dir, redirect = self._resolve_target_dir(context, data_meta_1_dir)

            image_map: dict[str, dict[str, Any]] = {
                img['file_name']: img for img in coco_data['images']
            }

            ann_map: dict[int, list[dict[str, Any]]] = defaultdict(list)
            for ann in coco_data['annotations']:
                ann_map[ann['image_id']].append(ann)

            shared: dict[str, Any] = {
                'categories': coco_data.get('categories', []),
            }
            if 'info' in coco_data:
                shared['info'] = coco_data['info']
            if 'licenses' in coco_data:
                shared['licenses'] = coco_data['licenses']

            created_files: list[str] = []
            coco_metadata: dict[str, dict[str, Any]] = {}
            coco_path_resolved = coco_path.resolve()

            for image_filename, coco_image in image_map.items():
                stem = Path(image_filename).stem
                if not stem:
                    continue
                image_id = coco_image['id']
                annotations = ann_map.get(image_id, [])

                per_image_json = {
                    **shared,
                    'images': [coco_image],
                    'annotations': annotations,
                }

                output_path = target_dir / f'{stem}.json'

                # Never overwrite the source COCO file
                if output_path.resolve() == coco_path_resolved:
                    continue

                output_path.write_text(
                    json.dumps(per_image_json, ensure_ascii=False, indent=2),
                    encoding='utf-8',
                )
                created_files.append(str(output_path))

                coco_metadata[image_filename] = {
                    'coco_image_id': image_id,
                    'coco_annotation_count': len(annotations),
                    'coco_image_width': coco_image.get('width'),
                    'coco_image_height': coco_image.get('height'),
                }

            context.params[self.PARAM_CREATED_FILES] = created_files
            context.params[self.PARAM_COCO_METADATA] = coco_metadata
            context.params[self.PARAM_SOURCE_COCO_PATH] = str(coco_path)
            if redirect is not None:
                context.params[self.PARAM_REDIRECT_ORIGINAL_PATH] = redirect['original_path']
                context.params[self.PARAM_CREATED_SUBDIR] = redirect['subdir_str']

            context.log(
                'pre_split_coco_complete',
                {
                    'source': str(coco_path),
                    'total_images': len(image_map),
                    'total_annotations': len(coco_data['annotations']),
                    'files_created': len(created_files),
                    'target_dir': str(target_dir),
                    'redirected': redirect is not None,
                },
            )

            return StepResult(
                success=True,
                data={
                    'split': True,
                    'files_created': len(created_files),
                    'total_coco_images': len(image_map),
                    'total_coco_annotations': len(coco_data['annotations']),
                    'redirected': redirect is not None,
                },
                rollback_data={
                    'created_files': list(created_files),
                    'redirect': redirect,
                },
            )

        except Exception as e:
            return StepResult(
                success=False,
                error=f'Pre-split COCO failed: {e}',
            )

    def rollback(self, context: UploadContext, result: StepResult) -> None:
        rb = result.rollback_data or {}
        files = rb.get('created_files', [])
        _delete_files(files)

        redirect = rb.get('redirect')
        if redirect:
            self._undo_redirect(context, redirect)

        context.params.pop(self.PARAM_CREATED_FILES, None)
        context.params.pop(self.PARAM_COCO_METADATA, None)
        context.params.pop(self.PARAM_SOURCE_COCO_PATH, None)
        context.params.pop(self.PARAM_REDIRECT_ORIGINAL_PATH, None)
        context.params.pop(self.PARAM_CREATED_SUBDIR, None)

    def _resolve_target_dir(
        self,
        context: UploadContext,
        data_meta_1_dir: Path,
    ) -> tuple[Path, dict[str, Any] | None]:
        """Decide where to write per-image JSONs.

        Returns:
            (target_dir, redirect_info or None). When redirect_info is set, the
            data_meta_1 asset path has been rewritten to point at a subdirectory
            so the SDK does not confuse same-path specs.
        """
        if context.use_single_path:
            return data_meta_1_dir, None

        assets = context.params.get('assets') or {}
        image_1_cfg = assets.get('image_1') or {}
        data_meta_1_cfg = assets.get('data_meta_1') or {}

        image_1_path = (image_1_cfg.get('path') or '').rstrip('/')
        data_meta_1_path = (data_meta_1_cfg.get('path') or '').rstrip('/')

        if not image_1_path or not data_meta_1_path:
            return data_meta_1_dir, None
        if image_1_path != data_meta_1_path:
            return data_meta_1_dir, None

        # Same-path conflict: redirect data_meta_1 to a subdirectory so the SDK
        # can distinguish the two specs by directory depth.
        subdir = data_meta_1_dir / SHARED_PATH_SPLIT_SUBDIR
        subdir.mkdir(parents=True, exist_ok=True)

        new_asset_path = f'{data_meta_1_path}/{SHARED_PATH_SPLIT_SUBDIR}'
        assets['data_meta_1']['path'] = new_asset_path
        context.params['assets'] = assets

        context.log(
            'pre_split_asset_path_redirected',
            {'from': data_meta_1_path, 'to': new_asset_path},
        )

        return subdir, {
            'original_path': data_meta_1_path,
            'subdir_str': str(subdir),
        }

    def _undo_redirect(self, context: UploadContext, redirect: dict[str, Any]) -> None:
        """Restore the original data_meta_1 asset path and remove the subdir."""
        original_path = redirect.get('original_path')
        subdir_str = redirect.get('subdir_str')

        if original_path:
            assets = context.params.get('assets') or {}
            if 'data_meta_1' in assets:
                assets['data_meta_1']['path'] = original_path
                context.params['assets'] = assets

        if subdir_str:
            try:
                subdir = Path(subdir_str)
                if subdir.exists() and subdir.is_dir():
                    shutil.rmtree(subdir, ignore_errors=True)
            except Exception:
                pass


class EnrichCocoMetadataStep(BaseStep[UploadContext]):
    """Attach COCO image metadata and group_name to each organized_files entry.

    Runs AFTER organize_files. By this point each image has already been paired
    with its per-image JSON (created by PreSplitCocoAnnotationsStep) by stem.
    This step just enriches the meta dict, applies group_name, and drops the
    centralized source COCO JSON group if it survived organize_files.

    Reads extra_params from context:
        - group_name (str | None): Group name to assign to all data units.
    """

    @property
    def name(self) -> str:
        return 'enrich_coco_metadata'

    @property
    def progress_weight(self) -> float:
        return 0.05

    def can_skip(self, context: UploadContext) -> bool:
        return not context.organized_files

    def execute(self, context: UploadContext) -> StepResult:
        try:
            extra = context.params.get('extra_params') or {}
            group_name = extra.get('group_name')
            coco_metadata: dict[str, dict[str, Any]] = (
                context.params.get(PreSplitCocoAnnotationsStep.PARAM_COCO_METADATA) or {}
            )
            source_coco_path_str = context.params.get(
                PreSplitCocoAnnotationsStep.PARAM_SOURCE_COCO_PATH
            )
            source_coco_resolved = (
                Path(source_coco_path_str).resolve() if source_coco_path_str else None
            )

            enriched_count = 0
            kept_files: list[dict[str, Any]] = []

            for file_group in context.organized_files:
                if self._is_source_coco_group(file_group, source_coco_resolved):
                    # Drop the centralized source COCO JSON so it isn't uploaded
                    # as a separate data unit.
                    continue

                image_path = self._get_primary_image_path(file_group)
                if image_path is not None:
                    img_meta = coco_metadata.get(image_path.name)
                    if img_meta:
                        meta = file_group.get('meta', {})
                        meta.update(img_meta)
                        file_group['meta'] = meta
                        enriched_count += 1

                if group_name:
                    file_group['groups'] = [group_name]

                kept_files.append(file_group)

            context.organized_files = kept_files

            context.log(
                'enrich_coco_metadata_complete',
                {
                    'enriched': enriched_count,
                    'total_groups': len(kept_files),
                    'group_name': group_name,
                },
            )

            return StepResult(
                success=True,
                data={'enriched': enriched_count, 'total_groups': len(kept_files)},
            )

        except Exception as e:
            return StepResult(
                success=False,
                error=f'Enrich COCO metadata failed: {e}',
            )

    def _is_source_coco_group(
        self,
        file_group: dict[str, Any],
        source_coco_resolved: Path | None,
    ) -> bool:
        if source_coco_resolved is None:
            return False
        files_dict = file_group.get('files', {})
        for file_path in files_dict.values():
            if isinstance(file_path, list):
                file_path = file_path[0] if file_path else None
            if file_path is None:
                continue
            try:
                if Path(file_path).resolve() == source_coco_resolved:
                    return True
            except Exception:
                continue
        return False

    def _get_primary_image_path(self, file_group: dict[str, Any]) -> Path | None:
        files_dict = file_group.get('files', {})
        # Prefer non-data_meta_1 file (image)
        for spec_name, file_path in files_dict.items():
            if spec_name == 'data_meta_1':
                continue
            if isinstance(file_path, list):
                file_path = file_path[0] if file_path else None
            if file_path is not None:
                return Path(file_path) if not isinstance(file_path, Path) else file_path
        # Fallback: first file regardless of spec
        for file_path in files_dict.values():
            if isinstance(file_path, list):
                file_path = file_path[0] if file_path else None
            if file_path is not None:
                return Path(file_path) if not isinstance(file_path, Path) else file_path
        return None


class FinalizeCocoSplitStep(BaseStep[UploadContext]):
    """Remove per-image JSON files and any helper subdirectory created by
    PreSplitCocoAnnotationsStep, and restore the data_meta_1 asset path.

    Runs after upload and data unit generation have completed successfully so the
    files exist during upload but are cleaned afterward. On failure paths,
    PreSplitCocoAnnotationsStep.rollback handles cleanup instead.
    """

    @property
    def name(self) -> str:
        return 'finalize_coco_split'

    @property
    def progress_weight(self) -> float:
        return 0.01

    def can_skip(self, context: UploadContext) -> bool:
        has_files = bool(context.params.get(PreSplitCocoAnnotationsStep.PARAM_CREATED_FILES))
        has_redirect = bool(context.params.get(PreSplitCocoAnnotationsStep.PARAM_REDIRECT_ORIGINAL_PATH))
        return not (has_files or has_redirect)

    def execute(self, context: UploadContext) -> StepResult:
        files = context.params.get(PreSplitCocoAnnotationsStep.PARAM_CREATED_FILES, [])
        _delete_files(files)

        subdir_str = context.params.get(PreSplitCocoAnnotationsStep.PARAM_CREATED_SUBDIR)
        original_path = context.params.get(PreSplitCocoAnnotationsStep.PARAM_REDIRECT_ORIGINAL_PATH)

        if original_path:
            assets = context.params.get('assets') or {}
            if 'data_meta_1' in assets:
                assets['data_meta_1']['path'] = original_path
                context.params['assets'] = assets

        if subdir_str:
            try:
                subdir = Path(subdir_str)
                if subdir.exists() and subdir.is_dir():
                    shutil.rmtree(subdir, ignore_errors=True)
            except Exception:
                pass

        context.params.pop(PreSplitCocoAnnotationsStep.PARAM_CREATED_FILES, None)
        context.params.pop(PreSplitCocoAnnotationsStep.PARAM_REDIRECT_ORIGINAL_PATH, None)
        context.params.pop(PreSplitCocoAnnotationsStep.PARAM_CREATED_SUBDIR, None)

        context.log(
            'pre_split_files_cleaned',
            {'count': len(files), 'subdir_removed': bool(subdir_str)},
        )
        return StepResult(
            success=True,
            data={'cleaned': len(files), 'subdir_removed': bool(subdir_str)},
        )
