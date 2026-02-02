"""Upload action for centralized-format-uploader."""

from __future__ import annotations

from plugin.steps import SplitCocoAnnotationsStep
from synapse_sdk.plugins.actions.upload import (
    DefaultUploadAction,
    UploadContext,
    UploadParams,
)
from synapse_sdk.plugins.steps import StepRegistry


class UploadAction(DefaultUploadAction[UploadParams]):
    """COCO centralized annotation uploader.

    Extends the standard 8-step workflow by inserting a SplitCocoAnnotationsStep
    after organize_files. The custom step finds the first JSON file in the
    data_meta_1 spec directory and splits it into per-image JSON files,
    pairing each with its corresponding image to create individual data units.

    Extra params (via config.yaml ui_schema):
        - group_name: Group name to assign to all data units
    """

    action_name = 'upload'
    params_model = UploadParams

    def setup_steps(self, registry: StepRegistry[UploadContext]) -> None:
        super().setup_steps(registry)
        registry.insert_after('organize_files', SplitCocoAnnotationsStep())
