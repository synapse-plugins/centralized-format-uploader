"""Upload action for centralized-format-uploader."""

from __future__ import annotations

from plugin.steps import (
    EnrichCocoMetadataStep,
    FinalizeCocoSplitStep,
    PreSplitCocoAnnotationsStep,
)
from synapse_sdk.plugins.actions.upload import (
    DefaultUploadAction,
    UploadContext,
    UploadParams,
)
from synapse_sdk.plugins.steps import StepRegistry


class UploadAction(DefaultUploadAction[UploadParams]):
    """COCO centralized annotation uploader.

    Extends the standard 8-step workflow with three custom steps so that a single
    centralized COCO JSON is uploaded as per-image data units, regardless of
    whether the annotation spec (data_meta_1) is configured as optional or
    required in the data collection.

    Custom steps:
    1. PreSplitCocoAnnotationsStep (before organize_files): splits the centralized
       COCO JSON into per-image JSON files inside the data_meta_1 source
       directory. Each per-image JSON is named after the image's file stem so
       organize_files pairs them naturally by stem.
    2. EnrichCocoMetadataStep (after organize_files): drops the centralized
       source COCO group (if it survived) and attaches per-image COCO metadata
       and group_name to each organized_files entry.
    3. FinalizeCocoSplitStep (after generate_data_units): removes the per-image
       JSON files from the data_meta_1 directory once upload is complete. On
       failure, PreSplitCocoAnnotationsStep.rollback removes them instead.

    Extra params (via config.yaml ui_schema):
        - group_name: Group name to assign to all data units
    """

    action_name = 'upload'
    params_model = UploadParams

    def setup_steps(self, registry: StepRegistry[UploadContext]) -> None:
        super().setup_steps(registry)
        registry.insert_before('organize_files', PreSplitCocoAnnotationsStep())
        registry.insert_after('organize_files', EnrichCocoMetadataStep())
        registry.insert_after('generate_data_units', FinalizeCocoSplitStep())
