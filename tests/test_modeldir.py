"""Deriving a model's sync directory from a GPUStack model-file record.
The trap: model names contain dots (Qwen3.6...4.6), so naive splitext breaks."""

from modelsync.gpustack import _model_dir

ORG = "/var/lib/gpustack/cache/huggingface/maci0"
MODEL = f"{ORG}/Qwen3.6-40B-Claude-4.6-Opus-NVFP4"  # dir, dots in name


def test_local_dir_wins():
    assert _model_dir({"local_dir": MODEL + "/"}) == MODEL


def test_local_path_fallback_keeps_full_dir():
    # the bug we fixed: must NOT collapse to the org dir
    assert _model_dir({"local_path": MODEL}) == MODEL


def test_resolved_dir_kept_as_is():
    assert _model_dir({"resolved_paths": [MODEL]}) == MODEL
    assert _model_dir({"resolved_paths": [MODEL]}) != ORG


def test_weight_file_steps_up_to_its_folder():
    assert _model_dir({"resolved_paths": [f"{MODEL}/model-Q4.gguf"]}) == MODEL
    assert _model_dir({"resolved_paths": [f"{MODEL}/model.safetensors"]}) == MODEL


def test_empty():
    assert _model_dir({}) is None
    assert _model_dir({"resolved_paths": []}) is None
