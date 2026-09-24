from aether_density_builder.config import DensityBuilderSettings


def test_settings_defaults_follow_plan() -> None:
    settings = DensityBuilderSettings()
    assert settings.h3_resolution == 4
    assert settings.aggregation_window_seconds == 180
    assert settings.emit_interval_seconds == 10
    assert settings.airborne_weight == 1.0
    assert settings.on_ground_weight == 0.35
