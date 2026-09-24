from aether_density_builder.config import DensityBuilderSettings


def test_service_defaults_are_stable() -> None:
    settings = DensityBuilderSettings()
    assert settings.redpanda_aircraft_topic == "aircraft.position.v1"
    assert settings.redpanda_mobility_density_topic == "mobility.density.v1"
    assert settings.redpanda_demand_region_topic == "demand.region.v1"
