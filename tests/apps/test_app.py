def test_importing_app():
    # this will raise an exception if pydantic model validation fails for the app
    from nomad_avl_fire_rdm.apps import app_entry_point

    assert app_entry_point.app.label == 'NewApp'
