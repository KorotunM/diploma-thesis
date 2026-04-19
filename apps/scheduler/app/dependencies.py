from libs.storage import get_postgres_session_factory


def get_scheduler_session():
    session_factory = get_postgres_session_factory(service_name="scheduler")
    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
