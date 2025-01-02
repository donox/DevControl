# database/database_manager.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError
from contextlib import contextmanager
import logging

Base = declarative_base()


class DatabaseManager:
    def __init__(self, connection_string):
        self.engine = create_engine(connection_string)
        self.Session = sessionmaker(bind=self.engine)
        self.logger = logging.getLogger(__name__)

    def init_db(self):
        """Initialize database tables"""
        Base.metadata.create_all(self.engine)

    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations"""
        session = self.Session()
        try:
            yield session
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            self.logger.error(f"Database error: {str(e)}")
            raise
        finally:
            session.close()

    def store_pipeline_data(self, step_name, data, metadata=None):
        """Store pipeline step data in the database"""
        with self.session_scope() as session:
            pipeline_data = PipelineData(
                step_name=step_name,
                data=data,
                metadata=metadata
            )
            session.add(pipeline_data)

    def get_pipeline_data(self, step_name):
        """Retrieve pipeline step data from the database"""
        with self.session_scope() as session:
            return session.query(PipelineData).filter(
                PipelineData.step_name == step_name
            ).first()