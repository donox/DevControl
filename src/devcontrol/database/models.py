# database/models.py
from sqlalchemy import Column, Integer, String, JSON, DateTime
from sqlalchemy.sql import func
from .database_manager import Base


class PipelineData(Base):
    __tablename__ = 'pipeline_data'

    id = Column(Integer, primary_key=True)
    step_name = Column(String, nullable=False)
    data = Column(JSON)
    metadata = Column(JSON)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())