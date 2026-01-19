"""Company model."""

from sqlalchemy import Column, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from app.db.base import Base


class Company(Base):
    """Company model."""

    __tablename__ = "companies"

    name = Column(String(255), nullable=False, index=True)
    domain = Column(String(255))  # company.com
    description = Column(Text)
    logo_url = Column(String(500))
    website = Column(String(500))
    
    # Contact info
    contact_info = Column(JSONB, default=dict)  # {"email": "", "phone": ""}
    
    # Metadata
    industry = Column(String(100))
    size = Column(String(50))  # startup, small, medium, large, enterprise
    location = Column(String(255))
    
    # Verification
    is_verified = Column(String(20), default="unverified")  # verified, unverified, blacklisted
    
    # Relationships
    jobs = relationship("Job", back_populates="company", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Company {self.name}>"
