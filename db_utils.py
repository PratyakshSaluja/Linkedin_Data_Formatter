"""
Database Utility Module
---------------------

This module handles PostgreSQL database connections and operations for the LinkedIn Profile Scraper.
It provides:
1. Database connection setup with SQLAlchemy
2. Table schema definitions for storing profile data
3. Functions for inserting and retrieving data

The module uses environment variables for database configuration, allowing for seamless
deployment across development, testing, and production environments.
"""
import os
from process_logger import process_logger
from sqlalchemy import create_engine, Column, Integer, String, Boolean, ForeignKey, Text, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

# Base class for SQLAlchemy ORM models
Base = declarative_base()

class Profile(Base):
    """SQLAlchemy ORM model for LinkedIn profiles"""
    __tablename__ = 'profiles'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    profile_id = Column(Integer, unique=True, nullable=False)
    profile_url = Column(String(255))
    profile_pic_url = Column(String(1000))  # Add profile picture URL column
    full_name = Column(String(255))
    headline = Column(Text)
    summary = Column(Text)
    country = Column(String(100))
    city = Column(String(100))
    email = Column(String(255))
    contact_number = Column(String(50))
    github = Column(String(255))
    twitter = Column(String(255))
    facebook = Column(String(255))
    skills = Column(Text)
    connections = Column(Integer)
    languages = Column(String(255))
    follower_count = Column(Integer)
    industry = Column(String(255))
    fortune_500 = Column(Boolean, default=False)
    entrepreneur = Column(Boolean, default=False)
    leadership_role = Column(Boolean, default=False)

class Education(Base):
    """SQLAlchemy ORM model for education history"""
    __tablename__ = 'educations'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    profile_id = Column(Integer, nullable=False)
    institution_name = Column(String(255))
    degree = Column(String(255))
    field_of_study = Column(String(255))
    start_date = Column(String(50))
    end_date = Column(String(50))

class Experience(Base):
    """SQLAlchemy ORM model for work experiences"""
    __tablename__ = 'experiences'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    profile_id = Column(Integer, nullable=False)
    title = Column(String(255))
    company = Column(String(255))
    location = Column(String(255))
    description = Column(Text)
    start_date = Column(String(50))
    end_date = Column(String(50))

class ClubExperience(Base):
    """SQLAlchemy ORM model for club/extracurricular experiences"""
    __tablename__ = 'club_experiences'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    profile_id = Column(Integer, nullable=False)
    club_name = Column(String(255))
    role = Column(String(255))
    description = Column(Text)
    start_date = Column(String(50))
    end_date = Column(String(50))
    location = Column(String(255))
    position = Column(String(100))  # New column for position (President, VP, etc.)

class Certification(Base):
    """SQLAlchemy ORM model for certifications"""
    __tablename__ = 'certifications'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    profile_id = Column(Integer, nullable=False)
    name = Column(String(255))
    issuing_organization = Column(String(255))
    issue_date = Column(String(50))
    expiration_date = Column(String(50))
    credential_id = Column(String(255))
    credential_url = Column(String(255))

class DatabaseManager:
    """
    Database manager class for handling database operations.
    
    This class provides methods to:
    - Initialize database connection
    - Create database schema
    - Insert and retrieve data
    - Export data to pandas DataFrames
    """
    
    def __init__(self, connection_string=None):
        """
        Initialize database connection.
        
        Parameters:
        -----------
        connection_string : str, optional
            SQLAlchemy database connection string. If not provided, will look for 
            DATABASE_URL environment variable.
        """
        if connection_string is None:
            connection_string = os.getenv("DATABASE_URL")
            
        if not connection_string:
            raise ValueError("Database connection string not provided and DATABASE_URL environment variable not set")
            
        try:
            self.engine = create_engine(connection_string)
            self.Session = sessionmaker(bind=self.engine)
            process_logger.log_db_operation('connect', 'all', 'success')
        except Exception as e:
            process_logger.log_db_operation('connect', 'all', 'failed', str(e))
            raise

    def create_tables(self):
        """
        Create database tables if they don't exist.
        
        Returns:
        --------
        bool
            True if successful, False otherwise
        """
        try:
            Base.metadata.create_all(self.engine)
            process_logger.log_db_operation('create_tables', 'all', 'success')
            return True
        except SQLAlchemyError as e:
            process_logger.log_db_operation('create_tables', 'all', 'failed', str(e))
            return False

    def insert_data(self, profiles_data, educations_data, experiences_data, club_experiences_data, certifications_data=None):
        """
        Insert data into database tables.
        
        Parameters:
        -----------
        profiles_data : list
            List of dictionaries containing profile data
        educations_data : list
            List of dictionaries containing education data
        experiences_data : list
            List of dictionaries containing experience data
        club_experiences_data : list
            List of dictionaries containing club experience data
        certifications_data : list, optional
            List of dictionaries containing certification data
            
        Returns:
        --------
        bool
            True if successful, False otherwise
        """
        session = self.Session()
        try:
            # Insert profiles
            for profile in profiles_data:
                # Convert integer flags to boolean
                fortune_500 = bool(profile.get('fortune_500', 0))
                entrepreneur = bool(profile.get('entrepreneur', 0))
                leadership_role = bool(profile.get('leadership_role', 0))
                
                # Check if profile already exists
                existing_profile = session.query(Profile).filter_by(
                    profile_id=profile.get('profile_id')
                ).first()
                
                if existing_profile:
                    # Update existing profile
                    for key, value in profile.items():
                        if key == 'fortune_500':
                            setattr(existing_profile, key, fortune_500)
                        elif key == 'entrepreneur':
                            setattr(existing_profile, key, entrepreneur)
                        elif key == 'leadership_role':
                            setattr(existing_profile, key, leadership_role)
                        elif hasattr(existing_profile, key):
                            setattr(existing_profile, key, value)
                else:
                    # Create new profile
                    db_profile = Profile(
                        profile_id=profile.get('profile_id'),
                        profile_url=profile.get('profile_url', ''),
                        profile_pic_url=profile.get('profile_pic_url', ''),  # Add this line
                        full_name=profile.get('full_name', ''),
                        headline=profile.get('headline', ''),
                        summary=profile.get('summary', ''),
                        country=profile.get('country', ''),
                        city=profile.get('city', ''),
                        email=profile.get('email', ''),
                        contact_number=profile.get('contact_number', ''),
                        github=profile.get('github', ''),
                        twitter=profile.get('twitter', ''),
                        facebook=profile.get('facebook', ''),
                        skills=profile.get('skills', ''),
                        connections=profile.get('connections', 0),
                        languages=profile.get('languages', ''),
                        follower_count=profile.get('follower_count', 0),
                        industry=profile.get('industry', ''),
                        fortune_500=fortune_500,
                        entrepreneur=entrepreneur,
                        leadership_role=leadership_role
                    )
                    session.add(db_profile)
            
            # Insert educations - clear existing records first to avoid duplicates
            if profiles_data:
                profile_ids = [p.get('profile_id') for p in profiles_data]
                session.query(Education).filter(Education.profile_id.in_(profile_ids)).delete(synchronize_session=False)
                
                for edu in educations_data:
                    # Convert numpy types to Python native types
                    db_edu = Education(
                        profile_id=int(edu.get('profile_id')),  # Convert numpy.int64 to int
                        institution_name=str(edu.get('institution_name', '')),
                        degree=str(edu.get('degree', '')),
                        field_of_study=str(edu.get('field_of_study', '')),
                        start_date=str(edu.get('start_date', '')),
                        end_date=str(edu.get('end_date', ''))
                    )
                    session.add(db_edu)
            
            # Insert experiences - clear existing records first to avoid duplicates
            if profiles_data:
                profile_ids = [p.get('profile_id') for p in profiles_data]
                session.query(Experience).filter(Experience.profile_id.in_(profile_ids)).delete(synchronize_session=False)
                
                for exp in experiences_data:
                    db_exp = Experience(
                        profile_id=int(exp.get('profile_id')),  # Convert numpy.int64 to int
                        title=str(exp.get('title', '')),
                        company=str(exp.get('company', '')),
                        location=str(exp.get('location', '')),
                        description=str(exp.get('description', '')),
                        start_date=str(exp.get('start_date', '')),
                        end_date=str(exp.get('end_date', ''))
                    )
                    session.add(db_exp)
            
            # Insert club experiences - clear existing records first to avoid duplicates
            if profiles_data:
                profile_ids = [p.get('profile_id') for p in profiles_data]
                session.query(ClubExperience).filter(ClubExperience.profile_id.in_(profile_ids)).delete(synchronize_session=False)
                
                for club_exp in club_experiences_data:
                    db_club_exp = ClubExperience(
                        profile_id=int(club_exp.get('profile_id')),  # Convert numpy.int64 to int
                        club_name=str(club_exp.get('club_name', '')),
                        role=str(club_exp.get('role', '')),
                        description=str(club_exp.get('description', '')),
                        start_date=str(club_exp.get('start_date', '')),
                        end_date=str(club_exp.get('end_date', '')),
                        location=str(club_exp.get('location', '')),
                        position=str(club_exp.get('position', ''))  # Add position field
                    )
                    session.add(db_club_exp)
            
            # Insert certifications - clear existing records first to avoid duplicates
            if profiles_data and certifications_data:
                profile_ids = [p.get('profile_id') for p in profiles_data]
                session.query(Certification).filter(Certification.profile_id.in_(profile_ids)).delete(synchronize_session=False)
                
                for cert in certifications_data:
                    db_cert = Certification(
                        profile_id=int(cert.get('profile_id')),  # Convert numpy.int64 to int
                        name=str(cert.get('name', '')),
                        issuing_organization=str(cert.get('issuing_organization', '')),
                        issue_date=str(cert.get('issue_date', '')),
                        expiration_date=str(cert.get('expiration_date', '')),
                        credential_id=str(cert.get('credential_id', '')),
                        credential_url=str(cert.get('credential_url', ''))
                    )
                    session.add(db_cert)
            
            session.commit()
            process_logger.log_db_operation('insert', 'all', 'success', f"Processed {len(profiles_data)} profiles")
            for profile in profiles_data:
                process_logger.log_profile_processing(
                    profile.get('profile_id'), 
                    True,  # Excel status (assumed from excel_writer.py success)
                    True   # DB status
                )
            return True
            
        except SQLAlchemyError as e:
            session.rollback()
            process_logger.log_db_operation('insert', 'all', 'failed', str(e))
            for profile in profiles_data:
                process_logger.log_profile_processing(
                    profile.get('profile_id'),
                    True,  # Excel status (assumed from excel_writer.py success)
                    False  # DB status
                )
            return False
        finally:
            session.close()
    
    def get_max_profile_id(self):
        """Get the highest profile_id from the database."""
        session = self.Session()
        try:
            result = session.query(Profile).order_by(Profile.profile_id.desc()).first()
            return result.profile_id if result else 0
        except SQLAlchemyError as e:
            process_logger.log_db_operation('query', 'profiles', 'failed', str(e))
            return 0
        finally:
            session.close()

    def profile_exists(self, profile_url):
        """Check if a profile URL already exists in the database."""
        session = self.Session()
        try:
            existing_profile = session.query(Profile).filter(Profile.profile_url == profile_url).first()
            if existing_profile:
                process_logger.log_db_operation('check', 'profiles', 'exists', f"Found profile {profile_url}")
                return existing_profile.profile_id
            return None
        except SQLAlchemyError as e:
            process_logger.log_db_operation('check', 'profiles', 'failed', str(e))
            return None
        finally:
            session.close()

    def insert_single_profile(self, profile_data, educations_data, experiences_data, club_experiences_data, certifications_data):
        """Insert a single profile and its related data."""
        session = self.Session()
        try:
            # Check if profile already exists by URL
            existing_profile = session.query(Profile).filter(
                Profile.profile_url == profile_data.get('profile_url')
            ).first()

            if existing_profile:
                process_logger.log_db_operation(
                    'insert', 'profiles', 'skipped', 
                    f"Profile {profile_data.get('profile_url')} already exists with ID {existing_profile.profile_id}"
                )
                return False

            # Convert flags to boolean
            fortune_500 = bool(profile_data.get('fortune_500', 0))
            entrepreneur = bool(profile_data.get('entrepreneur', 0))
            leadership_role = bool(profile_data.get('leadership_role', 0))
            
            # Create new profile
            db_profile = Profile(
                profile_id=profile_data.get('profile_id'),
                profile_url=profile_data.get('profile_url'),
                profile_pic_url=profile_data.get('profile_pic_url'),
                full_name=profile_data.get('full_name'),
                headline=profile_data.get('headline'),
                summary=profile_data.get('summary'),
                country=profile_data.get('country'),
                city=profile_data.get('city'),
                email=profile_data.get('email'),
                contact_number=profile_data.get('contact_number'),
                github=profile_data.get('github'),
                twitter=profile_data.get('twitter'),
                facebook=profile_data.get('facebook'),
                skills=profile_data.get('skills'),
                connections=profile_data.get('connections'),
                languages=profile_data.get('languages'),
                follower_count=profile_data.get('follower_count'),
                industry=profile_data.get('industry'),
                fortune_500=fortune_500,
                entrepreneur=entrepreneur,
                leadership_role=leadership_role
            )
            session.add(db_profile)
            
            # Add related data
            for edu in educations_data:
                db_edu = Education(
                    profile_id=profile_data.get('profile_id'),
                    institution_name=str(edu.get('institution_name', '')),
                    degree=str(edu.get('degree', '')),
                    field_of_study=str(edu.get('field_of_study', '')),
                    start_date=str(edu.get('start_date', '')),
                    end_date=str(edu.get('end_date', ''))
                )
                session.add(db_edu)
                
            for exp in experiences_data:
                db_exp = Experience(
                    profile_id=profile_data.get('profile_id'),
                    title=str(exp.get('title', '')),
                    company=str(exp.get('company', '')),
                    location=str(exp.get('location', '')),
                    description=str(exp.get('description', '')),
                    start_date=str(exp.get('start_date', '')),
                    end_date=str(exp.get('end_date', ''))
                )
                session.add(db_exp)
                
            for club_exp in club_experiences_data:
                db_club_exp = ClubExperience(
                    profile_id=profile_data.get('profile_id'),
                    club_name=str(club_exp.get('club_name', '')),
                    role=str(club_exp.get('role', '')),
                    description=str(club_exp.get('description', '')),
                    start_date=str(club_exp.get('start_date', '')),
                    end_date=str(club_exp.get('end_date', '')),
                    location=str(club_exp.get('location', '')),
                    position=str(club_exp.get('position', ''))
                )
                session.add(db_club_exp)
                
            session.commit()
            process_logger.log_db_operation(
                'insert', 'profiles', 'success',
                f"Added new profile {profile_data.get('profile_url')}"
            )
            # Add certifications
            for cert in certifications_data:
                db_cert = Certification(
                    profile_id=profile_data.get('profile_id'),
                    name=str(cert.get('name', '')),
                    issuing_organization=str(cert.get('issuing_organization', '')),
                    issue_date=str(cert.get('issue_date', '')),
                    expiration_date=str(cert.get('expiration_date', '')),
                    credential_id=str(cert.get('credential_id', '')),
                    credential_url=str(cert.get('credential_url', ''))
                )
                session.add(db_cert)

            session.commit()
            process_logger.log_db_operation(
                'insert', 'profiles', 'success',
                f"Added new profile {profile_data.get('profile_url')}"
            )
            return True

        except SQLAlchemyError as e:
            session.rollback()
            process_logger.log_db_operation('insert', 'single_profile', 'failed', str(e))
            return False
        finally:
            session.close()

    def get_all_data(self):
        """
        Retrieve all data from database.
        
        Returns:
        --------
        tuple
            (profiles_df, educations_df, experiences_df, club_experiences_df, certifications_df) as pandas DataFrames
        """
        session = self.Session()
        try:
            profiles = session.query(Profile).all()
            educations = session.query(Education).all()
            experiences = session.query(Experience).all()
            club_experiences = session.query(ClubExperience).all()
            certifications = session.query(Certification).all()
            
            # Convert to dictionaries
            profiles_dicts = [
                {c.name: getattr(p, c.name) for c in p.__table__.columns}
                for p in profiles
            ]
            educations_dicts = [
                {c.name: getattr(e, c.name) for c in e.__table__.columns}
                for e in educations
            ]
            experiences_dicts = [
                {c.name: getattr(e, c.name) for c in e.__table__.columns}
                for e in experiences
            ]
            club_experiences_dicts = [
                {c.name: getattr(ce, c.name) for c in ce.__table__.columns}
                for ce in club_experiences
            ]
            certifications_dicts = [
                {c.name: getattr(cert, c.name) for c in cert.__table__.columns}
                for cert in certifications
            ]
            
            # Convert to DataFrames
            profiles_df = pd.DataFrame(profiles_dicts)
            educations_df = pd.DataFrame(educations_dicts)
            experiences_df = pd.DataFrame(experiences_dicts)
            club_experiences_df = pd.DataFrame(club_experiences_dicts)
            certifications_df = pd.DataFrame(certifications_dicts)
            
            return profiles_df, educations_df, experiences_df, club_experiences_df, certifications_df
            
        except SQLAlchemyError as e:
            process_logger.log_db_operation('retrieve', 'all', 'failed', str(e))
            return None, None, None, None, None
        finally:
            session.close()
