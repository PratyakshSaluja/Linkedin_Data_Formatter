-- LinkedIn Profile Scraper Database Schema
-- This schema defines tables for storing LinkedIn profile information
-- Profiles table for storing basic LinkedIn profile information
CREATE TABLE profiles (
    id SERIAL PRIMARY KEY,
    profile_id INTEGER UNIQUE NOT NULL,
    profile_url VARCHAR(255),
    profile_pic_url VARCHAR(1000),
    full_name VARCHAR(255),
    headline TEXT,
    summary TEXT,
    country VARCHAR(100),
    city VARCHAR(100),
    email VARCHAR(255),
    contact_number VARCHAR(50),
    github VARCHAR(255),
    twitter VARCHAR(255),
    facebook VARCHAR(255),
    skills TEXT,
    connections INTEGER,
    languages VARCHAR(255),
    follower_count INTEGER,
    industry VARCHAR(255),
    fortune_500 BOOLEAN DEFAULT FALSE,
    entrepreneur BOOLEAN DEFAULT FALSE,
    leadership_role BOOLEAN DEFAULT FALSE
);

-- Education history table
CREATE TABLE educations (
    id SERIAL PRIMARY KEY,
    profile_id INTEGER NOT NULL,
    institution_name VARCHAR(255),
    degree VARCHAR(255),
    field_of_study VARCHAR(255),
    start_date VARCHAR(50),
    end_date VARCHAR(50)
);

-- Work experience table
CREATE TABLE experiences (
    id SERIAL PRIMARY KEY,
    profile_id INTEGER NOT NULL,
    title VARCHAR(255),
    company VARCHAR(255),
    location VARCHAR(255),
    description TEXT,
    start_date VARCHAR(50),
    end_date VARCHAR(50)
);

-- Club/Extracurricular experience table
CREATE TABLE club_experiences (
    id SERIAL PRIMARY KEY,
    profile_id INTEGER NOT NULL,
    club_name VARCHAR(255),
    role VARCHAR(255),
    description TEXT,
    start_date VARCHAR(50),
    end_date VARCHAR(50),
    location VARCHAR(255),
    position VARCHAR(100)
);

-- Certifications table
CREATE TABLE certifications (
    id SERIAL PRIMARY KEY,
    profile_id INTEGER NOT NULL,
    name VARCHAR(255),
    issuing_organization VARCHAR(255),
    issue_date VARCHAR(50),
    expiration_date VARCHAR(50),
    credential_id VARCHAR(255),
    credential_url VARCHAR(255)
);


-- Add foreign key constraints
ALTER TABLE educations ADD CONSTRAINT fk_educations_profile_id
    FOREIGN KEY (profile_id) REFERENCES profiles(profile_id) ON DELETE CASCADE;

ALTER TABLE experiences ADD CONSTRAINT fk_experiences_profile_id
    FOREIGN KEY (profile_id) REFERENCES profiles(profile_id) ON DELETE CASCADE;

ALTER TABLE club_experiences ADD CONSTRAINT fk_club_experiences_profile_id
    FOREIGN KEY (profile_id) REFERENCES profiles(profile_id) ON DELETE CASCADE;

ALTER TABLE certifications ADD CONSTRAINT fk_certifications_profile_id
    FOREIGN KEY (profile_id) REFERENCES profiles(profile_id) ON DELETE CASCADE;

-- Optional: Create helpful views
CREATE OR REPLACE VIEW profile_summary AS
SELECT 
    p.profile_id,
    p.full_name,
    p.headline,
    p.connections,
    p.country,
    p.city,
    p.industry,
    COUNT(DISTINCT e.id) AS experience_count,
    COUNT(DISTINCT ed.id) AS education_count,
    COUNT(DISTINCT c.id) AS certification_count,
    COUNT(DISTINCT ce.id) AS club_experience_count
FROM 
    profiles p
LEFT JOIN experiences e ON p.profile_id = e.profile_id
LEFT JOIN educations ed ON p.profile_id = ed.profile_id
LEFT JOIN certifications c ON p.profile_id = c.profile_id
LEFT JOIN club_experiences ce ON p.profile_id = ce.profile_id
GROUP BY 
    p.profile_id, p.full_name, p.headline, p.connections, p.country, p.city, p.industry;
