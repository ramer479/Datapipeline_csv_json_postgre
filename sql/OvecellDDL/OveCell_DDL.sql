-- User Dimension 
-- Staging
DROP TABLE IF EXISTS ovecell.dim_user_stg ;
CREATE TABLE ovecell.dim_user_stg (
    user_id VARCHAR(50) UNIQUE,
    gender VARCHAR(50),
    title VARCHAR(50),
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    email VARCHAR(255),
    dob DATE,
    registered_date DATE,
    phone VARCHAR(50),
    cell VARCHAR(50),
	ip_address VARCHAR(100),
    nationality VARCHAR(50),
	source VARCHAR(50),
	load_ingstn_id VARCHAR(10),
	load_dtm TIMESTAMP
);

-- Main Table 
DROP TABLE IF EXISTS ovecell.dim_user ; 
CREATE TABLE ovecell.dim_user (
    user_id VARCHAR(50) UNIQUE,
    gender VARCHAR(50),
    title VARCHAR(50),
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    email VARCHAR(255),
    dob DATE,
    registered_date DATE,
    phone VARCHAR(50),
    cell VARCHAR(50),
	ip_address VARCHAR(100),
    nationality VARCHAR(50),
	source VARCHAR(50),
	load_ingstn_id VARCHAR(10),
	load_dtm TIMESTAMP
);

-- SELECT * FROM ovecell.dim_user_stg;


-- Location Dimension Stage
DROP TABLE IF EXISTS ovecell.dim_location_stg ;
CREATE TABLE ovecell.dim_location_stg (
    location_id VARCHAR(50) PRIMARY KEY,
    street VARCHAR(255),
    city VARCHAR(255),
    state VARCHAR(255),
    postcode VARCHAR(50),
	nationality VARCHAR(10),
	load_ingstn_id VARCHAR(10),
	load_dtm TIMESTAMP
);

-- Location Dimension
DROP TABLE IF EXISTS ovecell.dim_location ;
CREATE TABLE ovecell.dim_location (
    location_id VARCHAR(50) PRIMARY KEY,
    street VARCHAR(255),
    city VARCHAR(255),
    state VARCHAR(255),
    postcode VARCHAR(50),
	nationality VARCHAR(10),
	load_ingstn_id VARCHAR(10),
	load_dtm TIMESTAMP
);


-- User Fact
DROP TABLE IF EXISTS ovecell.fact_user_stg ;
CREATE TABLE ovecell.fact_user_stg (
    user_id VARCHAR(50) UNIQUE,
    location_id VARCHAR(50),
	source VARCHAR(50),
	load_ingstn_id VARCHAR(10),
	load_dtm TIMESTAMP,
	FOREIGN KEY (user_id) REFERENCES ovecell.dim_user(user_id),
    FOREIGN KEY (location_id) REFERENCES ovecell.dim_location(location_id)
);



DROP TABLE IF EXISTS ovecell.fact_user ;
CREATE TABLE ovecell.fact_user (
    user_id VARCHAR(50) UNIQUE,
    location_id VARCHAR(50),
	source VARCHAR(50),
	load_ingstn_id VARCHAR(10),
	load_dtm TIMESTAMP,
	FOREIGN KEY (user_id) REFERENCES ovecell.dim_user(user_id),
    FOREIGN KEY (location_id) REFERENCES ovecell.dim_location(location_id)
);



--------------------------------------------------------------------
--------------------- Extra Dimensions -----------------------------
--------------------------------------------------------------------
-- Login Dimension not part of pipeline 
CREATE TABLE Login_Dimension (
    login_id SERIAL PRIMARY KEY,
    username VARCHAR(255),
    password VARCHAR(255),
    salt VARCHAR(255),
    md5 VARCHAR(255),
    sha1 VARCHAR(255),
    sha256 VARCHAR(255)
);

-- Picture Dimension not part of pipeline 
CREATE TABLE Picture_Dimension (
    picture_id SERIAL PRIMARY KEY,
    large_picture_url VARCHAR(500),
    medium_picture_url VARCHAR(500),
    thumbnail_picture_url VARCHAR(500)
);

