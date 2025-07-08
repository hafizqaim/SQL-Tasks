-- Movies Table

CREATE TABLE movies (
    id INT PRIMARY KEY,
    title VARCHAR(255)
);

-- Cast Table

CREATE TABLE cast (
    movie_id INT,
    cast_id INT,
    character_name VARCHAR(255),
    credit_id VARCHAR(255),
    gender INT,
    name VARCHAR(255),
    cast_order INT,
    profile_path VARCHAR(255),
    FOREIGN KEY (movie_id) REFERENCES movies(id)
);

-- Crew Table

CREATE TABLE crew (
    movie_id INT,
    credit_id VARCHAR(255),
    department VARCHAR(255),
    gender INT,
    job VARCHAR(255),
    name VARCHAR(255),
    profile_path VARCHAR(255),
    FOREIGN KEY (movie_id) REFERENCES movies(id)
);

-- Users Table

CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255)
);

-- Ratings Table

CREATE TABLE ratings (
    user_id INT,
    movie_id INT,
    rating DECIMAL(2, 1),
    timestamp BIGINT,
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (movie_id) REFERENCES movies(id)
);