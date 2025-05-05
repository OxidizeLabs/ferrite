-- Create test tables
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE posts (
    id INTEGER PRIMARY KEY,
    user_id INTEGER,
    title VARCHAR(200) NOT NULL,
    content TEXT,
    published_at TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

-- Insert sample data into users
INSERT INTO users (id, username, email, created_at) VALUES
(1, 'john_doe', 'john@example.com', TIMESTAMP '2023-01-01 10:00:00'),
(2, 'jane_smith', 'jane@example.com', TIMESTAMP '2023-01-01 10:00:00'),
(3, 'bob_wilson', 'bob@example.com', TIMESTAMP '2023-01-01 10:00:00');

-- Insert sample data into posts
INSERT INTO posts (id, user_id, title, content, published_at) VALUES
(1, 1, 'First Post', 'This is my first post content', TIMESTAMP '2024-01-01 10:00:00'),
(2, 1, 'Second Post', 'Another interesting post', TIMESTAMP '2024-01-02 15:30:00'),
(3, 2, 'Hello World', 'Jane''s first blog post', TIMESTAMP '2024-01-03 09:15:00');

-- Sample SELECT queries
-- Basic select
SELECT * FROM users;

-- Join example
SELECT u.username, p.title, p.published_at
FROM users u
JOIN posts p ON u.id = p.user_id
ORDER BY p.published_at DESC;

-- Aggregation example
SELECT u.username, COUNT(p.id) as post_count
FROM users u
LEFT JOIN posts p ON u.id = p.user_id
GROUP BY u.username;

-- Update example
UPDATE posts
SET content = 'Updated content'
WHERE id = 1;

-- Delete example
DELETE FROM posts WHERE id = 3;

-- Clean up (commented out for safety)
-- DROP TABLE posts;
-- DROP TABLE users; 