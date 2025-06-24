CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    amount DECIMAL(50,2) NOT NULL,
    card_mask CHAR(4) NOT NULL,
    status VARCHAR(20) CHECK (status IN ('pending', 'completed', 'declined')),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_transactions_user_id ON transactions(user_id);