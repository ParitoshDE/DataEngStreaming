CREATE TABLE IF NOT EXISTS hw7_q4_tumbling_pickup (
    window_start TIMESTAMP(3) NOT NULL,
    PULocationID INT NOT NULL,
    num_trips BIGINT,
    PRIMARY KEY (window_start, PULocationID)
);

CREATE TABLE IF NOT EXISTS hw7_q5_session_streak (
    window_start TIMESTAMP(3) NOT NULL,
    window_end TIMESTAMP(3) NOT NULL,
    PULocationID INT NOT NULL,
    num_trips BIGINT,
    PRIMARY KEY (window_start, window_end, PULocationID)
);

CREATE TABLE IF NOT EXISTS hw7_q6_tumbling_tip (
    window_start TIMESTAMP(3) NOT NULL PRIMARY KEY,
    total_tip_amount DOUBLE PRECISION
);
