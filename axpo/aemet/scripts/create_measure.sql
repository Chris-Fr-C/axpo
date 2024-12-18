CREATE TABLE
  IF NOT EXISTS Measure (
    identifier TEXT,
    ts TIMESTAMP,
    temperature FLOAT,
    pressure FLOAT,
    velocity FLOAT,
    PRIMARY KEY (identifier, ts)
    -- TODO: add the foreign key constraint
  );

