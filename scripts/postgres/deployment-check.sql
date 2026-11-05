-- Disposable deployment fixture only. Never applied to the application databases.
CREATE TABLE deployment_effects (
    request_id TEXT PRIMARY KEY,
    first_replica TEXT NOT NULL,
    effects INTEGER NOT NULL DEFAULT 1 CHECK (effects = 1),
    attempts INTEGER NOT NULL DEFAULT 1
);
