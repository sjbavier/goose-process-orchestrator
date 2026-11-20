-- Forward-only: add uploaded file-parameter contents to the durable task audit record.
-- Never edit an applied migration.
-- The empty string means no file parameters, preserving legacy-row equivalence.
-- Contents are unencrypted local audit data; never pass secrets.
ALTER TABLE tasks ADD COLUMN file_parameters TEXT NOT NULL DEFAULT '';
