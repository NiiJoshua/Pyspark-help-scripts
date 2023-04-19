#  Alter tables

ALTER TABLE silver_users
ADD COLUMN (
    attributes ARRAY<STRING>
);

DELETE FROM silver_users
WHERE project_id  = 'test'
AND attributes IS NOT NULL