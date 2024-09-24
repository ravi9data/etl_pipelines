BEGIN;

  DROP TABLE IF EXISTS web.users;
  ALTER TABLE web.users_tmp RENAME TO users;
  GRANT SELECT ON web.users TO redash_growth;

COMMIT;