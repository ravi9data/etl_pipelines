INSERT INTO {schema_name}.{table_name} (customer_id,
                                        label,
                                        updated_at)
VALUES {parameters} ON CONFLICT (customer_id) DO
UPDATE SET label = EXCLUDED.label, updated_at = EXCLUDED.updated_at
