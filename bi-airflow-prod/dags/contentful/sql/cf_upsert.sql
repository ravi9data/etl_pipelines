

delete from pricing.contentful_snapshots
using pricing.tmp_contentful_snapshots
where contentful_snapshots.snapshot_id = tmp_contentful_snapshots.snapshot_id;

INSERT
	INTO
	pricing.contentful_snapshots
SELECT
	extract_date::date,
	entry_id,
	content_type,
	snapshot_id,
	snapshot_type,
	environment,
	first_published_at,
	published_counter,
	published_version,
	published_at,
	published_by,
	default_locale,
	created_at,
	updated_at,
	fields
FROM
	pricing.tmp_contentful_snapshots;
