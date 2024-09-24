WITH running_pids AS (
	SELECT distinct a.pid,
		a.txn_owner::CHARACTER VARYING AS user_name,
		'Idle Trx' AS ptype,
		date_diff(
			'minute'::CHARACTER VARYING::text,
			a.txn_start,
			getdate()
		) AS running_time
	FROM svv_transactions a
	WHERE a.relation IS NOT NULL
	UNION
	SELECT stv_recents.pid,
		stv_recents.user_name,
		'Running Query' AS ptype,
		date_diff('minute', stv_recents.starttime, getdate()) AS running_time
	FROM stv_recents
	WHERE stv_recents.status = 'Running'::bpchar
),
user_execution_time_threshold AS (
	SELECT x.user_name,
		x.execution_time
	FROM (
			SELECT user_per_groups.user_name,
				min(user_per_groups.execution_time) AS execution_time
			FROM (
					SELECT p.user_name,
						COALESCE(et.group_name, et2.group_name) AS group_name,
						COALESCE(et.execution_time_min, et2.execution_time_min) AS execution_time
					FROM bi_audit_schema.user_group p
						LEFT JOIN bi_audit_schema.cfg_mapping_group_execution_time et ON p.group_name::text = COALESCE(et.group_name, 'unknown')::text
						LEFT JOIN bi_audit_schema.cfg_mapping_group_execution_time et2 ON 'unknown'::text = et2.group_name::text
					UNION ALL
					SELECT ug.user_name,
						ug.group_name,
						mp.execution_time_min
					FROM bi_audit_schema.cfg_mapping_group_execution_time mp
						LEFT JOIN bi_audit_schema.user_group ug ON mp.group_name::text = ug.user_name::text
					WHERE mp.type_user_group::text = 'user'::text
				) user_per_groups
			GROUP BY user_per_groups.user_name
		) x
)
SELECT rp.pid,
	rp.ptype,
	uet.user_name,
	rp.running_time,
	uet.execution_time
FROM running_pids rp
	LEFT JOIN user_execution_time_threshold uet ON rp.user_name::text = uet.user_name::text
WHERE rp.running_time > uet.execution_time
	AND rp.user_name not in (
		'ddl_manager',
		'tableau',
		'ceyda_peker',
		'erkin_unler',
		'glenda_vieira',
		'ishwariya_raveendran',
		'nasim_pezeshki',
		'pawan_pai',
		'saad_amir',
		'sahin_top',
		'daniele_piubello',
		'slobodan_ilic',
		'thomas_battaglia',
		'radwa_hosny',
		'yasemin_senlik',
		'marcel_weber',
		'dmitrii_kharlamov_admin'
	);
