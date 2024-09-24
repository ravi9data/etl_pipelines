INSERT INTO order_approval.customer_labels_state_changes3(customer_id, label_state, is_blacklisted, label_data, previous_state_id)
    ( SELECT labels.customer_id,
             labels."label",
             labels.is_blacklisted,
             row_to_json(labels),
             labels_states.state_id AS previous_state_id
     FROM order_approval.customer_labels3 labels
     LEFT JOIN
         ( SELECT clsc.state_id,
                  clsc.customer_id,
                  clsc.label_state,
                  clsc.is_blacklisted,
                  row_number () over (partition BY customer_id
                                      ORDER BY created_at DESC) AS pos
          FROM order_approval.customer_labels_state_changes3 clsc ) AS labels_states ON labels.customer_id = labels_states.customer_id
     AND labels_states.pos=1
     WHERE ( labels.label IS DISTINCT
            FROM labels_states.label_state
            OR labels.is_blacklisted IS DISTINCT
            FROM labels_states.is_blacklisted ) );
