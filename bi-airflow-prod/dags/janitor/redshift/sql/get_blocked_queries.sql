SELECT DISTINCT pid,username,objectname,block_min,waiting, max_sec_blocking,num_blocking,pidlist
FROM ADMIN.v_get_blocking_locks vgbl
WHERE block_min >20 -- minutes
OR waiting >500 -- seconds
