# If input multiple primary key columns 
src_keys = 'routine_schedule_id, sh_schedule_item'

# If input single
# src_keys = 'routine_schedule_id'
# src_keys = 'id'

print(src_keys)

main_prefix = 'main'
src_table = 'source_tablename'
temp_prefix = 'temp'
dest_table = 'destination_tablename'

# This is the logic to construct the SQL
src_keys_as_list = src_keys.split(',')
compare_clause = list()
for i, key in enumerate(src_keys_as_list):
    clause = '\n\tsrc.{key} = dest.{key}'.format(key=key)
    compare_clause.append(clause)

sql_statement = 'insert into something;\n'

sql_statement_del = """
DELETE FROM {main_prefix}.{src_table} dest
WHERE EXISTS 
(
    SELECT 1
    FROM {temp_prefix}.{dest_table} src
    WHERE {compare_clause}
);
""".format(main_prefix=main_prefix,
           src_table=src_table,
           src_keys=src_keys,
           temp_prefix=temp_prefix,
           dest_table=dest_table,
           compare_clause=' AND'.join(compare_clause)
           )

# Returned DELETE statement

sql_statement_insert = "insert into callplan_prod_temp with cte as (select * , ROW_NUMBER() over (partition by "+src_keys+" order by __ts_ms desc) as \"rnum\" from callplan_prod_cdc) select * from cte where rnum=1;"
sql_statement_delete = "DELETE FROM callplan_prod.routine_schedule_item dest WHERE EXISTS ( SELECT 1 FROM callplan_prod_temp.routine_schedule_item src WHERE src.routine_schedule_id = dest.routine_schedule_id AND src.sh_schedule_item = dest.sh_schedule_item);"
sql_statement_main_insert = 'insert into callplan_prod.routine_schedule_item with cte as (select "routine_schedule_id", "sh_schedule_item", "dec_value", "created_at", "updated_at" from callplan_prod_tmep.routine_schedule_item where __deleted=\'false\')select "routine_schedule_id", "sh_schedule_item", "dec_value", "created_at", "updated_at" from cte;'

print(sql_statement_insert)
print(sql_statement_delete)
print(sql_statement_main_insert)


sql_statement += sql_statement_del
print(sql_statement)