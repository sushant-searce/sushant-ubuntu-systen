import cx_Oracle 
  
try: 
  
    con = cx_Oracle.connect('admin/password@suushant-test-cf-db.chbcar19iy5o.us-east-1.rds.amazonaws.com/DATABASE:1521') 
      
    # Now execute the sqlquery 
    cursor = con.cursor() 
    cursor.execute("select * from regions")

    # insert into regions values(4, 'sri')
      
    # commit that insert the provided data 
    con.commit() 
      
    print("value inserted successful") 
  
except cx_Oracle.DatabaseError as e: 
    print("There is a problem with Oracle", e) 
    
# then also we can close the all database operation 