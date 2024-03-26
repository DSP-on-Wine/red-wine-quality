
import psycopg2

def isnotnull(id,item):
 
    connection = psycopg2.connect(
        user="postgres",
        password="dsp123456",
        host="localhost",
        port="5432",
        database="red_wine_data"
    )
    cursor = connection.cursor()
    if item =='':
        sql='''
             insert into error (
             wine_id,
             error) values
             ('''+str(id)+','+item +'is null)'
        cursor.execute(sql)
        connection.commit()
        cursor.close()
        connection.close()
        #print(id,item)
        return False
    else:
        cursor.close()
        connection.close()
        #print(id,item)
        return True
    
       

        
 