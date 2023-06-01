#--------------Código para mover fotos del aula virtual PAR al bucket --------------
from google.cloud import storage
import urllib.request
import psycopg2
import datetime
import configparser
#Conexion a la BD
config = configparser.ConfigParser()
config.read('config/config.ini')

connection = psycopg2.connect(
        host=config['postgresql']['host'],
        database=config['postgresql']['database'],
        user=config['postgresql']['user'],
        password=config['postgresql']['password']
    )
cursor = connection.cursor()

# Credenciales de acceso de Google Cloud Storage
credential_path = 'config/credentials.json'
client = storage.Client.from_service_account_json(credential_path)

def send_to_bucket(id, cedula, foto, quiz_id_key):
    try:
        # Nombre del bucket y archivo que se subirá
        blob_name = str(cedula) +'_q' + str(quiz_id_key) + '_' + str(id) + '.png'
        bucket_name = 'processed_photo_resources'
        urllib.request.urlretrieve(foto, 'tmp/imagentmp.jpg')

        # Obtener la instancia del bucket
        bucket = client.get_bucket(bucket_name)

        blob = bucket.blob(cedula + '/')
        if not blob.exists():
            print(blob.exists())
            blob = bucket.blob(cedula + '/')
            blob.upload_from_string('')

        # Subir el archivo
        blob = bucket.blob(cedula+"/"+blob_name)
        blob.upload_from_filename('tmp/imagentmp.jpg')

        query = "UPDATE sen_quizaccess_proctoring_logs SET awsscore = 2 WHERE id=" + str(id)
        cursor.execute(query)
        connection.commit()

        print(query)
        print(datetime.datetime.now())
        return 1
    except:
        print("Ha ocurrido un error")
        return 0

def getPhotos():
    #TODO: agregar el campo quizidkey en la subquery en el select, en el goup by y en el on del join
    cursor.execute("select distinct userid, quizid from sen_quizaccess_proctoring_logs where  webcampicture<>'' and quizid in (1145,970,1091,1108,1128,1226,1136,1113,1123,971) and awsscore=0")
    results = cursor.fetchall()
    for r in results:
        cursor.execute("select sqpl.id,username,webcampicture,sqpl.timemodified from sen_quizaccess_proctoring_logs sqpl left join sen_user su on (sqpl.userid=su.id) where webcampicture<>'' and userid="+str(r[0])+" and quizid="+str(r[1])+" order by sqpl.timemodified desc limit 10")
        photos = cursor.fetchall()
        for p in photos:
            print(f"Id {str(p[0])}, Usuario: {str(p[1])}")
            send_to_bucket(p[0], p[1], p[2], r[1])
        # Actualizar en la bd
        query = "UPDATE sen_quizaccess_proctoring_logs SET awsscore = 1 WHERE userid=" + str(r[0])+" and quizid="+ str(r[1])+" and awsscore=0";
        cursor.execute(query)
        connection.commit()

if __name__ == '__main__':
    getPhotos()
    cursor.close()
    connection.close()
    # print(datetime.datetime.now())