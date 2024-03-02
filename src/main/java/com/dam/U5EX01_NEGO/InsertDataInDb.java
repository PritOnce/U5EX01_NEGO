package com.dam.U5EX01_NEGO;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.*;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Properties;


public class InsertDataInDb {
    public static void main(String[] args) {
        /*
        El main la unica funcion que le doy en la conexion con MongoDB
        He tenido que buscar como hacerla porque mediante los ejemplos proporcionados no era suficiente
         */
        String uri = "mongodb://localhost:27017"; //Como en con MySQL le damos la direccion donde se conectara
        String path = obtenerPath(); //Mediante obtenerPath obtenemos la direccion del archivo, la cual está guardada dentro de un properties

        //Realizamos la conexion
        ConnectionString connectionString = new ConnectionString(uri);

        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connectionString).build();
        //Cargamos el cliente
        MongoClient mongoClient = MongoClients.create(settings);

        //Indicamos que base de datos usara junto a que coleccion usara
        MongoDatabase database = mongoClient.getDatabase("storeVCS");
        MongoCollection<Document> collection = database.getCollection("salesVCS");

        migrarData(path, collection);
        mongoClient.close();// Cerramos la conexion
    }

    //Metodo para insertar los datos del .csv
    public static void migrarData(String path, MongoCollection<Document> collection){
        // Leemos el archivo
        try(BufferedReader bfr = new BufferedReader(new FileReader(path))){
            String linia;
            // Salto la primera linea ya que unicamente ocupa el nombre de los campos
            bfr.readLine();

            //Ahora por cada linea que haya la leemos
            while ( (linia = bfr.readLine()) != null ){
                //Al csv venir los datos separados por comar hacemos un array por cada campo el cual detectara cada vez que haya una coma
                String[] data = linia.split(",");

                //Creamos el documento y le damos los datos que tendra con sus tipos correspondientemente
                Document document = new Document().append("item", data[0]).
                        append("email", data[1]).
                        append("telephone", data[2]).
                        append("address", data[3]).
                        append("price", Integer.parseInt(data[4])).
                        append("quantity", Integer.parseInt(data[5])).
                        append("date", LocalDate.parse(data[6], DateTimeFormatter.ofPattern("dd/MM/yyyy")));
                //Ahora como lee por lineas pues solo es un documento, asi que es solo un insertOnce
                collection.insertOne(document);
            }
            System.out.println("Migracion exitosa");
        }catch (IOException ioe){
            System.out.println(ioe.getMessage());
        }
    }

    // Metodo par aobtener el path
    public static String obtenerPath(){
        //Inicializamos un objeto properties
        Properties propFile = new Properties();
        try{
            //cargamos el contenido
            InputStream is = InsertDataInDb.class.getClassLoader().getResourceAsStream("application.properties");
            propFile.load(is);
            // Devolvemos un string con el conteido del campo file.path
            return propFile.getProperty("file.path");
        }catch (IOException ioe){
            ioe.printStackTrace();
            return null;
        }
    }
}
