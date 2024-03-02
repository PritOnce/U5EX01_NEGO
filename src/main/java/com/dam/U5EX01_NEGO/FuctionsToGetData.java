package com.dam.U5EX01_NEGO;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.*;
import org.bson.Document;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

import static com.mongodb.client.model.Filters.*;

public class FuctionsToGetData {

    public static void main(String[] args) {
        /*
        Lo mismo que el anterior main
         */
        String uri = "mongodb://localhost:27017";

        ConnectionString connectionString = new ConnectionString(uri);

        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connectionString).build();

        MongoClient mongoClient = MongoClients.create(settings);

        MongoDatabase database = mongoClient.getDatabase("storeVCS");
        MongoCollection<Document> collection = database.getCollection("salesVCS");

        System.out.println("Conexion establecida");

        //Aqui mostramos lo que nos devuelve nuestros metodos
        double totalVentasEnero = obtenerTotalVentasMes(collection);
        System.out.println("El importe total de las ventas en enero es: " + totalVentasEnero); //Apartado 4


        Map<String, Integer> ventasPorArticulo = obtenerUnidadesVendidasPorArticulo(collection);// Apartado 5
        for (Map.Entry<String, Integer> entry : ventasPorArticulo.entrySet()) {
            System.out.println(entry.getKey() + " → " + entry.getValue());
        }

        mongoClient.close();
    }

    //Apartado 4, le pasamos la coleccion para no tener que crear todo el tiempo una
    public static double obtenerTotalVentasMes(MongoCollection<Document> collection) {
        double gananciasEnero = 0.0;//Variable para obtener el resultado

        //Creamos las variables para usarlas en el filtro de busqueda teniendo en cuenta el formato de insercion
        LocalDate inicioEnero = LocalDate.of(2022, 1, 1);
        LocalDate finEnero = LocalDate.of(2022, 1, 31);

        //Recogemos los documentos que mantengan nuestras condiciones
        for (Document venta : collection.find(and(gte("date", inicioEnero), lt ("date", finEnero)))) {
            //Y unicamente recogemos el precio y la cantidad para hacer la cuenta
            int precio = venta.getInteger("price");
            int cantidad = venta.getInteger("quantity");
            gananciasEnero += precio * cantidad;
        }

        return gananciasEnero;// Devolvemos el resultado
    }

    //Creamos el metodo del tipo MAP para devolver varios resultados en uno mismo
    public static Map<String, Integer> obtenerUnidadesVendidasPorArticulo(MongoCollection<Document> collection) {
        //Creamos la variable para obtener tanto el nombre como la cantidad, al ser del tipo String e Integer relacionaremos el objeto por su cantidad vendida
        Map<String, Integer> ventasPorArticulo = new HashMap<>();

        // Recorremos lo obtenido y lo insertamos en el MAP
        for (Document venta : collection.find()) {
            String articulo = venta.getString("item");
            int cantidad = venta.getInteger("quantity");

            // Actualizar el mapa con la cantidad vendida del articulo
            ventasPorArticulo.put(articulo, ventasPorArticulo.getOrDefault(articulo, 0) + cantidad);
        }

        return ventasPorArticulo;// Devolvemos todo para mostrarlo por pantalla
    }
}
