package client;

import com.google.gson.Gson;

import java.time.Duration;
import java.time.Instant;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import java.util.Map;
import java.util.SortedMap;

import oracle.kv.*;


public class NoSqlApp {

    private KVStore kvstore;

    public NoSqlApp(String[] hhosts) {
        super();
        System.out.println("Initiating store access!");

        KVStoreConfig kconfig = new KVStoreConfig("mystore", hhosts);
        kvstore = KVStoreFactory.getStore(kconfig);

        System.out.println("Got a handle of the store!");
    }

    public void initStore(List<People> data) {
        System.out.println("i. Cargar los datos del archivo a la base de datos en NoSQL.");
        //First make the first part, /LastName/FirstName/-/personal_info
        Gson gson = new Gson();
        for (People p : data) {
            ArrayList<String> majorComponents = new ArrayList<String>();
            ArrayList<String> minorComponents = new ArrayList<String>();
            majorComponents.add(p.getLastName());
            majorComponents.add(p.getFirstName());
            minorComponents.add("personal_info");
            Key myKey = Key.createKey(majorComponents, minorComponents);
            //TODO: This is lazy serialization here.
            String valueString = p.getPhoneNumber() + "," + p.getEmail();
            Value myValue = Value.createValue(valueString.getBytes());
            //We Make the Record
            kvstore.putIfAbsent(myKey, myValue);

            //We also want to build the index.
            // /index_gender/<gender>/LN/FN/
            ArrayList<String> indexMajors = new ArrayList<String>();
            ArrayList<String> indexMinors = new ArrayList<String>();
            indexMajors.add("index_gender");
            indexMajors.add(p.getGender());
            indexMinors.add(p.getLastName());
            indexMinors.add(p.getFirstName());
            Key indexKey = Key.createKey(indexMajors, indexMinors);
            Value empty = Value.EMPTY_VALUE;
            kvstore.putIfAbsent(indexKey, empty);
        }

        System.out.println("Store loaded with csv values...");


    }

    public void findByName(String firstName, String lastName) {
        System.out.println("ii. Consultar a una persona dada por apellido y nombre");
        ArrayList<String> majorComponents = new ArrayList<String>();
        ArrayList<String> minorComponents = new ArrayList<String>();
        majorComponents.add(lastName);
        majorComponents.add(firstName);
        minorComponents.add("personal_info");
        // Create the key
        Key myKey = Key.createKey(majorComponents, minorComponents);
        ValueVersion vv = kvstore.get(myKey);
        if (vv != null) {
            Value v = vv.getValue();
            System.out.println("Obtener el teléfono y el correo electrónico de una persona.");
            String data = new String(v.getValue());
            System.out.println("Found this info for the first name last name : " + data);

        }
    }

    public void findAllWomenData() {
        System.out.println("Obtener el nombre, teléfono y correo electrónico de todas las mujeres");
        ArrayList<String> majorComponents = new ArrayList<String>();
        majorComponents.add("index_gender");
        //F = female
        majorComponents.add("F");
        // Create the key
        Key myKey = Key.createKey(majorComponents);
        SortedMap<Key, ValueVersion> myRecords = null;
        try {
            myRecords = kvstore.multiGet(myKey, null, null);
        } catch (ConsistencyException ce) {
            // The consistency guarantee was not met
        } catch (RequestTimeoutException re) {
            // The operation was not completed within the
            // timeout value
        }

        //Now we should have all women
        for (Map.Entry<Key, ValueVersion> entry : myRecords.entrySet()) {
            String lastName = entry.getKey()
                                   .getMinorPath()
                                   .get(0);
            String firstName = entry.getKey()
                                    .getMinorPath()
                                    .get(1);
            System.out.println(firstName + "," + lastName);


            //Now we have the index, we can add the second query
            ArrayList<String> personMajor = new ArrayList<String>();
            ArrayList<String> personMinor = new ArrayList<String>();
            personMajor.add(lastName);
            personMajor.add(firstName);
            personMinor.add("personal_info");
            // Create the key
            Key womanKey = Key.createKey(personMajor, personMinor);
            ValueVersion vv2 = kvstore.get(womanKey);
            if (vv2 != null) {
                Value v = vv2.getValue();
                String data = new String(v.getValue());
                System.out.println(data);

            }
        }
    }

    public void findAllByLastName(String lastName) {
        System.out.println("Consultar todos los registros con un mismo apellido." + lastName);
        ArrayList<String> majorComponents = new ArrayList<String>();
        majorComponents.add(lastName);
        Key myKey = Key.createKey(majorComponents);
        Iterator<KeyValueVersion> i = kvstore.storeIterator(Direction.UNORDERED, 0, myKey, null, null);
        while (i.hasNext()) {
            KeyValueVersion kvv = i.next();
            Value v = kvv.getValue();
            Key k = kvv.getKey();

            System.out.println("Found: " + k.toString() + new String(v.getValue()));
        }
    }

    public void deleteAllByLastName(String lastName) {
        System.out.println("Eliminar todos los registros con un mismo apellido." + lastName);
        ArrayList<String> majorComponents = new ArrayList<String>();
        majorComponents.add(lastName);
        Key myKey = Key.createKey(majorComponents);
        Iterator<KeyValueVersion> i = kvstore.storeIterator(Direction.UNORDERED, 0, myKey, null, null);
        while (i.hasNext()) {
            KeyValueVersion kvv = i.next();
            Key k = kvv.getKey();
            //We found the record, now we delete all with same last name
            ArrayList<String> majorToDelete = new ArrayList<String>();
            majorToDelete.add(lastName);
            majorToDelete.add(k.getMajorPath().get(1));
            Key deleteKey = Key.createKey(majorToDelete);
            kvstore.multiDelete(deleteKey, null, null);

        }

        System.out.println("Registros eliminados");
    }

    public void deleteAll() {
        System.out.println("Eliminar todos los registros .");

        final StoreIteratorConfig sc = new StoreIteratorConfig().setMaxConcurrentRequests(2);
        ParallelScanIterator<KeyValueVersion> iter =
            kvstore.storeIterator(Direction.UNORDERED, 0, null /* parentKey */, null /* subRange */, null /* Depth */,
                                  Consistency.NONE_REQUIRED, 0 /* timeout */, null /* timeoutUnit */, sc);
        try {
            while (iter.hasNext()) {
                KeyValueVersion kvv = iter.next();
                kvstore.delete(kvv.getKey());
            }
        } finally {
            if (iter != null) {
                iter.close();
            }
        }

        System.out.println("Registros eliminados");
    }

    /**
     * Inserts a number of records using store put method
     * */
    public void manualInsert(long records) {

        for (int i = 1; i < records; i++) {
            ArrayList<String> majorComponents = new ArrayList<String>();
            majorComponents.add("list");
            majorComponents.add(Integer.toString(i));
            Key myKey = Key.createKey(majorComponents);
            String valueString = NumberToWords.convert(i);
            Value myValue = Value.createValue(valueString.getBytes());
            //We Make the Record
            kvstore.put(myKey, myValue);
        }
    }

    public void clearList() {
        System.out.println("Eliminar la lista de numberos.");
        ArrayList<String> majorComponents = new ArrayList<String>();
        majorComponents.add("list");
        Key myKey = Key.createKey(majorComponents);

        Iterator<KeyValueVersion> i = kvstore.storeIterator(Direction.UNORDERED, 0, myKey, null, null);
        while (i.hasNext()) {
            KeyValueVersion kvv = i.next();
            Key k = kvv.getKey();
            //We found the record, now we delete all with same last name
            ArrayList<String> majorToDelete = new ArrayList<String>();
            majorToDelete.add("list");
            majorToDelete.add(k.getMajorPath().get(1));
            Key deleteKey = Key.createKey(majorToDelete);
            kvstore.multiDelete(deleteKey, null, null);
        }

    }

    public void printListHead(long count) {

        for (int i = 1; i <= count; i++) {
            ArrayList<String> majorComponents = new ArrayList<String>();
            majorComponents.add("list");
            majorComponents.add(Integer.toString(i));
            // Create the key
            Key myKey = Key.createKey(majorComponents);
            ValueVersion vv = kvstore.get(myKey);
            if (vv != null) {
                Value v = vv.getValue();
                String data = new String(v.getValue());
                System.out.println(data);

            }
        }


    }

    /**
     * Inserts a number of record in bulk
     * */
    public void bulkInsert(int records) {
        Integer streamParallelism = 2;
        Integer perShardParallelism = 2;
        Integer heapPercent = 30;

        BulkWriteOptions bulkWriteOptions = new BulkWriteOptions(null, 0, null);
        // Set the number of streams. The default is 1 stream.
        bulkWriteOptions.setStreamParallelism(streamParallelism);
        // Set the number of writer threads per shard.
        // The default is 3 writer threads.
        bulkWriteOptions.setPerShardParallelism(perShardParallelism);
        // Set the percentage of max memory used for bulk put.
        // The default is 40 percent.
        bulkWriteOptions.setBulkHeapPercent(heapPercent);
        final List<EntryStream<KeyValue>> streams = new ArrayList<EntryStream<KeyValue>>(streamParallelism);
        final int num = (records + (streamParallelism - 1)) / streamParallelism;
        for (int i = 0; i < streamParallelism; i++) {
            final int min = num * i;
            final int max = Math.min((min + num), records);
            streams.add(new LoadKVStream("Stream" + i, i, min, max));
        }
        kvstore.put(streams, bulkWriteOptions);
        long total = 0;
        long keyExists = 0;
        for (EntryStream<KeyValue> stream : streams) {
            total += ((LoadKVStream) stream).getCount();
            keyExists += ((LoadKVStream) stream).getKeyExistsCount();
        }
        final String fmt = "Loaded %,d records, %,d pre-existing.";
        System.err.println(String.format(fmt, total, keyExists));
    }

    public static void main(String[] args) throws InterruptedException {
        //10.0.0.12:8080, 10.0.0.203:8080
        List<String> hostsArray = new ArrayList<String>();
        String fileName = "/home/opc/data/people.csv";
        //Get the csv data
        System.out.println("Getting data from CSV!");
        List<People> result = CSVReader.read(fileName);
        System.out.println("Got " + result.size() + "results!");
        hostsArray.add(args[0]);
        hostsArray.add(args[1]);

        NoSqlApp noSqlApp = new NoSqlApp(hostsArray.toArray(new String[0]));

        System.out.println("----------------------------");
        System.out.println("Parte 1. Datos desde CSV");
        System.out.println("----------------------------");

        noSqlApp.initStore(result);

        //TODO: Probably ask for user input here.
        noSqlApp.findByName("Marcela", "Chaves");

        noSqlApp.findAllWomenData();

        noSqlApp.findAllByLastName("Chaves");

        noSqlApp.deleteAllByLastName("Chaves");

        //Just to confirm that it was actually deleted
        noSqlApp.findAllByLastName("Chaves");

        //Algun otro apellido
        noSqlApp.findAllByLastName("Uzaga");

        noSqlApp.deleteAll();

        //Algun otro apellido
        noSqlApp.findAllByLastName("Uzaga");

        System.out.println("----------------------------");
        System.out.println("Parte 2. Insercion manual vs Bulk");
        System.out.println("----------------------------");

        Instant start = Instant.now();
        noSqlApp.manualInsert(25000);
        Instant end = Instant.now();
        System.out.println("Manual insert took: " + Duration.between(start, end));

        System.out.println("Show some of the list records: " + Duration.between(start, end));
        noSqlApp.printListHead(10);

        noSqlApp.clearList();
        System.out.println("Show some of the list records after clean");
        noSqlApp.printListHead(10);

        start = Instant.now();
        noSqlApp.bulkInsert(25000);
        end = Instant.now();
        System.out.println("Bulk insert took: " + Duration.between(start, end));
        
        System.out.println("Show some of the list records");
        noSqlApp.printListHead(10);

        noSqlApp.clearList();
        System.out.println("Show some of the list records after clean");
        noSqlApp.printListHead(10);


    }
}
