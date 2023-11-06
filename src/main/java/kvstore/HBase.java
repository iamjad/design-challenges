package kvstore;

public class HBase implements KVStore<String, String>{

    HFile hfile = new HFile();

    @Override
    public String get(String key) {
        return hfile.getKeyValue(key);
    }

    @Override
    public void put(String key, String value) {
        hfile.appendData(key, value);
    }

    @Override
    public void delete(String key) {

    }

    public static void main(String[] args) {
        HBase hBase = new HBase();
        hBase.put("1", "One1");
        hBase.put("2", "Two1");
        hBase.put("3", "Three1");

        System.out.print(hBase.get("3"));
    }

}
