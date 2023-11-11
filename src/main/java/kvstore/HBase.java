package kvstore;

public class HBase implements KVStore<String, String>{

    HFile hfile = new HFile();

    @Override
    public String get(String key) {
        return hfile.getKeyValue(key);
    }

    @Override
    public void put(String key, String value) {
        hfile.appendData(key, value, false);
    }

    @Override
    public void delete(String key) {
        hfile.delete(key);
    }

    public static void main(String[] args) {
        HBase hBase = new HBase();
//        hBase.put("1", "One2");
//        hBase.put("2", "Two2");
//        hBase.put("3", "Three2");
//        hBase.put("3", "Three3");
//        hBase.put("3", "Three4");

        System.out.print(hBase.get("1"));
        hBase.delete("1");
        System.out.print(hBase.get("1"));
    }

}
