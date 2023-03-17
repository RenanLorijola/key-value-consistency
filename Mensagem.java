public class Mensagem {
    private String key;
    private String value;
    private String ipClient;
    private Integer portaClient;
    private String tipo;
    private Integer timestamp;

    public Mensagem(String key, String value, Integer timestamp) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.tipo = "PUT";
    }

    public Mensagem(String key, Integer timestamp) {
        this.key = key;
        this.timestamp = timestamp;
        this.tipo = "GET";
    }

    public Mensagem(String key, String value, Integer timestamp, String tipo) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.tipo = tipo;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public String getTipo() {
        return tipo;
    }

    public Integer getTimestamp() {
        return timestamp;
    }

    public String getIpClient() {
        return ipClient;
    }

    public void setIpClient(String ipClient) {
        this.ipClient = ipClient;
    }

    public Integer getPortaClient() {
        return portaClient;
    }

    public void setPortaClient(Integer portaClient) {
        this.portaClient = portaClient;
    }
}
