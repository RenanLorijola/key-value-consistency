import com.google.gson.Gson;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Servidor {

    static final private Scanner scanner = new Scanner(System.in);
    static final private Integer qtdServidores = 3;
    static final private Integer qtdSubordinados = qtdServidores - 1;
    static final private ConcurrentHashMap<String, String> keyValueTable = new ConcurrentHashMap<>();
    static final private ConcurrentHashMap<String, Integer> keyTimestampTable = new ConcurrentHashMap<>();
    static final private ConcurrentHashMap<String, Integer> keyPutReplicationOkIndexes = new ConcurrentHashMap();
    static final private String[] ips = new String[2];
    static final private Integer[] portas = new Integer[2];
    static private Boolean isLeader = false;
    static private String ip;
    static private Integer porta;
    static private String ipLeader;
    static private Integer portaLeader;

    public static void main(String[] args) throws Exception {
        //5.a) Inicializa o servidor, levando em conta os servidores default caso o servidor seja o lider
        // e o usuário não deseje inserir o ip e porta deles
        System.out.println("----------------------------------------------------------------------");
        System.out.println("Inicialização do servidor");
        System.out.println("----------------------------------------------------------------------");

        System.out.println("Digite o ip:porta desse servidor, caso vá utilizar os valores default, escolha um dos valores default para informar:");
        String input = scanner.nextLine();
        String[] ipPorta = input.split(":");
        ip = ipPorta[0];
        porta = Integer.parseInt(ipPorta[1]);

        System.out.println("Digite o ip:porta do servidor líder:");
        input = scanner.nextLine();
        ipPorta = input.split(":");
        ipLeader = ipPorta[0];
        portaLeader = Integer.parseInt(ipPorta[1]);

        isLeader = ip.equals(ipLeader) && porta.equals(portaLeader);

        if(isLeader){
            System.out.println("----------------------------------------------------------------------");
            System.out.println("Como esse é o servidor líder, insira o ip:porta dos outros " + qtdSubordinados + " servidores, um por linha");
            System.out.println("Caso queira usar os valores default, basta apertar enter sem informar nada");
            for(int i = 0; i < qtdSubordinados; i ++){
                input = scanner.nextLine();

                //armazena os valores default caso o usuário não insira valor algum.
                if(input.equals("")){
                    ips[0] = "127.0.0.1";
                    ips[1] = ips[0];
                    ArrayList<Integer> portasDefaultList = new ArrayList<>();
                    portasDefaultList.addAll(Arrays.asList(10097, 10098, 10099));
                    portasDefaultList.remove(porta);
                    portas[0] = portasDefaultList.get(0);
                    portas[1] = portasDefaultList.get(1);
                    break;
                }

                //insere os valores informados pelo usuário
                ipPorta = input.split(":");
                ips[i] = ipPorta[0];
                portas[i] = Integer.parseInt(ipPorta[1]);
            }
        }

        System.out.println("\nServidor inicializado!\n");

        ServerSocket serverSocket = new ServerSocket(porta);

        while (true) {
            Socket no = serverSocket.accept();

            //5.b) Recebe simultaneamente requisições de multiplos clientes, para cada requisição é criada uma thread
            // responsavel por processar a mensagem e responder o cliente8
            new ThreadResposta(no).start();
        }
    }

    //Thread responsavel por processar todos os sockets abertos pelo server socket, enviar novas requisições e responder outros sockets quando necessário
    public static class ThreadResposta extends Thread{

        private Socket socketRequest;

        public ThreadResposta(Socket socket){
            this.socketRequest = socket;
        }

        @Override
        public void run() {
            try{
                Gson gson = new Gson();

                InputStreamReader is = new InputStreamReader(socketRequest.getInputStream());
                BufferedReader reader = new BufferedReader(is);

                String requestString = reader.readLine();
                Mensagem mensagemRequest = gson.fromJson(requestString, Mensagem.class);

                socketRequest.close();

                switch (mensagemRequest.getTipo()) {
                    case "GET":
                        String valueGet = keyValueTable.get(mensagemRequest.getKey());
                        this.sleep(50); //Tempo necessário para aguardar a abertura de um server socket pelo cliente antes do envio da msg
                        Socket socketResponseGet = new Socket(mensagemRequest.getIpClient(), mensagemRequest.getPortaClient());

                        OutputStream osGet = socketResponseGet.getOutputStream();
                        DataOutputStream writerGet = new DataOutputStream(osGet);

                        //Tratativa se o valor não existir para devolver um erro de NOT FOUND
                        if (valueGet == null) {
                            //Apresenta as informações solicitadas no topico 6.
                            System.out.println("Cliente " + mensagemRequest.getIpClient() + ":" + mensagemRequest.getPortaClient() + " key: " + mensagemRequest.getKey() + " ts: " + mensagemRequest.getTimestamp() + ". Meu ts é " + keyTimestampTable.get(mensagemRequest.getKey()) + ", portanto devolvendo NOT_FOUND_ERROR");

                            Mensagem responseGet = new Mensagem(mensagemRequest.getKey(), null, null, "NOT_FOUND_ERROR");
                            writerGet.writeBytes(gson.toJson(responseGet) + "\n");
                            break;
                        }

                        //5.f) Não deve devolver o valor caso o timestamp armazenado seja menor do que o solicitado pelo cliente, devolvendo uma mensagem de erro TRY_OTHER_SERVER_OR_LATER
                        if(keyTimestampTable.get(mensagemRequest.getKey()) < mensagemRequest.getTimestamp()){
                            //Apresenta as informações solicitadas no topico 6.
                            System.out.println("Cliente " + mensagemRequest.getIpClient() + ":" + mensagemRequest.getPortaClient() + " key: " + mensagemRequest.getKey() + " ts: " + mensagemRequest.getTimestamp() + ". Meu ts é " + keyTimestampTable.get(mensagemRequest.getKey()) + ", portanto devolvendo TRY_OTHER_SERVER_OR_LATER");

                            Mensagem responseGet = new Mensagem(mensagemRequest.getKey(), null, keyTimestampTable.get(mensagemRequest.getKey()), "TRY_OTHER_SERVER_OR_LATER");
                            writerGet.writeBytes(gson.toJson(responseGet) + "\n");
                            break;
                        }

                        //Apresenta as informações solicitadas no topico 6.
                        System.out.println("Cliente " + mensagemRequest.getIpClient() + ":" + mensagemRequest.getPortaClient() + " key: " + mensagemRequest.getKey() + " ts: " + mensagemRequest.getTimestamp() + ". Meu ts é " + keyTimestampTable.get(mensagemRequest.getKey()) + ", portanto devolvendo " + valueGet);

                        Mensagem responseGet = new Mensagem(mensagemRequest.getKey(), valueGet, keyTimestampTable.get(mensagemRequest.getKey()), "GET");
                        writerGet.writeBytes(gson.toJson(responseGet) + "\n");

                        socketResponseGet.close();
                        break;
                    //5.c) Recebe a requisição put e verifica se é lider ou não para salvar e replicar ou encaminhar para o lider
                    case "PUT":
                        // Realiza o put em sua tabela e faz a replicação para todos os outros servidores caso seja o líder
                        if (isLeader) {
                            //Apresenta as informações solicitadas no topico 6.
                            System.out.println("Cliente " + mensagemRequest.getIpClient() + ":" + mensagemRequest.getPortaClient() + " PUT key: " + mensagemRequest.getKey() + " value: " + mensagemRequest.getValue());

                            //Armazena o valor do put em sua tabela
                            keyValueTable.put(mensagemRequest.getKey(), mensagemRequest.getValue());

                            Integer timestampArmazenado = keyTimestampTable.get(mensagemRequest.getKey()) == null ? 0 : keyTimestampTable.get(mensagemRequest.getKey());
                            Integer timestamp = Integer.max(timestampArmazenado, mensagemRequest.getTimestamp()) + 1;
                            keyTimestampTable.put(mensagemRequest.getKey(), timestamp);

                            //Cria uma mensagem do tipo replication
                            Mensagem mensagemReplication = new Mensagem(mensagemRequest.getKey(), mensagemRequest.getValue(), timestamp, "REPLICATION");

                            //Inicializa o contador de replication_ok recebidos
                            keyPutReplicationOkIndexes.put(mensagemRequest.getKey(), 0);

                            //cria varias threads para o envio das mensagens em paralelo para outros servidores
                            for(int i = 0; i < qtdSubordinados; i++){
                                ThreadReplication threadReplication = new ThreadReplication(i, mensagemReplication);
                                threadReplication.start();
                            }

                            //5.e) aguarda as threads de replicação finalizarem o recebimento dos replication_ok
                            Boolean replicationInProgress = true;
                            while(replicationInProgress){
                                replicationInProgress = keyPutReplicationOkIndexes.get(mensagemRequest.getKey()) < qtdSubordinados;
                            }

                            //Apresenta as informações solicitadas no topico 6.
                            System.out.println("Enviando PUT_OK ao Cliente " + mensagemRequest.getIpClient() + ":" + mensagemRequest.getPortaClient() + " da key: " + mensagemRequest.getKey() + " ts: " + timestamp);

                            // envia para o cliente o PUT_OK após receber todas as REPLICATION_OK
                            Mensagem responsePut = new Mensagem(mensagemRequest.getKey(), mensagemRequest.getValue(), timestamp, "PUT_OK");
                            this.sleep(50); //Tempo necessário para aguardar a abertura de um server socket pelo cliente antes do envio da msg
                            Socket socketResponsePut = new Socket(mensagemRequest.getIpClient(), mensagemRequest.getPortaClient());

                            OutputStream osPut = socketResponsePut.getOutputStream();
                            DataOutputStream writerPut = new DataOutputStream(osPut);

                            writerPut.writeBytes(gson.toJson(responsePut) + "\n");

                            socketResponsePut.close();

                        //Redireciona a mensagem para o servidor lider
                        } else {
                            //Apresenta as informações solicitadas no topico 6.
                            System.out.println("Encaminhando PUT key: " + mensagemRequest.getKey() + " value: " + mensagemRequest.getValue());

                            Socket socketLeader = new Socket(ipLeader, portaLeader);
                            OutputStream osLeader = socketLeader.getOutputStream();
                            DataOutputStream writerLeader = new DataOutputStream(osLeader);

                            writerLeader.writeBytes(requestString + "\n");
                            socketLeader.close();
                        }
                        break;
                    //5.d) Recebe a requisição replication, simula um atraso de 6 segundos e processar a mensagem
                    case "REPLICATION":
                        //Atraso proposital para simular o erro 5.f) TRY_OTHER_SERVER_OR_LATER. Para testar o fluxo sem erros remover a linha abaixo
                        this.sleep(6000);

                        //Apresenta as informações solicitadas no topico 6.
                        System.out.println("REPLICATION key: " + mensagemRequest.getKey() + " value: " + mensagemRequest.getValue() + " ts: " + mensagemRequest.getTimestamp());

                        //Salva as informações da mensagem replicada em suas tabelas
                        keyValueTable.put(mensagemRequest.getKey(), mensagemRequest.getValue());
                        keyTimestampTable.put(mensagemRequest.getKey(), mensagemRequest.getTimestamp());

                        //Envia a mensagem de replication ok para o lider
                        Socket socketResponseReplication = new Socket(ipLeader, portaLeader);

                        OutputStream osReplication = socketResponseReplication.getOutputStream();
                        DataOutputStream writerReplication = new DataOutputStream(osReplication);

                        Mensagem responseReplicationOK = new Mensagem(mensagemRequest.getKey(), mensagemRequest.getValue(), mensagemRequest.getTimestamp(), "REPLICATION_OK");
                        writerReplication.writeBytes(gson.toJson(responseReplicationOK) + "\n");

                        socketResponseReplication.close();
                        break;
                    //5.e) Ao receber uma mensagem de replication ok, adiciona 1 no contador de servidores já replicados
                    case "REPLICATION_OK":
                        keyPutReplicationOkIndexes.put(mensagemRequest.getKey(), keyPutReplicationOkIndexes.get(mensagemRequest.getKey()) + 1);
                        break;
                    default:
                        Socket socketError = new Socket(mensagemRequest.getIpClient(), mensagemRequest.getPortaClient());

                        OutputStream osError = socketError.getOutputStream();
                        DataOutputStream writerError = new DataOutputStream(osError);

                        Mensagem responseError = new Mensagem(null, null, null, "TYPE_ERROR");
                        writerError.writeBytes(gson.toJson(responseError) + "\n");
                        break;
                }
            } catch( Exception e) {
                e.printStackTrace();
                System.out.println("Houve um erro ao responder para o cliente");
            }
        }
    }

    //Thread responsavel por enviar uma mensagem de replication, utilizada para enviar em paralelo todas as mensagens de replication.
    static class ThreadReplication extends Thread {

        Integer index;
        Mensagem mensagemReplication;

        public ThreadReplication(Integer index, Mensagem mensagem) {
            this.index = index;
            this.mensagemReplication = mensagem;
        }

        @Override
        public void run() {
            try {
                Gson gson = new Gson();
                Socket socketReplication = new Socket(ips[index], portas[index]);
                OutputStream os = socketReplication.getOutputStream();
                DataOutputStream writer = new DataOutputStream(os);

                writer.writeBytes(gson.toJson(mensagemReplication) + "\n");
                socketReplication.close();
            } catch (IOException e) {
                System.out.println("Ocorreu um erro ao enviar a mensagem de replicação");
            }
        }
    }
}
