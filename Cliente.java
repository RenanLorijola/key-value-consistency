import com.google.gson.Gson;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Cliente {
    static final private Scanner scanner = new Scanner(System.in);
    static final private Integer qtdServidores = 3;
    static final private String[] ips = new String[qtdServidores];
    static final private Integer[] portas = new Integer[qtdServidores];
    //Tabela de timestamps que vai conter contadores de cada key
    static final private ConcurrentHashMap<String, Integer> keyTimestampTable = new ConcurrentHashMap();

    public static void main(String[] args) {
        while(true){
            System.out.println("----------------------------------------------------------------------");
            System.out.println("Escolha entre INIT, PUT ou GET");
            System.out.println("----------------------------------------------------------------------");
            String funcao = scanner.nextLine();
            switch (funcao.toUpperCase(Locale.ROOT)){
                //4.a) Inicialização do cliente, obtendo os ips e portas dos servidores
                case "INIT":
                    if(ips[0] != null){
                        System.out.println("Cliente já inicializado, tente usar \"PUT\" ou \"GET\"");
                        break;
                    }

                    System.out.println("Digite o ip:porta dos " + qtdServidores + " servidores, um por linha");
                    System.out.println("Caso queira usar os valores default, basta apertar enter sem informar nada");
                    for(int i = 0; i < qtdServidores; i ++){
                        String input = scanner.nextLine();

                        //armazena os valores default caso o usuário não insira valor algum.
                        if(input.equals("")){
                            ips[0] = "127.0.0.1";
                            ips[1] = ips[0];
                            ips[2] = ips[0];
                            portas[0] = 10097;
                            portas[1] = 10098;
                            portas[2] = 10099;
                            break;
                        }

                        //insere os valores informados pelo usuário
                        String[] ipPorta = input.split(":");
                        ips[i] = ipPorta[0];
                        portas[i] = Integer.parseInt(ipPorta[1]);
                    }
                    break;

                //4.b) Faz o envio de uma mensagem PUT, capturando os valores de key e value do teclado
                // e abrindo uma thread para fazer o envio e receber a resposta do servidor
                case "PUT":
                    if(ips[0] == null){
                        System.out.println("Cliente ainda não inicializado, tente usar \"INIT\"");
                        break;
                    }

                    // Obtem o valor de key e value do usuário
                    System.out.println("Digite a key e a value a ser inserida, um por linha");
                    String keyPut = scanner.nextLine();
                    String valuePut = scanner.nextLine();

                    if(keyTimestampTable.get(keyPut) == null){
                        keyTimestampTable.put(keyPut, 0);
                    }

                    keyTimestampTable.put(keyPut, keyTimestampTable.get(keyPut) + 1);
                    //Ao realizar um put, adiciona o timestamp local na mensagem e incrementa o timestamp para o próximo evento
                    Mensagem mensagemPut = new Mensagem(keyPut, valuePut, keyTimestampTable.get(keyPut));

                    // Inicia uma thread responsavel pelo envio da mensagem e recebimento da resposta
                    new ThreadSendAndReceiveMensagem(mensagemPut).start();
                    break;
                //4.c) Faz o envio de uma mensagem GET, capturando os valores de key e value do teclado
                // e abrindo uma thread para fazer o envio e receber a resposta do servidor
                case "GET":
                    if(ips[0] == null){
                        System.out.println("Cliente ainda não inicializado, tente usar \"INIT\"");
                        break;
                    }

                    // Obtem o valor da key procurada no get
                    System.out.println("Digite a key a ser buscada");
                    String keyGet = scanner.nextLine();

                    //Coloca o valor -1 na key caso ela seja null, para evitar erros de null pointer no servidor
                    if(keyTimestampTable.get(keyGet) == null){
                        keyTimestampTable.put(keyGet, 0);
                    }

                    Mensagem mensagemGet = new Mensagem(keyGet, keyTimestampTable.get(keyGet));

                    // Inicia uma thread responsavel pelo envio da mensagem e recebimento da resposta
                    new ThreadSendAndReceiveMensagem(mensagemGet).start();
                    break;
                default:
                    System.out.println("Opção não mapeada, use \"INIT\", \"PUT\" OU \"GET\"");
                    break;
            }
        }
    }

    //Thread responsavel por criar um socket para enviar a mensagem a algum servidor e criar um server socket para esperar a resposta de algum servidor.
    public static class ThreadSendAndReceiveMensagem extends Thread {
        private Mensagem mensagemRequest;

        public ThreadSendAndReceiveMensagem(Mensagem mensagem) {
            this.mensagemRequest = mensagem;
        }

        @Override
        public void run(){
            try {
                Gson gson = new Gson();
                Random gerador = new Random();
                Integer servidorAleatorio = gerador.nextInt(qtdServidores);

                Socket socketRequest = new Socket(ips[servidorAleatorio], portas[servidorAleatorio]);

                OutputStream os = socketRequest.getOutputStream();
                DataOutputStream writer = new DataOutputStream(os);

                mensagemRequest.setIpClient((((InetSocketAddress) socketRequest.getRemoteSocketAddress()).getAddress()).toString().replace("/",""));
                mensagemRequest.setPortaClient(socketRequest.getLocalPort());

                String mensagemString = gson.toJson(mensagemRequest);

                writer.writeBytes(mensagemString + "\n");
                socketRequest.close();

                ServerSocket serverSocket = new ServerSocket(socketRequest.getLocalPort());
                Socket socketResponse = serverSocket.accept();

                InputStreamReader is = new InputStreamReader(socketResponse.getInputStream());
                BufferedReader reader = new BufferedReader(is);

                String responseString = reader.readLine();
                socketResponse.close();

                Mensagem mensagemResponse = gson.fromJson(responseString, Mensagem.class);

                if (mensagemResponse.getTipo().equals("PUT_OK")){
                    //Apresenta as informações solicitadas no topico 6.
                    System.out.println("PUT_OK key: " + mensagemResponse.getKey() + " value: " + mensagemResponse.getValue() + " timestamp: " + mensagemResponse.getTimestamp() + " realizada no servidor " + ips[servidorAleatorio]+":"+portas[servidorAleatorio]);

                    //Salva o timestamp do lider na tabela
                    keyTimestampTable.put(mensagemResponse.getKey(), mensagemResponse.getTimestamp());
                }else if(mensagemResponse.getTipo().equals("GET")){
                    //Apresenta as informações solicitadas no topico 6.
                    System.out.println("GET key: " + mensagemResponse.getKey() + " value: " + mensagemResponse.getValue() + " obtido do servidor " + ips[servidorAleatorio]+":"+portas[servidorAleatorio]+", meu timestamp " + keyTimestampTable.get(mensagemResponse.getKey()) + " e do servidor " + mensagemResponse.getTimestamp());

                    //Salva o timestamp do lider na tabela
                    keyTimestampTable.put(mensagemResponse.getKey(), mensagemResponse.getTimestamp());
                }else{
                    // Mostra todas as mensagens de erro devolvidas pelo servidor
                    System.out.println(mensagemResponse.getTipo() + "! Ocorreu um erro no processamento da requisição pelo servidor");
                    System.out.println("Valores erro -> key: " + mensagemResponse.getKey() + " value: " + mensagemResponse.getValue() + " ts: " + mensagemResponse.getTimestamp());
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Ocorreu um erro ao enviar a requisição para o servidor");
            }
        }
    }
}
