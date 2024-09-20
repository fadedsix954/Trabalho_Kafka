package mensageria;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class ClienteKafka {
    private static JTextArea areaStatus;
    private JTextField campoProduto;
    private static String value = "";

    public ClienteKafka() {
        String topicName = "ecommerce-eletronicos";
        Properties propsProducer = new Properties();
        propsProducer.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        propsProducer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propsProducer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        JFrame janela = new JFrame("Loja de Eletrônicos");
        janela.setSize(500, 400);
        janela.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        janela.setLayout(new BorderLayout());

        JPanel painelPrincipal = new JPanel();
        painelPrincipal.setLayout(new GridBagLayout());
        janela.add(painelPrincipal, BorderLayout.CENTER);

        GridBagConstraints restricoes = new GridBagConstraints();
        restricoes.insets = new Insets(10, 10, 10, 10);
        restricoes.anchor = GridBagConstraints.WEST;

        JLabel rotuloProduto = new JLabel("Produto:");
        restricoes.gridx = 0;
        restricoes.gridy = 0;
        painelPrincipal.add(rotuloProduto, restricoes);

        campoProduto = new JTextField(20);
        restricoes.gridx = 1;
        restricoes.gridy = 0;
        restricoes.fill = GridBagConstraints.HORIZONTAL;
        painelPrincipal.add(campoProduto, restricoes);

        JButton botaoComprar = new JButton("Comprar");
        restricoes.gridx = 0;
        restricoes.gridy = 1;
        restricoes.gridwidth = 2;
        restricoes.fill = GridBagConstraints.NONE;
        restricoes.anchor = GridBagConstraints.CENTER;
        painelPrincipal.add(botaoComprar, restricoes);

        areaStatus = new JTextArea(10, 30);
        areaStatus.setEditable(false);
        JScrollPane scrollPane = new JScrollPane(areaStatus);
        restricoes.gridx = 0;
        restricoes.gridy = 2;
        restricoes.gridwidth = 2;
        restricoes.fill = GridBagConstraints.BOTH;
        painelPrincipal.add(scrollPane, restricoes);

        botaoComprar.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                value = campoProduto.getText();
                areaStatus.append("Compra iniciada: " + value + "\n");
                areaStatus.append("Aguardando Confirmação...\n");
                new ServidorKafka(value, propsProducer);
                campoProduto.setText("");
            }
        });

        janela.setVisible(true);
        janela.setLocationRelativeTo(null);
    }

    public static void main(String[] args) {
        String topicName = "pedidos-eletronicos";
        String topicConfirmacao = "confirmacao-pedidos";
        String topicCancelamento = "cancelamento-pedidos";

        Properties propsConsumer = new Properties();
        propsConsumer.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        propsConsumer.put(ConsumerConfig.GROUP_ID_CONFIG, topicName);
        propsConsumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propsConsumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propsConsumer.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumerPedidos = new KafkaConsumer<>(propsConsumer);
        consumerPedidos.subscribe(Collections.singleton(topicName));

        new Thread(() -> {
            while (true) {
                ConsumerRecords<String, String> records = consumerPedidos.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Pedido Recebido > " + record.value());
                }
            }
        }).start();

        KafkaConsumer<String, String> consumerConfirmacao = new KafkaConsumer<>(propsConsumer);
        consumerConfirmacao.subscribe(Collections.singleton(topicConfirmacao));

        new Thread(() -> {
            while (true) {
                ConsumerRecords<String, String> records = consumerConfirmacao.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Confirmação Recebida > " + record.value());
                    SwingUtilities.invokeLater(() -> {
                        areaStatus.append("Confirmação: " + record.value() + "\n");
                    });
                }
            }
        }).start();

        KafkaConsumer<String, String> consumerCancelamento = new KafkaConsumer<>(propsConsumer);
        consumerCancelamento.subscribe(Collections.singleton(topicCancelamento));

        new Thread(() -> {
            while (true) {
                ConsumerRecords<String, String> records = consumerCancelamento.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Cancelamento Recebido > " + record.value());
                    SwingUtilities.invokeLater(() -> {
                        areaStatus.append("Cancelamento: " + record.value() + "\n");
                    });
                }
            }
        }).start();

        SwingUtilities.invokeLater(() -> new ClienteKafka());
    }
}