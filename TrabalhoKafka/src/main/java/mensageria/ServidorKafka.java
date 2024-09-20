package mensageria;


import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

class ServidorKafka {
    private String produto;
    private Properties propsProducer;

    public ServidorKafka(String produto, Properties propsProducer) {
        this.produto = produto;
        this.propsProducer = propsProducer;

        JDialog dialog = new JDialog();
        dialog.setTitle("Confirmar Pedido");
        dialog.setSize(300, 150);
        dialog.setLayout(new FlowLayout());
        dialog.setModal(true);

        JLabel mensagem = new JLabel("Você deseja confirmar a compra de: " + produto + "?");
        dialog.add(mensagem);

        JButton confirmarButton = new JButton("Confirmar");
        JButton cancelarButton = new JButton("Cancelar");

        dialog.add(confirmarButton);
        dialog.add(cancelarButton);

        confirmarButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                KafkaProducer<String, String> producer = new KafkaProducer<>(propsProducer);
                ProducerRecord<String, String> recordProducer = new ProducerRecord<>("confirmacao-pedidos", "Confirmado");
                producer.send(recordProducer);
                System.out.println(recordProducer.value());
                dialog.dispose();
            }
        });

        cancelarButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                KafkaProducer<String, String> producer = new KafkaProducer<>(propsProducer);
                ProducerRecord<String, String> recordProducer = new ProducerRecord<>("cancelamento-pedidos", "Cancelado");
                producer.send(recordProducer);
                System.out.println(recordProducer.value());
                dialog.dispose();
            }
        });

        dialog.setLocationRelativeTo(null);
        dialog.setVisible(true);
    }
}
