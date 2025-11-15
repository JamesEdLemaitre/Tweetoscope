package tweetoscope.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import tweetoscope.tweetsFilter.*;
import tweetoscope.tweetsProducer.MockTwitterStreamRandom;
import tweetoscope.tweetsProducer.MockTwitterStreamRecorded;
import tweetoscope.tweetsProducer.MockTwitterStreamScenario;
import tweetoscope.tweetsProducer.TweetsProducer;
import tweetoscope.tweetsProducer.TwitterFilteredStreamReaderSingleton;
import tweetoscope.tweetsProducer.TwitterSampledStreamReaderSingleton;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerRecord;
import tweetoscope.tweetsProducer.MockTwitterStreamRecorded;

import java.util.concurrent.TimeUnit;

import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import com.twitter.clientlib.model.Tweet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.time.OffsetDateTime;


public class KafkaTweetProducer {

    protected static String fileName;
    private static final String TOPIC_NAME = "raw-tweets";
    private static final String ENV_VAR_NAME = "tweetoscope_sourcefile";

    private static TweetsProducer tweetsProducer;

    public static void main(String[] args) {
        if (args.length == 0) {
            tweetsProducer = new MockTwitterStreamRandom();
            System.out.println("Starting producing random tweets");
            new KafkaTweetProducer();
        } else {
            switch (args[0]) {
                case "random":
                    tweetsProducer = new MockTwitterStreamRandom();
                    System.out.println("Starting producing random tweets");
                    new KafkaTweetProducer();
                case "recorded":
                    fileName = System.getenv(ENV_VAR_NAME);

                    if (fileName == null || fileName.isEmpty()) {
                        System.err.println("Erreur: La variable d'environnement '" + ENV_VAR_NAME + "' n'est pas définie ou est vide pour le mode 'recorded'.");
                        System.exit(1);
                    }

                    tweetsProducer = new MockTwitterStreamRecorded(fileName);
                    System.out.println("Starting producing pre-recorded tweets");
                    new KafkaTweetProducer();
            }
        }
    }

    KafkaTweetProducer() {

        try (KafkaProducer<Void, String> kafkaProducer = new KafkaProducer<>(configureKafkaProducer())) {
            // TweetsProducer tweetsProducer = null;

            // For now, let's only code for a recorded stream:

            // On ajoute la logique de lecture décrite dans MockTwitterStreamRecorder.java
            Gson gson = new GsonBuilder()
                    .registerTypeAdapter(OffsetDateTime.class,
                            (com.google.gson.JsonSerializer<OffsetDateTime>) (src, typeOfSrc, context) ->
                                    new com.google.gson.JsonPrimitive(src.toString()))
                    .create();

            tweetsProducer.subscribe(new Subscriber<Tweet>() {
                private Subscription subscription;

                @Override
                public void onSubscribe(Subscription subscription) {
                    this.subscription = subscription;
                    subscription.request(Long.MAX_VALUE);
                }

                @Override  //
                public void onNext(Tweet tweet) {
                    try {
                        // Convertir le Tweet en JSON
                        String tweetJson = gson.toJson(tweet);

                        // Envoyer au topic Kafka
                        ProducerRecord<Void, String> record =
                                new ProducerRecord<>(TOPIC_NAME, null, tweetJson);

                        kafkaProducer.send(record, (metadata, exception) -> {
                            if (exception != null) {
                                System.err.println("Error sending tweet: " + exception.getMessage());
                                exception.printStackTrace();
                            } else {
                                System.out.println("Tweet sent to partition " + metadata.partition() + " with offset " + metadata.offset());
                            }
                        });

                    } catch (Exception e) {
                        System.err.println("Exception in onNext: " + e.getMessage());
                        e.printStackTrace();
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    throwable.printStackTrace();
                }

                @Override
                public void onComplete() {
                    kafkaProducer.flush();  // Force l'envoi des messages en attente
                }
            });


            // Lancer le thread qui lit les tweets
            Thread producerThread = new Thread(tweetsProducer);
            producerThread.start();

            // Attendre la fin du thread
            try {
                producerThread.join();
            } catch (InterruptedException e) {
                System.err.println(e.getMessage());
            }
        }
    }


    private Properties configureKafkaProducer() {
        Properties producerProperties = new Properties();
        String bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            bootstrapServers = "kafka:9092"; // Default for Docker environment
        }
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.VoidSerializer");
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        return producerProperties;
    }
}
