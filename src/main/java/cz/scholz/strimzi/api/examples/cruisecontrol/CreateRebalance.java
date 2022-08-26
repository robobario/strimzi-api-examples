package cz.scholz.strimzi.api.examples.cruisecontrol;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.KafkaRebalanceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static io.strimzi.api.kafka.model.CustomResourceConditions.isProposalReady;
import static io.strimzi.api.kafka.model.CustomResourceConditions.isReady;

public class CreateRebalance {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateRebalance.class);
    private static final String NAMESPACE = "myproject";
    private static final String REBALANCE_NAME = "myrebalance";

    public static void main(String[] args) {
        try (KubernetesClient client = new KubernetesClientBuilder().build()) {
            LOGGER.info("create rebalance");
            KafkaRebalanceBuilder kafkaRebalanceBuilder = new KafkaRebalanceBuilder().editMetadata()
                    .withName(REBALANCE_NAME).addToLabels("strimzi.io/cluster", "my-cluster")
                    .withClusterName("my-cluster").endMetadata().withNewSpec().endSpec();
            Crds.kafkaRebalanceOperation(client).inNamespace(NAMESPACE)
                    .resource(kafkaRebalanceBuilder.build()).create();
            LOGGER.info("wait for rebalance proposal ready");
            Crds.kafkaRebalanceOperation(client).inNamespace(NAMESPACE).withName(REBALANCE_NAME)
                    .waitUntilCondition(isProposalReady(), 5, TimeUnit.MINUTES);
            LOGGER.info("annotate rebalance as approved");
            Crds.kafkaRebalanceOperation(client)
                    .inNamespace(NAMESPACE).resource(kafkaRebalanceBuilder
                            .editMetadata().addToAnnotations("strimzi.io/rebalance", "approve")
                            .endMetadata().build()).createOrReplace();
            LOGGER.info("wait for rebalance to finish");
            Crds.kafkaRebalanceOperation(client).inNamespace(NAMESPACE).withName(REBALANCE_NAME)
                    .waitUntilCondition(isReady(), 5, TimeUnit.MINUTES);
            LOGGER.info("wait for ready");
        }
    }
}