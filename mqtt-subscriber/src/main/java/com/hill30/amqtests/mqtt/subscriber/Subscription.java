package com.hill30.amqtests.mqtt.subscriber;

import javax.persistence.*;

@Entity
@Table(name = "subscription")
public class Subscription {
        @Id
        @GeneratedValue(strategy= GenerationType.AUTO)
        private Integer id;

        @Column(name = "userId",unique=true)
        private String clientId;
        private Long sent;
        private Long received;

        public Subscription() {}

        public Subscription(Integer id, String clientId) {
            this.id = id;
            this.clientId = clientId;
        }

        public Integer getId() {
            return this.id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public String getClientId() {
            return clientId;
        }

        public void setEmail(String clientId) {
            this.clientId = clientId;
        }

        public void incrementSentCounter() {
            this.sent++;
        }

        public void incrementReceivedCounter() {
            this.received++;
        }
}
