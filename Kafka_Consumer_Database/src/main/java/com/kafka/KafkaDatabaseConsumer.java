package com.kafka;

import com.kafka.entity.WikimediaData;
import com.kafka.repository.WikimediaDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaDatabaseConsumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaDatabaseConsumer.class);

    @Autowired
    private WikimediaDataRepository repo;

    @KafkaListener(topics = "wikimedia_recentchange",groupId = "myGroup")
    public void consumer(String eventMessage){
         logger.info(String.format("Event message received -> %s", eventMessage));
        WikimediaData data = new WikimediaData();
        data.setWikiEventData(eventMessage);
        repo.save(data);
    }

}
